import asyncio
import sys
import time
import json
import os
import math
import re
from datetime import datetime
from typing import Dict, Any, Optional, List

# High-performance async I/O loop on Linux environments
if sys.platform != "win32":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx

from bot.config import settings, CONFIG_PATH
import bot.data as data
import bot.ws_data as ws_data
import bot.chainlink as chainlink
import bot.indicators as indicators
import bot.engines as engines
import bot.utils as utils
from bot.clob_trader import clob_trader
import bot.longshot as longshot          # PATCH: longshot
import bot.redeem_sweep as redeem_sweep  # PATCH: longshot
import bot.ls_maker as ls_maker          # PATCH: longshot maker
from types import SimpleNamespace        # PATCH: longshot maker

STATE_PATH = "state_data.json"
SIGNALS_PATH = os.path.join("logs", "signals.csv")
LOG_FILE_PATH = os.path.join("logs", "app.log")
TELEGRAM_SUBS_PATH = "telegram_subscribers.json"

# ── WebSocket broadcast clients for real-time live PnL & dashboard ────────────
_ws_clients = set()

# ── Event-driven early entries & wakeups ───────────────────────────────────────
MIN_EVAL_INTERVAL_S = 0.0   # was 0.25 (at most 4 checks a second, so a tick could wait up to 250 ms); every tick is checked now
CTX_MAX_AGE_S = 4.0        # 4.0s context validity from TOG-main
POLY_WS_MAX_AGE_MS = 2500
MARK_CAPTURE_WINDOW_MS = 20_000

_market_event = asyncio.Event()
_entry_lock = asyncio.Lock()
_trade_lock = asyncio.Lock()
_in_flight_markets = set()
_last_eval_ts = 0.0
_loop_wake = asyncio.Event()   # runs update_loop's next pass now (set when the new window's first Chainlink price arrives)
STRIKE_WAIT_MS = 2500          # a Chainlink price stamped before the window opened is only used as the strike after this
NEXT_MARKET_PREFETCH_MS = 60_000   # look up the next window's market this long before it opens
ORDER_CONN_PING_S = 15.0       # keep the order connection to Polymarket open (see order_conn_keeper)
_logged_once: Dict[str, float] = {}


def _log_once(key: str, msg: str):
    """log_message once per key (keys include the market, so once per window): lines that
    would otherwise repeat on every tick."""
    if key in _logged_once:
        return
    if len(_logged_once) > 500:
        _logged_once.clear()
    _logged_once[key] = time.time()
    log_message(msg)


# ── Long shots (PATCH: longshot) ──────────────────────────────────────────────
_ls_lock = asyncio.Lock()
_ls_entered_windows = set()   # window start_ms values where a long shot was entered


def _is_longshot(t: Dict[str, Any]) -> bool:
    return t.get("kind") == "longshot"


def _is_ls_maker(t: Dict[str, Any]) -> bool:
    """PATCH: longshot maker — a long shot bought by a resting maker bid (bot/ls_maker.py)."""
    return t.get("open_reason") == "longshot_maker"


def _slot_keys(market: Dict[str, Any], kind: str = "regular") -> List[str]:
    """In-flight keys for a market. Long shots use their own ("ls:"-prefixed) keys, so a
    long shot and a regular trade never block each other."""
    mid, slug = str(market.get("id") or ""), str(market.get("slug") or "")
    pre = "ls:" if kind == "longshot" else ""
    return [pre + k for k in (mid, slug) if k]


def _model_steps(minutes_left: float):
    """Model clock for the regular strategy (settings model.time_mode)."""
    if settings.MODEL_TIME_MODE == "exact":
        return max(float(minutes_left) * 60.0, 0.5) / 300.0
    return max(1, math.ceil(minutes_left / 5))

def _sync_active_trades_to_latest_data():
    if "trading_state" in state.get("latest_data", {}):
        ts = state["latest_data"]["trading_state"]
        ts["active_trades"] = list(state["active_trades"])
        live_open_val = 0.0
        for t in state["active_trades"]:
            if t.get("live_status") == "FILLED" and t.get("live_shares"):
                try:
                    s = float(t.get("live_shares") or 0.0)
                    p = float(t.get("live_mark_price") or t.get("live_entry_price") or 0.0)
                    live_open_val += s * p
                except (ValueError, TypeError):
                    pass
        ts["open_value"] = live_open_val
        live_bal = float(state.get("live_balance") or 0.0)
        ts["equity"] = live_bal + live_open_val
        ts["balance"] = live_bal
        ts["has_live_creds"] = bool(settings.PRIVATE_KEY)

_last_spot_broadcast_ts = 0.0
_last_broadcast_spot_price = 0.0

def _wake_entry(data_payload=None):
    global _last_spot_broadcast_ts, _last_broadcast_spot_price
    _market_event.set()
    if data_payload and isinstance(data_payload, dict):
        source = str(data_payload.get("source", ""))
        p = data_payload.get("price")
        if p and p > 0:
            if "spot" in source or not state.get("last_spot_price"):
                state["last_spot_price"] = p
                prices = state.setdefault("latest_data", {}).setdefault("prices", {})
                prices["spot"] = p
                now = time.time()
                if (now - _last_spot_broadcast_ts >= 0.25 and _ws_clients
                        and abs(p - _last_broadcast_spot_price) >= 0.01):
                    _last_spot_broadcast_ts = now
                    _last_broadcast_spot_price = p
                    asyncio.create_task(broadcast_state())

def _on_poly_chainlink_tick(data_payload=None):
    """Polymarket's Chainlink feed (where the strike comes from). Checks entries on it, and
    when the window that just opened has no strike yet and this price is from the new
    window, runs update_loop's next pass now instead of up to POLL_INTERVAL_MS later.
    PATCH: longshot — every price also goes into the long-shot recorder (the 60-second
    averages), stamped with the price's own time (never later than now)."""
    _market_event.set()
    if not isinstance(data_payload, dict):
        return
    _p = data_payload.get("price")
    if _p and _p > 0:
        _ts = data_payload.get("updatedAt")
        longshot.record_price(min((float(_ts) / 1000.0) if _ts else time.time(), time.time()), _p)
    w_ms = int(settings.CANDLE_WINDOW_MINUTES * 60_000)
    start = int(time.time() * 1000) // w_ms * w_ms
    win = state.get("market_opens", {}).get(start)
    if (win is None or win.get("chainlink") is None) and float(data_payload.get("updatedAt") or 0) >= start:
        _loop_wake.set()


def _on_chainlink_tick(data_payload=None):
    if data_payload and isinstance(data_payload, dict):
        p = data_payload.get("price")
        if p and p > 0:
            state["last_chainlink_price"] = p
            prices = state.setdefault("latest_data", {}).setdefault("prices", {})
            prices["chainlink"] = p
            prices["chainlink_source"] = data_payload.get("source", "Chainlink RPC WS")
            _market_event.set()
            asyncio.create_task(broadcast_state())


# ── Global state ───────────────────────────────────────────────────────────────
state = {
        "latest_data": {
        "timestamp": datetime.now().isoformat(),
        "timing": {},
        "market": None,
        "trading_state": {
            "mode": "hybrid",
            "running": False,
            "balance": 1000.0,
            "equity": 1000.0,
            "open_value": 0.0,
            "active_trades": [],
            "history_count": 0,
            "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
            "symbol": settings.SYMBOL,
            "has_live_creds": bool(settings.PRIVATE_KEY),
            "withdraw_state": "idle",
            "last_withdrawal": None,
            "withdraw": {
                "enabled": settings.AUTO_WITHDRAW_ENABLED,
                "state": "ARMED",
                "amount": settings.WITHDRAW_AMOUNT,
                "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE
            }
        },
        "prices": {
            "spot": None,
            "chainlink": None,
            "chainlink_source": "Connecting...",
            "poly_up": None,
            "poly_down": None,
            "window_open": None,
            "window_open_source": None,
            "model_open": None,
            "window_start_ms": None,
            "price_is_fresh": True,
            "book_source": None
        },
        "indicators": {
            "rsi": None,
            "heiken": {"color": None, "count": 0},
            "heiken_5m": {"color": None, "count": 0},
            "fair": None
        },
        "analysis": {
            "probability": {"adjustedUp": 0.5, "adjustedDown": 0.5},
            "edge": {"edgeUp": None, "edgeDown": None},
            "decision": {"action": "NO_TRADE", "reason": "initializing"}
        }
    },
    "last_update_ts": 0,
    "trading_mode": "hybrid",
    "paper_balance": 1000.0,
    "live_balance": 0.0,
    "precomputed_live_stake": 0.0,
    "active_trades": [],
    "trade_history": [],
    "logs": [],
    "log_seq": 0,
    "last_trade_side": None,
    "last_balance_refresh": 0,
    # Trading is OFF until user presses Start.
    "running": False,
    "market_opens": {},
    "last_window_start": None,
    "last_seen_price": None,
    # Capital extractor
    "withdraw_state": "idle",  # "idle" | "in_progress" | "cooldown"
    "last_withdrawal": None,
    "withdraw_flat_since": None,
    "withdraw_submitted_at": None,
    "withdraw_locked_market": None,
    # Telegram
    "telegram_subscribers": [],
    # Event-driven execution
    "trade_ctx": None,
    "event_exec": None,
}

def log_message(msg: str):
    now_dt = datetime.now()
    timestamp = now_dt.strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted)
    state["logs"].append(formatted)
    state["log_seq"] = state.get("log_seq", 0) + 1
    if len(state["logs"]) > 200:
        state["logs"].pop(0)

    try:
        os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
        date_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{date_str}] {msg}\n")
    except Exception:
        pass

clob_trader.set_log_hook(log_message)  # LIVE WALLET / LIVE SIGNER lines go to the app log

def save_state():
    try:
        data_to_save = {
            "paper_balance": state["paper_balance"],
            "live_balance": state.get("live_balance", 0.0),
            "active_trades": state["active_trades"],
            "trade_history": state["trade_history"],
            "last_trade_side": state["last_trade_side"],
            "last_withdrawal": state.get("last_withdrawal"),
            "ls_maker_open": state.get("ls_maker_open", []),  # PATCH: longshot maker — cancelled at the next start
        }
        with open(STATE_PATH, "w") as f:
            json.dump(data_to_save, f, indent=2)
    except Exception as e:
        print(f"Error saving state: {e}")

def load_state():
    try:
        load_telegram_subscribers()
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f:
                loaded = json.load(f)
                state["paper_balance"] = loaded.get("paper_balance", 1000.0)
                state["live_balance"] = loaded.get("live_balance", 0.0)
                state["active_trades"] = loaded.get("active_trades", [])
                state["trade_history"] = loaded.get("trade_history", [])
                state["last_trade_side"] = loaded.get("last_trade_side")
                state["last_withdrawal"] = loaded.get("last_withdrawal")
                state["ls_maker_open"] = loaded.get("ls_maker_open") or []  # PATCH: longshot maker
                log_message(f"State loaded from {STATE_PATH}")
    except Exception as e:
        print(f"Error loading state: {e}")

# ── Telegram notifications & poller ───────────────────────────────────────────
def _normalize_subscriber(s: Any) -> Optional[Dict[str, Any]]:
    if isinstance(s, dict):
        cid = s.get("chat_id")
        if cid is not None:
            try:
                return {
                    "chat_id": int(cid),
                    "name": str(s.get("name") or f"Chat {cid}"),
                    "type": str(s.get("type") or "chat"),
                    "subscribed_at": s.get("subscribed_at")
                }
            except (ValueError, TypeError):
                return None
    elif isinstance(s, (int, str)):
        try:
            cid = int(s)
            return {
                "chat_id": cid,
                "name": f"Chat {cid}",
                "type": "chat",
                "subscribed_at": None
            }
        except (ValueError, TypeError):
            return None
    return None

def load_telegram_subscribers() -> List[Dict[str, Any]]:
    try:
        if os.path.exists(TELEGRAM_SUBS_PATH):
            with open(TELEGRAM_SUBS_PATH, "r") as f:
                subs = json.load(f)
                if isinstance(subs, list):
                    normalized = []
                    for s in subs:
                        norm = _normalize_subscriber(s)
                        if norm and not any(x["chat_id"] == norm["chat_id"] for x in normalized):
                            normalized.append(norm)
                    state["telegram_subscribers"] = normalized
                    return state["telegram_subscribers"]
    except Exception as e:
        print(f"Error loading telegram subscribers: {e}")
    state["telegram_subscribers"] = []
    return []

def save_telegram_subscribers():
    try:
        with open(TELEGRAM_SUBS_PATH, "w") as f:
            json.dump(state["telegram_subscribers"], f, indent=2)
    except Exception as e:
        print(f"Error saving telegram subscribers: {e}")

def add_telegram_subscriber(chat_id: int, name: str = "", chat_type: str = "chat"):
    subs = state["telegram_subscribers"]
    for s in subs:
        if s.get("chat_id") == chat_id:
            if name and name != f"Chat {chat_id}":
                s["name"] = name
            if chat_type:
                s["type"] = chat_type
            save_telegram_subscribers()
            return
    subs.append({
        "chat_id": chat_id,
        "name": name or f"Chat {chat_id}",
        "type": chat_type or "chat",
        "subscribed_at": datetime.now().isoformat()
    })
    save_telegram_subscribers()

def remove_telegram_subscriber(chat_id: int):
    subs = state["telegram_subscribers"]
    state["telegram_subscribers"] = [s for s in subs if s.get("chat_id") != chat_id]
    save_telegram_subscribers()

async def send_telegram(text: str):
    if not settings.TELEGRAM_ENABLED or not settings.TELEGRAM_BOT_TOKEN:
        return
    subs = state.get("telegram_subscribers", [])
    if not subs:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    proxy = ws_data.get_proxy_url_for(url)
    for sub in subs:
        chat_id = sub.get("chat_id") if isinstance(sub, dict) else sub
        if not chat_id:
            continue
        try:
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
                await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
        except Exception as e:
            print(f"Failed to send telegram to {chat_id}: {e}")

async def send_telegram_to(chat_id: int, text: str, bot_token: Optional[str] = None):
    tok = bot_token or settings.TELEGRAM_BOT_TOKEN
    if not tok:
        return
    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    proxy = ws_data.get_proxy_url_for(url)
    try:
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    except Exception as e:
        print(f"Failed to send telegram to {chat_id}: {e}")

async def telegram_poller():
    offset = 0
    while True:
        tok = settings.TELEGRAM_BOT_TOKEN
        if not tok:
            await asyncio.sleep(5)
            continue
        try:
            url = f"https://api.telegram.org/bot{tok}/getUpdates"
            proxy = ws_data.get_proxy_url_for(url)
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=10.0) as client:
                resp = await client.get(url, params={"offset": offset, "timeout": 5})
                if resp.status_code == 200:
                    data_updates = resp.json().get("result", [])
                    for update in data_updates:
                        offset = max(offset, update.get("update_id", 0) + 1)
                        msg = update.get("message") or update.get("channel_post") or {}
                        chat = msg.get("chat", {})
                        chat_id = chat.get("id")
                        if not chat_id:
                            continue
                        chat_type = chat.get("type", "chat")
                        first_name = chat.get("first_name", "")
                        last_name = chat.get("last_name", "")
                        username = chat.get("username", "")
                        title = chat.get("title", "")
                        name = title or f"{first_name} {last_name}".strip() or (f"@{username}" if username else f"Chat {chat_id}")
                        text = (msg.get("text") or "").strip()
                        
                        cmd = text.split()[0].lower() if text else ""
                        if cmd == "/stop":
                            remove_telegram_subscriber(chat_id)
                            await send_telegram_to(chat_id, "👋 *Unsubscribed from alerts.*", bot_token=tok)
                        elif cmd == "/status":
                            add_telegram_subscriber(chat_id, name, chat_type)
                            running = "🟢 RUNNING" if state["running"] else "🔴 STOPPED"
                            bal_str = f"${state.get('live_balance', 0.0):.2f}" if settings.PRIVATE_KEY else "Paper (No Wallet)"
                            active_cnt = len(state["active_trades"])
                            msg_txt = f"📊 *Bot Status*\n• Status: {running}\n• Mode: `HYBRID (Paper + Live Copy)`\n• Live Balance: `{bal_str}`\n• Active Trades: `{active_cnt}`"
                            await send_telegram_to(chat_id, msg_txt, bot_token=tok)
                        elif cmd == "/balance":
                            add_telegram_subscriber(chat_id, name, chat_type)
                            bal_str = f"${state.get('live_balance', 0.0):.2f}" if settings.PRIVATE_KEY else "No Live Key"
                            funder = clob_trader.get_funder() or "N/A"
                            await send_telegram_to(chat_id, f"💰 *Current Live Balance*\n• pUSD Balance: `{bal_str}`\n• Wallet: `{funder}`", bot_token=tok)
                        elif cmd == "/help":
                            add_telegram_subscriber(chat_id, name, chat_type)
                            await send_telegram_to(chat_id, "ℹ️ *Commands*\n/start - Subscribe\n/stop - Unsubscribe\n/status - Bot status\n/balance - Wallet balance", bot_token=tok)
                        else:
                            add_telegram_subscriber(chat_id, name, chat_type)
                            if cmd == "/start":
                                await send_telegram_to(chat_id, "🤖 *Subscribed to 15m Polymarket Bot alerts!* Use /status or /balance to check current status.", bot_token=tok)
                elif resp.status_code == 401:
                    await asyncio.sleep(30)
                    continue
        except Exception:
            pass
        await asyncio.sleep(2)

# ── Live PnL and State WebSocket broadcast ────────────────────────────────────
def _json_default(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "hex"):
        return obj.hex()
    if isinstance(obj, set):
        return list(obj)
    return str(obj)

async def broadcast_state():
    if not _ws_clients:
        return
    try:
        msg = json.dumps({
            "type": "state_update",
            "data": state.get("latest_data") or {},
            "logs": state.get("logs", [])[-20:],
            "log_seq": state.get("log_seq", 0),
        }, default=_json_default)
    except Exception as e:
        print(f"broadcast_state json serialization error: {e}")
        return

    dead = set()
    for ws in list(_ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    for ws in dead:
        _ws_clients.discard(ws)

# ── Background task streams ───────────────────────────────────────────────────
def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL),
    on_update=_on_poly_chainlink_tick
)
polymarket_clob_ws = ws_data.PolymarketClobMarketStream(on_update=lambda: _on_book_update())  # PATCH: longshot
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL), on_update=_on_chainlink_tick)

def get_candle_window_timing(window_minutes: int) -> Dict[str, float]:
    now_ms = time.time() * 1000
    window_ms = window_minutes * 60_000
    start_ms = (now_ms // window_ms) * window_ms
    end_ms = start_ms + window_ms
    elapsed_ms = now_ms - start_ms
    remaining_ms = end_ms - now_ms
    return {
        "startMs": start_ms,
        "endMs": end_ms,
        "elapsedMs": elapsed_ms,
        "remainingMs": remaining_ms,
        "elapsedMinutes": elapsed_ms / 60_000,
        "remainingMinutes": remaining_ms / 60_000
    }

_cached_market = None
_cached_market_ts = 0.0
_cached_market_series = ""
_prefetch_tries: Dict[int, float] = {}

_next_market: Dict[str, Any] = {"start_ms": None, "market": None, "tokens": [], "series": None}
_next_market_busy = False


def _token_ids_of(market: Dict[str, Any]):
    """(up token, down token) of a market."""
    outcomes = market.get("outcomes", [])
    ids = market.get("clobTokenIds", [])
    try:
        outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        ids = json.loads(ids) if isinstance(ids, str) else ids
    except Exception:
        return None, None
    up = down = None
    for i, o in enumerate(outcomes or []):
        tid = ids[i] if i < len(ids or []) else None
        if not tid:
            continue
        if str(o).lower() == settings.POLYMARKET_UP_LABEL.lower():
            up = tid
        elif str(o).lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down = tid
    return up, down


def _next_window_tokens(current_market: Optional[Dict[str, Any]]) -> List[str]:
    """Token ids of the next window's market when it was looked up already (its order
    books are followed alongside the current ones)."""
    nm = _next_market
    if not nm.get("market") or not nm.get("tokens") or nm.get("start_ms") is None:
        return []
    if current_market and str(current_market.get("id")) == str(nm["market"].get("id")):
        return []
    if nm["start_ms"] < time.time() * 1000 - 60_000:
        return []
    return list(nm["tokens"])


async def _prefetch_next_market(next_start_ms: int):
    """Look up the next window's market before it opens, follow its order books on the
    websocket and get its tokens ready for orders, so nothing has to be fetched when the
    window opens (that took a REST round trip or two at every window open)."""
    global _next_market_busy
    if _next_market_busy:
        return
    _next_market_busy = True
    try:
        series = settings.POLYMARKET_SLUG or settings.POLYMARKET_SERIES_ID
        market = await _discover_market(next_start_ms, strict=True)
        if not market:
            return
        up, down = _token_ids_of(market)
        if not up or not down:
            return
        _next_market.update(start_ms=int(next_start_ms), market=market, tokens=[up, down], series=series)
        polymarket_clob_ws.update_assets([a for a in polymarket_clob_ws.asset_ids if a not in (up, down)] + [up, down])
        if settings.PRIVATE_KEY:
            clob_trader.warm_token(up)
            clob_trader.warm_token(down)
    except Exception as e:
        print(f"Warning: next market lookup error: {e}")
    finally:
        _next_market_busy = False


async def _discover_market(target_start_ms: float, strict: bool = False) -> Optional[Dict[str, Any]]:
    """The market of the window starting at target_start_ms (strict: only that window)."""
    now_ms = time.time() * 1000
    market = None
    if settings.POLYMARKET_SLUG:
        return None if strict else await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
    if settings.POLYMARKET_AUTO_SELECT_LATEST:
        events = await data.fetch_live_events_by_series_id(settings.POLYMARKET_SERIES_ID)
        markets = data.flatten_event_markets(events)
        live_markets = []
        for m in markets:
            end_str = m.get("endDate")
            if not end_str:
                continue
            try:
                end_m_ts = datetime.fromisoformat(end_str.replace('Z', '+00:00')).timestamp() * 1000
            except Exception:
                continue
            if end_m_ts <= now_ms + 15_000:
                continue
            esr = m.get("eventStartTime") or m.get("gameStartTime")
            start_m_ts = None
            if esr:
                try:
                    start_m_ts = datetime.fromisoformat(str(esr).replace('Z', '+00:00')).timestamp() * 1000
                except Exception:
                    pass
            if start_m_ts is None:
                start_m_ts = end_m_ts - (settings.CANDLE_WINDOW_MINUTES * 60_000)
            if abs(start_m_ts - target_start_ms) < 120_000 or (not strict and start_m_ts <= now_ms < end_m_ts):
                live_markets.append((m, end_m_ts, abs(start_m_ts - target_start_ms)))
        if live_markets:
            live_markets.sort(key=lambda x: (x[2], x[1]))
            market = live_markets[0][0]
    return market


async def get_active_market(force: bool = False) -> Optional[Dict[str, Any]]:
    global _cached_market, _cached_market_ts, _cached_market_series
    now = time.time()
    now_ms = now * 1000
    timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
    cur_start_ms = timing["startMs"]

    curr_target = settings.POLYMARKET_SLUG or settings.POLYMARKET_SERIES_ID
    # The window that just opened was looked up before it opened: use that, no REST call now.
    nm = _next_market
    if (not force and nm.get("market") and nm.get("series") == curr_target and nm.get("start_ms") is not None
            and abs(nm["start_ms"] - cur_start_ms) < 1000):
        _cached_market, _cached_market_ts, _cached_market_series = nm["market"], now, curr_target
        nm.update(start_ms=None, market=None, tokens=[], series=None)
        return _cached_market
    if not force and _cached_market and _cached_market_series == curr_target:
        end_date = _cached_market.get("endDate")
        if end_date:
            try:
                end_ts = datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp()
                if end_ts > now + 15 and (now - _cached_market_ts) < 30.0:
                    return _cached_market
            except Exception:
                pass

    market = None
    try:
        market = await _discover_market(cur_start_ms)
    except Exception as e:
        print(f"Warning: Market discovery error: {e}")

    if market:
        _cached_market = market
        _cached_market_ts = now
        _cached_market_series = curr_target
        return market

    _cached_market = None
    _cached_market_ts = 0.0
    return None

async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    market = await get_active_market()
    if not market:
        return {"ok": False, "reason": "market_not_found"}

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    clob_token_ids = market.get("clobTokenIds", [])
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    outcome_prices = market.get("outcomePrices", [])
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    up_token_id = None
    down_token_id = None

    for i, outcome in enumerate(outcomes):
        token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
        if not token_id: continue
        if outcome.lower() == settings.POLYMARKET_UP_LABEL.lower():
            up_token_id = token_id
        elif outcome.lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down_token_id = token_id

    up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), -1)
    down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), -1)

    gamma_yes = float(outcome_prices[up_index]) if up_index >= 0 and up_index < len(outcome_prices) else None
    gamma_no = float(outcome_prices[down_index]) if down_index >= 0 and down_index < len(outcome_prices) else None

    if not up_token_id or not down_token_id:
        return {"ok": False, "reason": "missing_token_ids"}

    # Update active tokens in the WebSocket stream (+ the next window's, once it was looked up)
    polymarket_clob_ws.update_assets([up_token_id, down_token_id] + _next_window_tokens(market))

    up_ws = polymarket_clob_ws.get_token_market(up_token_id)
    down_ws = polymarket_clob_ws.get_token_market(down_token_id)

    # Use WS best_ask if available, else fallback to REST
    up_buy = up_ws.get("best_ask")
    down_buy = down_ws.get("best_ask")

    if up_buy is None or down_buy is None:
        try:
            up_rest_price, down_rest_price = await asyncio.gather(
                data.fetch_clob_price(up_token_id, "buy") if up_buy is None else asyncio.sleep(0, result=up_buy),
                data.fetch_clob_price(down_token_id, "buy") if down_buy is None else asyncio.sleep(0, result=down_buy)
            )
            if up_buy is None: up_buy = up_rest_price
            if down_buy is None: down_buy = down_rest_price
        except Exception:
            pass

    # Build orderbook summaries using WS orderbooks if available, fallback to REST
    up_book_summary = None
    down_book_summary = None

    if up_ws.get("bids") or up_ws.get("asks"):
        up_book_summary = data.summarize_order_book(up_ws)
    if down_ws.get("bids") or down_ws.get("asks"):
        down_book_summary = data.summarize_order_book(down_ws)

    if not up_book_summary or not down_book_summary:
        try:
            up_book, down_book = await asyncio.gather(
                data.fetch_order_book(up_token_id) if not up_book_summary else asyncio.sleep(0, result={}),
                data.fetch_order_book(down_token_id) if not down_book_summary else asyncio.sleep(0, result={})
            )
            if not up_book_summary: up_book_summary = data.summarize_order_book(up_book)
            if not down_book_summary: down_book_summary = data.summarize_order_book(down_book)
        except Exception:
            if not up_book_summary:
                up_book_summary = {"bestBid": None, "bestAsk": up_buy, "spread": None, "bidLiquidity": None, "askLiquidity": None}
            if not down_book_summary:
                down_book_summary = {"bestBid": None, "bestAsk": down_buy, "spread": None, "bidLiquidity": None, "askLiquidity": None}

    # Track whether the book data came from WS or REST
    book_source = "ws" if ((up_ws.get("bids") or up_ws.get("asks")) and (down_ws.get("bids") or down_ws.get("asks"))) else "rest"
    # Smooth out prices with previous valid state to prevent visual glitching if a poll momentarily drops
    final_up = up_buy if up_buy is not None else gamma_yes
    final_down = down_buy if down_buy is not None else gamma_no
    if final_up is not None:
        state["last_poly_up"] = final_up
    else:
        final_up = state.get("last_poly_up")

    if final_down is not None:
        state["last_poly_down"] = final_down
    else:
        final_down = state.get("last_poly_down")

    # Pre-warm active market tokens in CLOB client so order creation executes in <10ms
    if settings.PRIVATE_KEY:
        if up_token_id:
            clob_trader.warm_token(up_token_id)
        if down_token_id:
            clob_trader.warm_token(down_token_id)

    return {
        "ok": True,
        "market": market,
        "prices": {
            "up": final_up,
            "down": final_down
        },
        "token_ids": {
            "up": up_token_id,
            "down": down_token_id
        },
        "orderbook": {
            "up": up_book_summary,
            "down": down_book_summary
        },
        "book_source": book_source
    }

async def handle_order_gone_quiet(order_id: str, trade: Dict[str, Any], token_id: str,
                                  current_ask: float, limit_price: float, otype: str) -> bool:
    """Never assume an unindexed order terminated without fill.
    1. Explicitly cancel by ID on Polymarket.
    2. Sum all partial fills for THIS order ID across taker and maker records.
    3. If fills exist: record live fill (shares, real weighted average price, cost).
    4. Else: log confirmed cancelled, no fill.
    """
    side = trade.get("side", "UNKNOWN")
    # 1. Never assume. Cancel explicitly by ID.
    cancel_success = False
    try:
        c_res = await asyncio.to_thread(clob_trader.cancel_order, order_id)
        cancel_success = bool(c_res and c_res.get("ok"))
    except Exception as e:
        log_message(f"cancel {order_id} failed: {e}")

    # Brief buffer for indexer to catch any matching engine fills
    await asyncio.sleep(0.5)

    # Double-check: if cancel call failed or unconfirmed, ensure emergency cancel
    if not cancel_success:
        try:
            ord_check = await asyncio.to_thread(clob_trader.check_order_status, order_id)
            if ord_check.get("status") == "OPEN":
                log_message(f"Warning: order {order_id} still open on book; sending emergency cancel_order")
                await asyncio.to_thread(clob_trader.cancel_order, order_id)
        except Exception:
            pass

    # 2. Check what actually happened to THIS order (sum all partial fills).
    fills = await asyncio.to_thread(clob_trader.get_order_fills, order_id, token_id)
    if fills:
        total_shares = sum(float(f["size"]) for f in fills)
        total_usd = sum(float(f["size"]) * float(f["price"]) for f in fills)
        avg_price = total_usd / total_shares if total_shares > 0 else limit_price

        trade["live_status"] = "FILLED"
        trade["live_entry_price"] = avg_price
        trade["live_shares"] = total_shares
        trade["live_amount"] = total_usd
        trade["live_entry_time"] = datetime.now().isoformat()
        trade["live_slippage"] = avg_price - current_ask

        # Deduct spent funds in RAM
        if total_usd > 0:
            new_bal = max(0.0, (state.get("live_balance") or 0.0) - total_usd)
            state["live_balance"] = new_bal
            risk_type = (settings.RISK_TYPE or "percent").lower()
            if risk_type == "fixed":
                new_stake = float(settings.RISK_VALUE)
            else:
                new_stake = (float(settings.RISK_VALUE) / 100.0) * new_bal
            state["precomputed_live_stake"] = round(max(1.0, min(new_stake, new_bal)) if new_bal >= 1.0 else 0.0, 2)

        save_state()
        _sync_active_trades_to_latest_data()
        await broadcast_state()

        msg = (f"LIVE {otype} fill confirmed across {len(fills)} piece(s): {side} {total_shares:.2f} shares @ {avg_price:.4f} "
               f"(${total_usd:.2f}, order {order_id})")
        log_message(msg)
        await send_telegram(f"🚀 *LIVE Copy FILLED ({otype} Limit)*\n• Side: `{side}`\n• Fill Price: `{avg_price:.4f}`\n• Shares: `{total_shares:.2f}`\n• Amount: `${total_usd:.2f}`\n• Pieces: `{len(fills)}`\n• Market: `{trade['market_slug']}`")
        return True
    else:
        log_message(f"{otype} order {order_id} confirmed cancelled on exchange, no fill")
        trade["live_status"] = "CANCELLED"
        save_state()
        _sync_active_trades_to_latest_data()
        await broadcast_state()
        return False

# ── Long-shot fills (PATCH: longshot) ─────────────────────────────────────────
async def _fak_fill(res: Dict[str, Any], token_id: str, req_shares: float):
    """(shares, dollars, status) a fill-and-kill buy got. From Polymarket's answer first
    (takingAmount = shares, makingAmount = dollars for a buy; read as 6-decimal fixed-math when
    the number is far above what was ordered), else from the order's own fills. A "delayed"
    order has no fills yet: its fills are looked up for a few seconds."""
    data = res.get("response") or {}
    st = str(data.get("status") or "").lower()

    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0
    sh, usd = num(data.get("takingAmount")), num(data.get("makingAmount"))
    if sh > max(float(req_shares), 1.0) * 1.5:
        sh, usd = sh / 1e6, usd / 1e6
    if sh > 0 and usd > 0:
        return sh, usd, st
    if st in ("matched", "delayed", ""):
        for delay in ((1.0, 1.5, 2.5) if st == "delayed" else (0.3, 1.0)):
            await asyncio.sleep(delay)
            fills = await asyncio.to_thread(clob_trader.get_order_fills, res.get("order_id"), token_id)
            if fills:
                return (sum(float(f["size"]) for f in fills),
                        sum(float(f["size"]) * float(f["price"]) for f in fills), st)
    return 0.0, 0.0, st


def _ls_add_fill(trade: Dict[str, Any], shares: float, usd: float, quoted_ask: float):
    """Adds one fill-and-kill fill to a long shot (several orders can fill one long shot)."""
    trade["ls_fak_shares"] = float(trade.get("ls_fak_shares") or 0.0) + shares
    trade["ls_fak_usd"] = float(trade.get("ls_fak_usd") or 0.0) + usd
    trade["live_shares"] = trade["ls_fak_shares"]
    trade["live_amount"] = trade["ls_fak_usd"]
    trade["live_entry_price"] = trade["ls_fak_usd"] / trade["ls_fak_shares"]
    trade["live_entry_time"] = trade.get("live_entry_time") or datetime.now().isoformat()
    trade["live_slippage"] = trade["live_entry_price"] - quoted_ask
    if usd > 0:                                   # spent funds come off the balance in RAM
        new_bal = max(0.0, (state.get("live_balance") or 0.0) - usd)
        state["live_balance"] = new_bal
        risk_type = (settings.RISK_TYPE or "percent").lower()
        new_stake = float(settings.RISK_VALUE) if risk_type == "fixed" else (float(settings.RISK_VALUE) / 100.0) * new_bal
        state["precomputed_live_stake"] = round(max(1.0, min(new_stake, new_bal)) if new_bal >= 1.0 else 0.0, 2)


async def _ls_close_out(trade: Dict[str, Any], status_if_empty: str, why: str):
    """Ends a fill-and-kill long shot: FILLED with what its orders bought, else status_if_empty."""
    if float(trade.get("ls_fak_shares") or 0.0) > 0:
        trade["live_status"] = "FILLED"
        msg = (f"LIVE long shot FILLED (FAK, {trade.get('live_attempts', 1)} order(s)): {trade['side']} ${trade['live_amount']:.2f} "
               f"on {trade['market_slug']} — {trade['live_shares']:.2f} shares @ {trade['live_entry_price']:.4f} "
               f"(paper entry {trade['paper_entry_price']:.4f})" + (f"; {why}" if why else ""))
        log_message(msg)
        await send_telegram(f"🚀 *LIVE Long Shot FILLED (FAK)*\n• Side: `{trade['side']}`\n• Fill Price: `{trade['live_entry_price']:.4f}`\n"
                            f"• Shares: `{trade['live_shares']:.2f}`\n• Amount: `${trade['live_amount']:.2f}`\n• Market: `{trade['market_slug']}`")
    else:
        trade["live_status"] = status_if_empty
        log_message(f"LONGSHOT live copy stopped for {trade['market_slug']}: {why}")
    await asyncio.to_thread(save_state)
    _sync_active_trades_to_latest_data()
    await broadcast_state()


# ── Maker long shots (PATCH: longshot maker) ──────────────────────────────────
# bot/ls_maker.py keeps the resting bids; these are its links to the rest of the bot.
def _mk_is_live() -> bool:
    """Real orders only with a private key and both long shots and maker bids set live."""
    return bool(settings.PRIVATE_KEY) and longshot.maker_live(settings.LONGSHOT)


def _mk_running() -> bool:
    return bool(state.get("running")) and state.get("withdraw_state") != "in_progress"


async def _mk_place(token_id: str, shares: float, price: float) -> Dict[str, Any]:
    """A post-only GTC bid: Polymarket refuses it rather than letting it take the ask."""
    return await clob_trader.place_limit_buy_async(token_id, shares, price, None, longshot.tick_size_str(price),
                                                   "GTC", post_only=True)


async def _mk_cancel(order_id: str) -> Dict[str, Any]:
    return await asyncio.to_thread(clob_trader.cancel_order, order_id)


async def _mk_get_order(order_id: str) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(clob_trader.get_order, order_id)


def _mk_book(token_id: str) -> Optional[Dict[str, Any]]:
    """Best ask and ask levels from the order-book stream (a quiet book is still a valid book)."""
    s = polymarket_clob_ws.get_summary(token_id, max_age_s=60.0) if token_id else None
    if not s:
        return None
    asks = []
    for a in s.get("asks") or []:
        try:
            asks.append((float(a.get("px", a.get("price"))), float(a.get("size") or 0.0)))
        except (TypeError, ValueError):
            pass
    return {"best_ask": s.get("bestAsk"), "asks": asks}


def _mk_balance(live: bool) -> float:
    return float(state.get("live_balance") or 0.0) if live else float(state.get("paper_balance") or 0.0)


def _mk_persist(ids: List[str]) -> None:
    """Live bids on the book, saved so the next start can cancel them if the bot stopped meanwhile."""
    if list(state.get("ls_maker_open") or []) != list(ids):
        state["ls_maker_open"] = list(ids)
        try:
            asyncio.get_running_loop().create_task(asyncio.to_thread(save_state))
        except RuntimeError:
            save_state()


async def _mk_on_fill(bid: "ls_maker.Bid", shares: float, usd: float, via: str) -> None:
    """A maker bid bought `shares` for `usd` (always at its own price). The first fill makes a
    long-shot trade (open_reason "longshot_maker"); later fills add to it. It settles, redeems
    and shows like any long shot. Paper bids only move the paper balance."""
    t = bid.trade
    first = t is None
    if first:
        m = bid.meta or {}
        market = m.get("market") or {}
        prob = float(bid.last_prob)
        t = {
            "trade_id": f"{market.get('id')}_{int(time.time() * 1000)}_lsm",
            "market_id": market.get("id"),
            "market_slug": market.get("slug"),
            "side": bid.side,
            "token_id": bid.token_id,
            "paper_entry_price": bid.price,
            "paper_amount": 0.0,
            "paper_shares": 0.0,
            "paper_entry_time": datetime.now().isoformat(),
            "paper_close_price": None,
            "paper_closed_time": None,
            "paper_profit_loss": None,
            "live_status": "FILLED" if bid.live else ("PAPER_ONLY" if settings.PRIVATE_KEY else "NO_CREDS"),
            "live_attempts": 1 if bid.live else 0,
            "live_entry_price": None,
            "live_amount": None,
            "live_shares": None,
            "live_entry_time": None,
            "live_order_id": bid.order_id if bid.live else None,
            "live_mark_price": None,
            "live_unrealized_pl": None,
            "live_close_price": None,
            "live_closed_time": None,
            "live_profit_loss": None,
            "status": "OPEN",
            "awaiting_resolution": False,
            "strike_price": m.get("strike"),
            "strike_source": m.get("strike_src"),
            "window_start_ms": bid.start_ms,
            "open_reason": "longshot_maker",
            "end_ts": m.get("end_ts"),
            "mode": "hybrid",
            "kind": "longshot",
            "longshot": dict(m.get("info") or {}, band="maker", order_type="MAKER", prob=prob,
                             multiple=(prob / bid.price) if bid.price > 0 else None, max_price=bid.price,
                             bid_shares=bid.shares, prob_at_bid=bid.prob_at_place,
                             secs_left=round(float(m.get("end_ts") or 0.0) - time.time(), 2), fill_via=via),
        }
        if bid.live:
            t["live_order_ids"] = [bid.order_id]
        bid.trade = t
        state["active_trades"].append(t)
    t["paper_shares"] = float(t.get("paper_shares") or 0.0) + shares
    t["paper_amount"] = float(t.get("paper_amount") or 0.0) + usd
    state["paper_balance"] = float(state.get("paper_balance") or 0.0) - usd
    if bid.live:
        _ls_add_fill(t, shares, usd, bid.price)       # live shares / amount / entry price, live balance
        t["live_status"] = "FILLED"
    vias = str(t["longshot"].get("fill_via") or "")
    if via not in vias.split("+"):
        t["longshot"]["fill_via"] = f"{vias}+{via}" if vias else via
    mode = "LIVE" if bid.live else "PAPER"
    log_message(f"LONGSHOT MAKER {mode} fill: {bid.side} {shares:g} shares @ {bid.price:.3f} (${usd:.2f}, via {via}) "
                f"on {t['market_slug']} — bid {bid.filled:g}/{bid.shares:g} filled, chance {bid.last_prob:.3f}")
    if first:
        asyncio.create_task(send_telegram(
            f"{'🚀' if bid.live else '🟢'} *{mode} Maker Long Shot Filled*\n• Side: `{bid.side}`\n• Price: `{bid.price:.3f}`\n"
            f"• Shares: `{shares:g}`\n• Amount: `${usd:.2f}`\n• Market: `{t['market_slug']}`"))
    asyncio.create_task(asyncio.to_thread(save_state))
    _sync_active_trades_to_latest_data()
    asyncio.create_task(broadcast_state())


ls_maker_mgr = ls_maker.MakerManager(SimpleNamespace(
    get_cfg=lambda: settings.LONGSHOT, is_live=_mk_is_live, running=_mk_running,
    place=_mk_place, cancel=_mk_cancel, get_order=_mk_get_order, book=_mk_book,
    trades_since=lambda tok, ts: polymarket_clob_ws.recent_trades(tok, ts) if tok else [],
    balance=_mk_balance, on_fill=_mk_on_fill, log=log_message, persist=_mk_persist,
    orphans=lambda: list(state.get("ls_maker_open") or []) if settings.PRIVATE_KEY else []))


async def _continuous_live_copy_fill(trade: Dict[str, Any], market: Dict[str, Any], token_ids: Dict[str, Any], evaluated_price: Optional[float] = None, ask_liq_usd: Optional[float] = None,
                                     order_sent: Optional[asyncio.Event] = None):
    token_id = trade.get("token_id")
    side = trade.get("side")
    # PATCH: longshot — long shots use their own slot keys, stake, price cap, tick, order type and timings
    is_ls = _is_longshot(trade)
    ls_cfg = settings.LONGSHOT if is_ls else {}
    slot_keys = _slot_keys(market, "longshot" if is_ls else "regular")

    if not token_id:
        if order_sent is not None:
            order_sent.set()
        trade["live_status"] = "FAILED"
        log_message(f"LIVE copy aborted: missing token_id for side {side}")
        for k in slot_keys:
            _in_flight_markets.discard(k)
        return

    # (the "LIVE copy started" line is written after the first order went out: no file write before it)
    retry_interval = max(0.01, float(getattr(settings, "COPY_RETRY_INTERVAL_MS", 300) or 300) / 1000.0)
    if is_ls:
        max_attempts = int(ls_cfg.get("max_attempts", 3) or 3)
        resting_s = float(ls_cfg.get("resting_s", 2.0) or 2.0)
        ls_fak = str(ls_cfg.get("order_type", "FAK")).upper() == "FAK"
    else:
        max_attempts = int(getattr(settings, "COPY_MAX_RETRIES", 5) or 5)
        resting_s = float(getattr(settings, "COPY_RESTING_TIMEOUT_S", 2.0) or 2.0)
        ls_fak = False

    try:
        while trade["status"] == "OPEN" and trade.get("live_status") == "FILLING":
            now_ts = time.time()
            # 1. Check if window is close to expiry (<15s)
            if trade.get("end_ts") and now_ts >= trade["end_ts"]:
                if ls_fak:
                    await _ls_close_out(trade, "TIMEOUT", "window closed")
                    break
                trade["live_status"] = "TIMEOUT"
                log_message(f"LIVE copy stopped for {trade['market_slug']}: window expired")
                await asyncio.to_thread(save_state)
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                break

            attempt_num = trade.get("live_attempts", 0) + 1
            trade["live_attempts"] = attempt_num

            if attempt_num > max_attempts:
                if ls_fak:
                    await _ls_close_out(trade, "TIMEOUT", f"max attempts reached ({max_attempts})")
                    break
                trade["live_status"] = "TIMEOUT"
                log_message(f"LIVE copy max attempts reached ({max_attempts}) for {trade['market_slug']} — halting to prevent duplicate orders")
                await asyncio.to_thread(save_state)
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                break

            # 2. Get freshest book ask for this token:
            # Attempt #1 uses evaluated_price instantly with 0ms RAM lookup.
            # Subsequent attempts (re-quotes) MUST refresh the live ask from WebSocket and live CLOB REST API!
            if attempt_num == 1 and evaluated_price and evaluated_price > 0:
                current_ask = evaluated_price
                cur_ask_liq_usd = ask_liq_usd
            else:
                # Priority 1: WebSocket cache with relaxed age window
                summary = polymarket_clob_ws.get_summary(token_id, max_age_s=15.0)
                current_ask = summary.get("bestAsk") if summary else None
                ask_liq_sh = summary.get("askLiquidity") if summary else None
                cur_ask_liq_usd = (ask_liq_sh * current_ask) if (ask_liq_sh is not None and current_ask and current_ask > 0) else None

                # Priority 2: Direct query to CLOB API to guarantee 100% fresh on-exchange live price
                if not current_ask or current_ask <= 0:
                    try:
                        from bot.data import fetch_clob_price
                        clob_p = await fetch_clob_price(token_id, "buy")
                        if clob_p and clob_p > 0:
                            current_ask = float(clob_p)
                    except Exception:
                        pass

                # Priority 3: Check raw WebSocket book without age filtering
                if not current_ask or current_ask <= 0:
                    raw_book = polymarket_clob_ws.get_token_market(token_id)
                    asks = raw_book.get("asks", [])
                    if asks:
                        try:
                            current_ask = float(asks[0].get("px", asks[0]["price"]))
                        except Exception:
                            pass

                # Priority 4: Fallback to initial price only if all real-time network/cache sources failed
                if not current_ask or current_ask <= 0:
                    current_ask = evaluated_price or trade.get("paper_entry_price") or 0.50

                if cur_ask_liq_usd is None or cur_ask_liq_usd <= 0:
                    cur_ask_liq_usd = ask_liq_usd

            # 3. Sizing: Instant RAM lookup of precomputed stake from background updater (0ms latency)
            live_balance = state.get("live_balance") or 0.0
            live_amount = state.get("precomputed_live_stake") or 0.0

            # Safety fallback if background updater hasn't populated yet
            if live_balance <= 0 or live_amount <= 0:
                live_balance = await asyncio.to_thread(clob_trader.get_usdc_balance) or 0.0
                state["live_balance"] = live_balance
                risk_type = (settings.RISK_TYPE or "percent").lower()
                if risk_type == "fixed":
                    live_amount = float(settings.RISK_VALUE)
                else:
                    live_amount = (float(settings.RISK_VALUE) / 100.0) * live_balance
                live_amount = max(1.0, min(live_amount, live_balance)) if live_balance >= 1.0 else 0.0
                state["precomputed_live_stake"] = round(live_amount, 2)

            if is_ls:
                # PATCH: longshot — the long-shot stake (fixed, % or growth-max, per band), a limit of
                # ask + slippage never above the band's cap, on the book's tick, whole shares whose
                # cost is whole cents, and its own order type (FAK by default). No growing sweep.
                lsd = trade.get("longshot") or {}
                max_px = float(lsd.get("max_price") or current_ask)
                if current_ask > max_px + 1e-9:
                    if ls_fak:
                        await _ls_close_out(trade, "PRICE_ABOVE_CAP", f"ask {current_ask:.3f} is above the {max_px:.3f} cap")
                    else:
                        trade["live_status"] = "PRICE_ABOVE_CAP"
                        log_message(f"LONGSHOT live copy stopped for {trade['market_slug']}: ask {current_ask:.3f} is above the {max_px:.3f} cap")
                        await asyncio.to_thread(save_state)
                        _sync_active_trades_to_latest_data()
                        await broadcast_state()
                    break
                if trade.get("ls_target_usd") is None:
                    _target = longshot.stake_for(live_balance, ls_cfg, lsd.get("band"), lsd.get("prob"),
                                                 longshot.taker_price(ls_cfg, current_ask))  # PATCH: longshot maker — fee
                    if _target <= 0:
                        trade["live_status"] = "STAKE_ZERO"
                        log_message(f"LONGSHOT live copy stopped for {trade['market_slug']}: the stake is $0 "
                                    f"(chance {float(lsd.get('prob') or 0):.3f} vs ask {current_ask:.3f})")
                        await asyncio.to_thread(save_state)
                        _sync_active_trades_to_latest_data()
                        await broadcast_state()
                        break
                    trade["ls_target_usd"] = round(_target, 2)
                _spent = float(trade.get("ls_fak_usd") or 0.0)
                ls_usd = float(trade["ls_target_usd"]) - _spent
                if _spent > 0 and ls_usd < 1.0:
                    await _ls_close_out(trade, "FILLED", "")       # what's left is under the $1 minimum
                    break
                if cur_ask_liq_usd is not None and cur_ask_liq_usd >= 1.0:
                    ls_usd = min(ls_usd, cur_ask_liq_usd)
                gtd_price = longshot.limit_price(current_ask, float(ls_cfg.get("slippage", 0.01)), max_px)
                gtd_tick = longshot.tick_size_str(gtd_price)
                gtd_shares = longshot.order_shares(max(1.0, ls_usd), gtd_price, max_usd=live_balance)
                order_type = "FAK" if ls_fak else "GTD"
                if gtd_shares * gtd_price > live_balance + 1e-9:
                    _why = (f"deposit balance too low (${live_balance:.2f} < ${gtd_shares * gtd_price:.2f} "
                            f"minimum order @ {gtd_price:.3f})")
                    if ls_fak:
                        await _ls_close_out(trade, "INSUFFICIENT_FUNDS", _why)
                    else:
                        trade["live_status"] = "INSUFFICIENT_FUNDS"
                        log_message(f"LONGSHOT live copy stopped for {trade['market_slug']}: {_why}")
                        await asyncio.to_thread(save_state)
                        _sync_active_trades_to_latest_data()
                        await broadcast_state()
                    break
            else:
                # Polymarket order size minimum: at least 5 shares and at least $1.00
                min_order_cost = round(max(1.00, math.ceil(5.0 * current_ask * 100.0) / 100.0), 2)
                if live_balance < min_order_cost:
                    trade["live_status"] = "INSUFFICIENT_FUNDS"
                    log_message(f"LIVE copy halted for {trade['market_slug']}: deposit balance too low (${live_balance:.2f} < ${min_order_cost:.2f} minimum for 5 shares @ {current_ask:.4f})")
                    await asyncio.to_thread(save_state)
                    _sync_active_trades_to_latest_data()
                    await broadcast_state()
                    break

                if live_amount < min_order_cost:
                    live_amount = min_order_cost

                if cur_ask_liq_usd is not None and cur_ask_liq_usd >= min_order_cost and live_amount > cur_ask_liq_usd:
                    live_amount = round(cur_ask_liq_usd, 2)

                # 4. Place GTD limit BUY order directly with progressive sweep
                extra_slip = min(0.08, (attempt_num - 1) * 0.015)
                gtd_price = min(0.99, round(current_ask + settings.CLOB_MAX_SLIPPAGE + extra_slip, 4))
                safe_usd = max(1.00, live_amount)
                # Guarantee shares >= 5.0 and shares * price >= 1.00 (Polymarket min order size rule: min 5 shares)
                min_shares = 5.0
                gtd_shares = max(min_shares, math.ceil((safe_usd / gtd_price) * 100.0) / 100.0)
                while round(gtd_shares * gtd_price, 4) < 1.00:
                    gtd_shares = round(gtd_shares + 0.01, 2)
                gtd_tick, order_type = "0.01", "GTD"

            market_end = trade.get("end_ts") or 0
            expiration = int(max(time.time() + 300, market_end))

            _t_send = time.perf_counter()
            gtd_res = await clob_trader.place_limit_buy_async(token_id, gtd_shares, gtd_price, expiration, gtd_tick, order_type)
            _send_ms = (time.perf_counter() - _t_send) * 1000.0
            if order_sent is not None:
                order_sent.set()
            if attempt_num == 1:
                log_message(f"LIVE copy started for {trade['market_slug']} ({side}{', long shot' if is_ls else ''}) — continuous {order_type} fill loop initiated")
            # (same start as before, so anything that reads these lines still matches; now written
            # once the order is out, with how long Polymarket took to answer)
            log_message(f"LIVE copy attempt #{attempt_num} for {side}{' long shot' if is_ls else ''}: placing {order_type} limit order for {gtd_shares:.2f} shares @ {gtd_price:.4f} "
                        + (f"(exp: {expiration})" if order_type == "GTD" else "(fill-and-kill)")
                        + f"... sent, Polymarket answered in {_send_ms:.0f} ms")

            if ls_fak:
                # PATCH: longshot — fill-and-kill: what it bought is known now and nothing rests on
                # the book. More orders go out for what's still unfilled (up to max_attempts) while
                # the ask stays under the cap; a "delayed" order ends it (its result comes later).
                if gtd_res.get("ok") and gtd_res.get("order_id"):
                    _oid = gtd_res["order_id"]
                    trade.setdefault("live_order_ids", []).append(_oid)
                    trade["live_order_id"] = _oid
                    trade["live_limit_price"] = gtd_price
                    trade["live_quoted_price"] = current_ask
                    _sh, _usd, _st = await _fak_fill(gtd_res, token_id, gtd_shares)
                    if _sh > 0:
                        _ls_add_fill(trade, _sh, _usd, current_ask)
                        log_message(f"LONGSHOT FAK attempt #{attempt_num} bought {_sh:.2f} shares for ${_usd:.2f} "
                                    f"(avg {_usd / _sh:.4f}, order {_oid})")
                    else:
                        log_message(f"LONGSHOT FAK attempt #{attempt_num}: nothing bought at or below {gtd_price:.3f} "
                                    f"(order {_oid}, status {_st or '?'})")
                    if _st == "delayed":
                        await _ls_close_out(trade, "NO_FILL", "Polymarket delayed the order; its fills are checked again when the window settles")
                        break
                else:
                    err = clob_trader.enrich_order_error(str(gtd_res.get("error", "fak_placement_failed")))
                    if any(k in err for k in ("setup_failed", "client_not_ready", "invalid_private_key", "not enough balance", "balance is not enough")):
                        await _ls_close_out(trade, "FAILED", f"order refused: {err}")
                        break
                    if "no orders found to match" in err.lower():
                        log_message(f"LONGSHOT FAK attempt #{attempt_num}: nothing offered at or below {gtd_price:.3f}")
                    else:
                        log_message(f"LONGSHOT FAK attempt #{attempt_num} failed: {err}")
                _spent = float(trade.get("ls_fak_usd") or 0.0)
                if _spent > 0 and float(trade["ls_target_usd"]) - _spent < 1.0:
                    await _ls_close_out(trade, "FILLED", "")
                    break
                if attempt_num >= max_attempts:
                    await _ls_close_out(trade, "NO_FILL", f"{'stake not fully bought' if _spent > 0 else 'nothing bought'} "
                                                          f"with {max_attempts} fill-and-kill order(s)")
                    break
                await asyncio.sleep(retry_interval)
                continue

            if gtd_res.get("ok") and gtd_res.get("order_id"):
                order_id = gtd_res["order_id"]
                order_placed_ts = time.time()
                trade["live_status"] = "GTD_OPEN"
                trade["live_order_id"] = order_id
                trade["live_limit_price"] = gtd_price
                trade["live_quoted_price"] = current_ask
                trade["live_expiration"] = expiration
                save_state()
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                log_message(f"LIVE GTD limit order placed for {side}: {gtd_shares:.2f} shares @ {gtd_price:.4f} (order {order_id}) — resting on book")

                # Monitor GTD order until filled, window expires (<15s), or 4s re-quote timeout
                while trade["status"] == "OPEN" and trade.get("live_status") == "GTD_OPEN":
                    now_ts = time.time()
                    if trade.get("end_ts") and now_ts >= trade["end_ts"]:
                        log_message(f"Cancelling resting GTD order {order_id}: window expired")
                        filled = await handle_order_gone_quiet(order_id, trade, token_id, current_ask, gtd_price, "GTD")
                        if not filled:
                            trade["live_status"] = "TIMEOUT"
                            save_state()
                            _sync_active_trades_to_latest_data()
                            await broadcast_state()
                        return

                    # Check status on CLOB
                    ord_status = await asyncio.to_thread(clob_trader.check_order_status, order_id)
                    st = ord_status.get("status")

                    if st == "FILLED":
                        # Never assume fills or prices: verify exact trade fills directly from CLOB API!
                        fills = await asyncio.to_thread(clob_trader.get_order_fills, order_id, token_id)
                        if fills:
                            total_shares = sum(float(f["size"]) for f in fills)
                            total_usd = sum(float(f["size"]) * float(f["price"]) for f in fills)
                            fill_size = total_shares
                            fill_price = total_usd / total_shares if total_shares > 0 else float(ord_status.get("price") or gtd_price)
                            fill_usd = total_usd
                        else:
                            fill_size = float(ord_status.get("size_matched") or gtd_shares)
                            fill_price = float(ord_status.get("price") or gtd_price)
                            fill_usd = fill_size * fill_price

                        trade["live_status"] = "FILLED"
                        trade["live_entry_price"] = fill_price
                        trade["live_shares"] = fill_size
                        trade["live_amount"] = fill_usd
                        trade["live_entry_time"] = datetime.now().isoformat()
                        trade["live_slippage"] = fill_price - current_ask

                        # Immediately deduct spent funds in RAM
                        if fill_usd > 0:
                            new_bal = max(0.0, (state.get("live_balance") or 0.0) - fill_usd)
                            state["live_balance"] = new_bal
                            risk_type = (settings.RISK_TYPE or "percent").lower()
                            if risk_type == "fixed":
                                new_stake = float(settings.RISK_VALUE)
                            else:
                                new_stake = (float(settings.RISK_VALUE) / 100.0) * new_bal
                            state["precomputed_live_stake"] = round(max(1.0, min(new_stake, new_bal)) if new_bal >= 1.0 else 0.0, 2)

                        save_state()
                        _sync_active_trades_to_latest_data()
                        await broadcast_state()

                        msg = (f"LIVE trade FILLED via GTD limit (attempt #{attempt_num}): {side} ${fill_usd:.2f} on {trade['market_slug']} "
                               f"— {fill_size:.2f} shares @ {fill_price:.4f} (paper entry {trade['paper_entry_price']:.4f}, order {order_id})")
                        log_message(msg)
                        await send_telegram(f"🚀 *LIVE Copy FILLED (GTD Limit)*\n• Side: `{side}`\n• Fill Price: `{fill_price:.4f}`\n• Shares: `{fill_size:.2f}`\n• Amount: `${fill_usd:.2f}`\n• Market: `{trade['market_slug']}`")
                        return

                    # Check if partial fill happened while resting on book
                    matched = ord_status.get("size_matched", 0.0)
                    if matched > 0 and matched != trade.get("live_shares"):
                        trade["live_shares"] = matched
                        trade["live_entry_price"] = float(ord_status.get("price") or gtd_price)
                        save_state()
                        _sync_active_trades_to_latest_data()
                        await broadcast_state()

                    # Explicit cancellation confirmed from exchange
                    if st == "CANCELLED":
                        await handle_order_gone_quiet(order_id, trade, token_id, current_ask, gtd_price, "GTD")
                        return

                    # Check if market ask has surged above our resting limit price
                    # If market jumped away, cancel early to re-quote at the higher price instead of waiting 4s passively
                    if now_ts - order_placed_ts >= 1.5:
                        ws_sum = polymarket_clob_ws.get_summary(token_id, max_age_s=5.0)
                        ws_ask = ws_sum.get("bestAsk") if ws_sum else None
                        if ws_ask and ws_ask > gtd_price:
                            log_message(f"Market ask surged ({ws_ask:.4f} > limit {gtd_price:.4f}) — cancelling resting GTD early to re-quote at fresh ask...")
                            filled = await handle_order_gone_quiet(order_id, trade, token_id, current_ask, gtd_price, "GTD")
                            if filled:
                                return
                            if attempt_num < max_attempts:
                                trade["live_status"] = "FILLING"
                                break

                    # 4-Second Resting Lifetime (long shots: settings longshot.resting_s):
                    # Enforce the full resting window. Once reached, cancel and verify all partial fills!
                    if now_ts - order_placed_ts >= resting_s:
                        log_message(f"Resting GTD order {order_id} reached {resting_s:g}s timeout — verifying fills and cancelling on exchange...")
                        filled = await handle_order_gone_quiet(order_id, trade, token_id, current_ask, gtd_price, "GTD")
                        if filled:
                            return

                        if attempt_num < max_attempts:
                            log_message(f"Resting GTD order {order_id} cancelled on exchange — re-quoting at freshest ask (attempt #{attempt_num + 1})...")
                            trade["live_status"] = "FILLING"
                            break
                        else:
                            log_message(f"GTD order {order_id} cancelled on exchange after max attempts ({max_attempts}) — stopping order placement for this window to prevent duplicate entry.")
                            trade["live_status"] = "CANCELLED"
                            save_state()
                            _sync_active_trades_to_latest_data()
                            await broadcast_state()
                            return

                    await asyncio.sleep(0.5)

                if trade.get("live_status") == "FILLED":
                    return
            else:
                err = clob_trader.enrich_order_error(str(gtd_res.get("error", "gtd_placement_failed")))
                if any(k in err for k in ("setup_failed", "client_not_ready", "invalid_private_key", "not enough balance", "balance is not enough")):
                    trade["live_status"] = "FAILED"
                    log_message(f"LIVE copy aborted: {err}")
                    save_state()
                    _sync_active_trades_to_latest_data()
                    await broadcast_state()
                    return

                log_message(f"LIVE copy attempt #{attempt_num} for {side} GTD placement failed: {err} — retrying in {int(retry_interval * 1000)}ms...")

            await asyncio.sleep(retry_interval)
    finally:
        if order_sent is not None:
            order_sent.set()
        for k in slot_keys:
            _in_flight_markets.discard(k)


_BG_FUTURES: set = set()  # keeps fire-and-forget order tasks referenced until they finish


async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], strike_open: Optional[float], token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None,
                        strike_source: str = "chainlink_ws", window_start_ms: Optional[int] = None,
                        open_reason: str = "ev_entry", kind: str = "regular"):
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    cur_mkt_id = str(market.get("id") or "")
    cur_slug = str(market.get("slug") or "")
    is_ls = kind == "longshot"  # PATCH: longshot
    ls_cfg = settings.LONGSHOT if is_ls else {}
    slot_keys = _slot_keys(market, kind)

    async with _trade_lock:
        if any(k in _in_flight_markets for k in slot_keys):
            return "slot_busy"

        def _same_window(t):
            return ((cur_mkt_id and str(t.get("market_id")) == cur_mkt_id)
                    or (cur_slug and t.get("market_slug") == cur_slug)
                    or (window_start_ms is not None and t.get("window_start_ms") == int(window_start_ms)))

        if is_ls:
            # PATCH: longshot — up to longshot.max_per_window long shots per window;
            # the regular trade (if any) doesn't count.
            # (PATCH: longshot maker — maker fills have their own cap, maker.max_fills_per_window)
            n_ls = sum(1 for t in state["active_trades"] if _is_longshot(t) and not _is_ls_maker(t) and _same_window(t))
            if n_ls >= int(ls_cfg.get("max_per_window", 1)):
                return "slot_busy"
        else:
            # CONSTRAINT: Only one position per market window (do NOT block entering a new window while an old one resolves)
            # PATCH: longshot — long-shot trades don't take the regular slot.
            for t in state["active_trades"]:
                if not _is_longshot(t) and _same_window(t):
                    return "slot_busy"

        if state["withdraw_state"] == "in_progress":
            return "withdraw_in_progress"

        if strike_open is None:
            return "no_strike"

        side = decision["side"]
        price = market_prices["up"] if side == "UP" else market_prices["down"]
        if price is None:
            return "no_price"

        # Internal paper balance risk sizing
        balance = state["paper_balance"]
        if balance <= 0 and settings.PRIVATE_KEY:
            balance = state["paper_balance"] = float(settings.PAPER_BALANCE_USD or 1000.0)
        risk_type = (settings.RISK_TYPE or "percent").lower()
        if is_ls:  # PATCH: longshot — fixed, % of balance or growth-max (per band if the band has its own)
            # (PATCH: longshot maker — a growth stake prices the share with the taker fee)
            paper_amount = longshot.stake_for(balance, ls_cfg, decision.get("band"), decision.get("prob"),
                                              longshot.taker_price(ls_cfg, price))
        elif risk_type == "fixed":
            paper_amount = float(settings.RISK_VALUE)
        else:
            paper_amount = (float(settings.RISK_VALUE) / 100.0) * balance

        if paper_amount <= 0:
            return "stake_zero"

        ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
        ask_liq_shares = ob.get("askLiquidity")
        ask_liq_usd = None
        min_liq = float(ls_cfg.get("min_ask_liquidity_usd", 0.0)) if is_ls else settings.MIN_BOOK_LIQUIDITY_USD
        if ask_liq_shares is not None and price > 0:
            ask_liq_usd = ask_liq_shares * price
            if ask_liq_usd < min_liq:
                _log_once(f"thin:{'ls:' if is_ls else ''}{cur_slug or cur_mkt_id}:{side}",
                          f"Skip {side}{' long shot' if is_ls else ''}: thin book (${ask_liq_usd:.2f} ask liquidity)")
                return "thin_book"
            paper_amount = min(paper_amount, ask_liq_usd)

        if settings.PRIVATE_KEY and balance < paper_amount and balance > 0:
            paper_amount = balance

        if balance < paper_amount or paper_amount <= 0:
            if f"bal:{cur_slug}" not in _logged_once:
                _logged_once[f"bal:{cur_slug}"] = time.time()
                print(f"Insufficient internal paper balance ({balance})")
            return "insufficient_balance"

        end_date_str = market.get("endDate")
        end_ts = 0
        if end_date_str:
            try:
                end_ts = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).timestamp()
            except Exception:
                pass
        if not end_ts:
            end_ts = time.time() + settings.CANDLE_WINDOW_MINUTES * 60

        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        trade_id = f"{cur_mkt_id}_{int(time.time() * 1000)}{'_ls' if is_ls else ''}"
        live_on = bool(settings.PRIVATE_KEY) and (not is_ls or bool(ls_cfg.get("live", True)))

        trade = {
            "trade_id": trade_id,
            "market_id": market["id"],
            "market_slug": market.get("slug"),
            "side": side,
            "token_id": token_id,

            # ── Paper execution (immediate) ──────────────────────────────────
            "paper_entry_price": price,
            "paper_amount": paper_amount,
            "paper_shares": paper_amount / price,
            "paper_entry_time": datetime.now().isoformat(),
            "paper_close_price": None,
            "paper_closed_time": None,
            "paper_profit_loss": None,

            # ── Live execution (continuous GTD copy worker) ───────────────────
            "live_status": "FILLING" if live_on else ("PAPER_ONLY" if settings.PRIVATE_KEY else "NO_CREDS"),
            "live_attempts": 0,
            "live_entry_price": None,
            "live_amount": None,
            "live_shares": None,
            "live_entry_time": None,
            "live_order_id": None,
            "live_mark_price": None,
            "live_unrealized_pl": None,
            "live_close_price": None,
            "live_closed_time": None,
            "live_profit_loss": None,

            # ── Lifecycle ───────────────────────────────────────────────────
            "status": "OPEN",
            "awaiting_resolution": False,
            "strike_price": strike_open,
            "strike_source": strike_source,
            "window_start_ms": int(window_start_ms) if window_start_ms is not None else None,
            "open_reason": open_reason,
            "end_ts": end_ts,
            "mode": "hybrid",
            "kind": kind,  # PATCH: longshot — "regular" | "longshot"
        }
        if is_ls:
            trade["longshot"] = dict(decision.get("longshot") or {}, band=decision.get("band"),
                                     prob=decision.get("prob"), multiple=decision.get("multiple"),
                                     max_price=decision.get("max_price"))

        for k in slot_keys:
            _in_flight_markets.add(k)

        trade_dispatched = False
        try:
            state["paper_balance"] -= paper_amount
            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            if is_ls:
                lsd = trade["longshot"]
                msg = (f"Executed PAPER long shot ({lsd.get('band')}): {side} @ {price:.4f} for {market.get('slug')} "
                       f"(Amount: ${paper_amount:.2f}, chance {float(lsd.get('prob') or 0):.3f}, ×{float(lsd.get('multiple') or 0):.1f}, "
                       f"{float(lsd.get('secs_left') or 0):.0f}s left)")
            else:
                msg = f"Executed PAPER trade: {side} @ {price:.4f} for {market.get('slug')} (Amount: ${paper_amount:.2f})"
            order_sent = asyncio.Event()

            # Parallel dispatch: Never wait for Paper disk writes or Telegram alerts before firing Live!
            async def _bg_paper_post_actions():
                if live_on:
                    # Write the state file once the live order is out: json-dumping the state holds
                    # Python's GIL for milliseconds, which would slow the order's own Python steps.
                    try:
                        await asyncio.wait_for(order_sent.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
                await asyncio.to_thread(save_state)
                await send_telegram(f"🟢 *PAPER {'Long Shot' if is_ls else 'Trade'} Entered*\n• Side: `{side}`\n• Price: `{price:.4f}`\n• Stake: `${paper_amount:.2f}`\n• Market: `{market.get('slug')}`")

            if live_on:
                # Fire LIVE trade order and paper background tasks concurrently via asyncio.gather
                trade_dispatched = True
                # ensure_future (not create_task): gather() returns a future, and create_task()
                # raised TypeError on it after the orders had already started.
                _fut = asyncio.ensure_future(asyncio.gather(
                    _continuous_live_copy_fill(trade, market, token_ids, evaluated_price=price, ask_liq_usd=ask_liq_usd,
                                               order_sent=order_sent),
                    _bg_paper_post_actions()
                ))
                _BG_FUTURES.add(_fut)
                _fut.add_done_callback(_BG_FUTURES.discard)
                # Yield once: the live order is signed and handed to the sender right now,
                # before the bookkeeping and log lines below.
                await asyncio.sleep(0)

            _sync_active_trades_to_latest_data()
            log_message(msg)
            if not live_on:
                if settings.PRIVATE_KEY:
                    log_message("LIVE copy skipped: long shots are set to paper only")
                else:
                    log_message("LIVE copy skipped: no private key set in settings")
                asyncio.create_task(_bg_paper_post_actions())

            return "entered"
        finally:
            if not trade_dispatched:
                for k in slot_keys:
                    _in_flight_markets.discard(k)


async def maybe_flip_position(decision: Dict[str, Any], poly_snapshot: Dict[str, Any], time_left_min: Optional[float]):
    if not settings.FLIP_ENABLED:
        return
    if decision.get("action") != "ENTER" or not state["active_trades"]:
        return

    new_side = decision["side"]
    new_prob = decision.get("prob", 0) or 0
    if new_prob < settings.FLIP_MIN_CONVICTION:
        return
    if time_left_min is not None and time_left_min < settings.FLIP_MIN_MINUTES_LEFT:
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    cur_trades = [t for t in state["active_trades"] if str(t.get("market_id")) == str(market.get("id"))
                  and not _is_longshot(t)]  # PATCH: longshot — long shots are never flipped
    if not cur_trades:
        return
    trade = cur_trades[0]
    if trade["side"] == new_side:
        return

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)
    if not exit_price or exit_price <= 0:
        log_message(f"FLIP aborted: no exit price for {trade['side']}")
        return

    now_iso = datetime.now().isoformat()

    # 1. Close live trade first if filled. If sell fails, ABORT flip to avoid self-hedging!
    if trade.get("live_status") == "FILLED" and trade.get("live_shares"):
        token_id = token_ids.get(held_key)
        sell_shares = float(trade["live_shares"])
        exp = int(max(time.time() + 300, trade.get("end_ts") or 0))
        result = await asyncio.to_thread(clob_trader.place_limit_sell, token_id, sell_shares, exit_price, exp)
        if not result.get("ok"):
            log_message(f"FLIP ABORTED: Live sell of {trade['side']} failed ({result.get('error')}) — holding position to prevent self-hedging")
            return None
        live_fill_px = float(result.get("fill_price") or exit_price)
        trade["live_close_price"] = live_fill_px
        trade["live_closed_time"] = now_iso
        trade["live_exit_order_id"] = result.get("order_id")
        trade["live_profit_loss"] = (sell_shares * live_fill_px) - float(trade.get("live_amount") or 0.0)
        log_message(f"FLIP: Live {trade['side']} sold @ {live_fill_px:.4f} (Live P/L ${trade['live_profit_loss']:.2f})")
    elif trade.get("live_status") in ("FILLING", "GTD_OPEN"):
        order_id = trade.get("live_order_id")
        token_id = token_ids.get(held_key)
        if order_id:
            log_message(f"FLIP: Cancelling resting GTD order {order_id}")
            await asyncio.to_thread(clob_trader.cancel_order, order_id)
            fills = await asyncio.to_thread(clob_trader.get_order_fills, order_id, token_id) if token_id else []
            if fills:
                filled_shares = sum(float(f["size"]) for f in fills)
                if filled_shares > 0:
                    log_message(f"FLIP: Resting GTD had {filled_shares:.2f} filled shares — selling before flip")
                    sell_res = await asyncio.to_thread(clob_trader.place_limit_sell, token_id, filled_shares, exit_price)
                    if not sell_res.get("ok"):
                        log_message(f"FLIP ABORTED: Live sell of partial fills failed ({sell_res.get('error')}) — holding to prevent self-hedging")
                        return None
                    live_fill_px = float(sell_res.get("fill_price") or exit_price)
                    trade["live_shares"] = filled_shares
                    trade["live_close_price"] = live_fill_px
                    trade["live_closed_time"] = now_iso
                    trade["live_exit_order_id"] = sell_res.get("order_id")
                    trade["live_status"] = "FILLED"
                    trade["live_profit_loss"] = (filled_shares * live_fill_px) - float(trade.get("live_amount") or 0.0)
                    log_message(f"FLIP: Sold partial fills @ {live_fill_px:.4f} (Live P/L ${trade['live_profit_loss']:.2f})")
        if trade.get("live_status") != "FILLED":
            trade["live_status"] = "CANCELLED_BY_FLIP"
            trade["live_closed_time"] = now_iso
            trade["live_profit_loss"] = 0.0

    # 2. Close paper trade only after live position exit is confirmed
    state["paper_balance"] += trade["paper_shares"] * exit_price
    trade["paper_close_price"] = exit_price
    trade["paper_closed_time"] = now_iso
    trade["paper_profit_loss"] = (trade["paper_shares"] * exit_price) - trade["paper_amount"]

    trade["status"] = "CLOSED"
    trade["exit_reason"] = "flip"
    trade["resolution"] = "flip_exit"
    trade["settlement_price_at_expiry"] = exit_price
    trade["open_price"] = trade.get("strike_price")
    trade["close_price"] = state.get("last_seen_price")

    state["trade_history"].append(_archive(trade))
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    _sync_active_trades_to_latest_data()
    log_message(f"FLIP: Closed {trade['side']} @ {exit_price:.2f} (Paper P/L ${trade['paper_profit_loss']:.2f}); opening {new_side}")
    return new_side

def mark_window_open(start_ms: int, window_ms: int, current_price: Optional[float],
                      spot_price: Optional[float], price_source: Optional[str],
                      price_is_fresh: bool = True, price_ts_ms: Optional[float] = None) -> Dict[str, Any]:
    opens = state["market_opens"]
    prev_ws = state.get("last_window_start")

    # On rollover, freeze the PRIOR window's close
    if prev_ws is not None and prev_ws != start_ms and prev_ws in opens:
        if opens[prev_ws].get("close") is None and state.get("last_seen_price"):
            opens[prev_ws]["close"] = state["last_seen_price"]
    if current_price:
        state["last_seen_price"] = current_price

    observed_prev = prev_ws is not None and abs((start_ms - window_ms) - prev_ws) < 2000
    if start_ms not in opens:
        opens[start_ms] = {"chainlink": None, "binance": None,
                           "close": None, "genuine": observed_prev}
        for k in list(opens.keys()):
            if k < start_ms - 4 * window_ms:
                del opens[k]

    win = opens[start_ms]
    since_start = time.time() * 1000 - start_ms
    # The strike is the first Chainlink price of the new window. Now that the loop runs right
    # at the open, the latest price can still be the one stamped just before it: that one is
    # only used if nothing newer has arrived STRIKE_WAIT_MS after the open.
    ts_ok = price_ts_ms is None or price_ts_ms >= start_ms or since_start >= STRIKE_WAIT_MS
    if (win["chainlink"] is None and current_price and price_is_fresh and ts_ok
            and 0 <= since_start < MARK_CAPTURE_WINDOW_MS):
        win["chainlink"] = current_price
        win["binance"] = spot_price
        win["genuine"] = True
        log_message(f"Window open marked @ eventStartTime: Chainlink {current_price:.2f} "
                    f"({price_source}) / Binance {spot_price if spot_price else '-'}")
    elif win["chainlink"] is None and since_start >= MARK_CAPTURE_WINDOW_MS:
        if not win.get("skip_logged"):
            win["skip_logged"] = True
            log_message(f"Window open not captured (started late mid-window): No strike @ eventStartTime — skipping trading this window")
    state["last_window_start"] = start_ms
    return win

async def _redeem_win(trade: Dict[str, Any], market: Optional[Dict[str, Any]],
                      up_index: int, down_index: int, winning_index: int):
    condition_id = (market or {}).get("conditionId") or (market or {}).get("condition_id")
    if not condition_id:
        trade["redeem"] = {"ok": False, "error": "missing_condition_id"}
        log_message(f"REDEEM skipped for {trade['market_slug']}: no conditionId on the market")
        return

    # Defer on-chain redemption if market was settled via local strike fallback and on-chain outcome is still pending
    # (PATCH: longshot — also the 60-second-average fallback; the redeem sweep claims it once Polymarket resolves)
    if trade.get("resolution") in ("close_vs_open", "sampled_twap"):
        trade["redeem_status"] = "PENDING_ONCHAIN_RESOLUTION"
        log_message(f"REDEEM deferred for {trade['market_slug']}: Position won via local fallback; waiting for on-chain Polymarket resolution before redeeming.")
        return

    amounts = [0.0, 0.0]
    idx = up_index if winning_index == up_index else down_index
    shares = float(trade.get("live_shares") or trade.get("shares") or 0.0)
    if shares <= 0:
        return
    if 0 <= idx < len(amounts):
        amounts[idx] = shares

    neg_risk = bool((market or {}).get("negRisk") or (market or {}).get("neg_risk") or False)
    try:
        res = await asyncio.to_thread(clob_trader.redeem, condition_id, amounts, neg_risk)
    except Exception as e:
        res = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    trade["redeem"] = res
    if res.get("ok"):
        redeem_sweep.mark_attempted(condition_id)  # PATCH: longshot — the sweep won't send it again
        trade["redeem_status"] = "CONFIRMED"
        log_message(f"REDEEM ok for {trade['market_slug']}: {amounts[idx]:.2f} live shares (tx {res.get('tx')})")
    else:
        err = str(res.get("error") or "")
        if "401" in err or "Unauthorized" in err:
            trade["redeem_status"] = "PENDING_RELAYER_KEY"
            log_message(f"REDEEM PENDING for {trade['market_slug']}: 401 Unauthorized — Polymarket Relayer API Key is required for gasless redemption. Add your API key in config.json -> relayer -> api_key, or claim on polymarket.com.")
        else:
            trade["redeem_status"] = "FAILED"
            log_message(f"REDEEM FAILED for {trade['market_slug']}: {err}")

def _archive(trade: Dict[str, Any]) -> Dict[str, Any]:
    for k in ("_market", "_market_closed", "order_response"):
        trade.pop(k, None)
    return trade

async def update_trades(current_prices: Dict[str, Any]):
    # Snapshot pattern to prevent mid-pass mutations from dropping concurrent trades
    active_snapshot = list(state["active_trades"])
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    cur_price = current_prices.get("chainlink") or current_prices.get("spot")
    SETTLEMENT_GRACE_SECONDS = 300

    for trade in active_snapshot:
        if cur_price:
            trade["last_price"] = cur_price

        end_ts = trade.get("end_ts", 0)
        if not end_ts:
            try:
                end_ts = datetime.fromisoformat(trade.get("paper_entry_time") or trade.get("entry_time")).timestamp() + settings.CANDLE_WINDOW_MINUTES * 60
            except Exception:
                end_ts = now_ts
        expired = now_ts >= end_ts
        trade["awaiting_resolution"] = expired and trade.get("status") == "OPEN"

        # Hard cancel on expiry: Never let a resting order bleed into the next window!
        if expired and trade.get("live_order_id") and trade.get("live_status") in ("FILLING", "GTD_OPEN", "GTC_OPEN"):
            try:
                await asyncio.to_thread(clob_trader.cancel_order, trade["live_order_id"])
            except Exception:
                pass

        if expired and trade.get("close_price") is None:
            frozen_close = cur_price or trade.get("last_price")
            if frozen_close:
                trade["close_price"] = frozen_close
        # PATCH: longshot — the bot's own 60-second averages at the open and the close,
        # taken 3 s after the close while the recorded prices are still in memory
        if expired and now_ts >= end_ts + 3.0 and "sampled_close_avg" not in trade:
            trade["sampled_close_avg"] = longshot.sampled_twap(end_ts)
            _ws_ms = trade.get("window_start_ms")
            trade["sampled_open_avg"] = longshot.sampled_twap(_ws_ms / 1000.0) if _ws_ms else None
        # End-of-window wallet fill reconciliation:
        # Before evaluating settlement, compare wallet's actual fills with bot records.
        # This catches any ghost fills or orders that filled without local confirmation.
        # PATCH: longshot — a long shot that never sent an order (paper only, $0 stake...) has
        # nothing to reconcile; a fill-and-kill long shot can have several orders.
        _ls_no_order = _is_longshot(trade) and not (trade.get("live_order_ids") or trade.get("live_order_id"))
        if (expired and trade.get("live_status") != "FILLED" and clob_trader.is_ready() and not trade.get("_reconciled")
                and not _ls_no_order):
            trade["_reconciled"] = True
            token_id = trade.get("token_id")
            order_id = trade.get("live_order_id")
            reconciled_fills = []
            for _oid in (trade.get("live_order_ids") or ([order_id] if order_id else [])):
                reconciled_fills += await asyncio.to_thread(clob_trader.get_order_fills, _oid, token_id)
            # PATCH: longshot — the token-wide lookup below can't tell two trades on the same token
            # apart, so it is skipped when another trade in this window also ordered that token.
            shared_token = any(o is not trade and o.get("token_id") == token_id
                               and str(o.get("market_id")) == str(trade.get("market_id"))
                               and (o.get("live_order_id") or o.get("live_order_ids"))
                               for o in active_snapshot)
            if not reconciled_fills and token_id and not shared_token:
                try:
                    entry_str = trade.get("live_entry_time") or trade.get("paper_entry_time") or trade.get("entry_time")
                    start_ts = datetime.fromisoformat(entry_str).timestamp()
                except Exception:
                    start_ts = end_ts - (settings.CANDLE_WINDOW_MINUTES * 60)
                reconciled_fills = await asyncio.to_thread(clob_trader.get_token_fills_in_window, token_id, start_ts, end_ts)

            if reconciled_fills:
                total_shares = sum(float(f["size"]) for f in reconciled_fills)
                total_usd = sum(float(f["size"]) * float(f["price"]) for f in reconciled_fills)
                avg_price = total_usd / total_shares if total_shares > 0 else float(trade.get("paper_entry_price") or 0.5)

                log_message(f"WALLET RECONCILIATION: Discovered {total_shares:.2f} filled shares in wallet for {trade['market_slug']} ({trade['side']}) — reconciling trade to FILLED!")
                trade["live_status"] = "FILLED"
                trade["live_shares"] = total_shares
                trade["live_entry_price"] = avg_price
                trade["live_amount"] = total_usd
                trade["live_entry_time"] = trade.get("live_entry_time") or datetime.now().isoformat()

                # Deduct spent funds in RAM
                if total_usd > 0:
                    new_bal = max(0.0, (state.get("live_balance") or 0.0) - total_usd)
                    state["live_balance"] = new_bal
                    risk_type = (settings.RISK_TYPE or "percent").lower()
                    if risk_type == "fixed":
                        new_stake = float(settings.RISK_VALUE)
                    else:
                        new_stake = (float(settings.RISK_VALUE) / 100.0) * new_bal
                    state["precomputed_live_stake"] = round(max(1.0, min(new_stake, new_bal)) if new_bal >= 1.0 else 0.0, 2)

                save_state()
                _sync_active_trades_to_latest_data()
                await broadcast_state()
                trades_changed = True


        market = trade.get("_market")
        poll_every = 3.0 if expired else 30.0
        if expired and now_ts - end_ts > 120:
            poll_every = 10.0  # PATCH: longshot — official mode can wait minutes; poll Gamma less often
        if trade.get("last_api_check", 0) < now_ts - poll_every:
            try:
                fetched = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                fetched = None
            trade["last_api_check"] = now_ts
            if fetched is not None:
                market = fetched
                trade["_market"] = fetched
                trade["_market_closed"] = bool(fetched.get("closed"))
        market_closed = trade.get("_market_closed", False)

        if not expired and not market_closed:
            remaining_active.append(trade)
            continue

        outcomes = []
        outcome_prices = []
        if market:
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str): outcomes = json.loads(outcomes)
            outcome_prices = market.get("outcomePrices", [])
            if isinstance(outcome_prices, str): outcome_prices = json.loads(outcome_prices)
        if not outcomes:
            outcomes = [settings.POLYMARKET_UP_LABEL, settings.POLYMARKET_DOWN_LABEL]

        up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), 0)
        down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), 1)

        winning_index = -1
        resolution = None
        # PATCH: longshot — settlement.mode "official" only accepts Polymarket's resolved result
        # (price >= official_min_price, and closed if official_require_closed); "price" is the
        # original rule (any outcome price above price_threshold, default 0.9).
        official = settings.SETTLEMENT_MODE == "official"
        for i, p in enumerate(outcome_prices):
            try:
                fp = float(p)
            except Exception:
                continue
            if official:
                if fp >= settings.OFFICIAL_MIN_PRICE and (market_closed or not settings.OFFICIAL_REQUIRE_CLOSED):
                    winning_index = i
                    resolution = "polymarket_official"
                    break
            elif fp > settings.PRICE_SETTLE_THRESHOLD:
                winning_index = i
                resolution = "polymarket_settled"
                break

        strike = trade.get("strike_price")
        settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                            or trade.get("last_price") or cur_price)
        if winning_index == -1 and (expired or market_closed):
            if trade.get("expired_at") is None:
                trade["expired_at"] = now_ts
            waited = now_ts - trade["expired_at"]

            if waited < (settings.OFFICIAL_SETTLE_WAIT_S if official else settings.AUTHORITATIVE_SETTLE_WAIT_S):
                remaining_active.append(trade)
                continue

            # PATCH: longshot — fallback on the bot's own 60-second averages (closing >= opening = Up)
            if official and settings.SETTLEMENT_FALLBACK == "sampled_twap":
                close_avg = trade.get("sampled_close_avg")
                open_avg = trade.get("sampled_open_avg") or strike
                if close_avg and open_avg:
                    winning_index = up_index if close_avg >= open_avg else down_index
                    resolution = "sampled_twap"
                    trade["settlement_price_at_expiry"] = close_avg
                    trade["settle_wait_s"] = round(waited, 1)
                    log_message(f"SETTLEMENT FALLBACK for {trade['market_slug']}: no official result after {waited:.0f}s — "
                                f"resolved locally on the bot's 60-second averages ({close_avg:.2f} vs {open_avg:.2f})")

            if winning_index == -1 and strike and settlement_price:
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price > strike else down_index
                resolution = "close_vs_open"
                trade["settle_wait_s"] = round(waited, 1)
                log_message(f"SETTLEMENT FALLBACK for {trade['market_slug']}: Polymarket resolution unavailable after {waited:.0f}s — resolved locally via {resolution} ({settlement_price:.2f} vs strike {strike:.2f})")

        if winning_index == -1:
            first_seen = trade.get("unresolved_since")
            if first_seen is None:
                trade["unresolved_since"] = now_ts
                remaining_active.append(trade)
                continue
            if now_ts - first_seen < SETTLEMENT_GRACE_SECONDS:
                remaining_active.append(trade)
                continue
            trade["status"] = "VOID"
            trade["exit_reason"] = "void"
            now_iso = datetime.now().isoformat()
            trade["paper_closed_time"] = now_iso
            trade["live_closed_time"] = now_iso if trade.get("live_status") == "FILLED" else None
            trade["paper_profit_loss"] = 0.0
            trade["live_profit_loss"] = 0.0 if trade.get("live_status") == "FILLED" else None
            state["paper_balance"] += float(trade.get("paper_amount") or 0.0)
            state["trade_history"].append(_archive(trade))
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded.")
            continue

        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        open_px = strike
        close_px = trade.get("close_price") or settlement_price
        trade["open_price"] = open_px
        trade["close_price"] = close_px
        trade["paper_close_price"] = close_px
        trade["live_close_price"] = close_px if trade.get("live_status") == "FILLED" else None
        trade["resolution"] = resolution or "unknown"
        if open_px and close_px:
            move_side = "UP" if close_px > open_px else "DOWN"
            dir_txt = f"open {open_px:.2f} -> close {close_px:.2f} ({move_side} by {abs(close_px - open_px):.2f})"
        else:
            dir_txt = f"open {open_px} -> close {close_px}"

        now_iso = datetime.now().isoformat()
        trade["paper_closed_time"] = now_iso
        trade["live_closed_time"] = now_iso if trade.get("live_status") in ("FILLED", "FILLING") else None
        if trade.get("live_status") == "FILLING":
            trade["live_status"] = "TIMEOUT"

        if won:
            # Paper payout
            paper_shares = float(trade.get("paper_shares") or 0.0)
            paper_amt = float(trade.get("paper_amount") or 0.0)
            paper_payout = paper_shares * 1.0
            state["paper_balance"] += paper_payout
            trade["paper_profit_loss"] = paper_payout - paper_amt

            # Live payout
            if trade.get("live_status") == "FILLED" and trade.get("live_shares"):
                live_shares = float(trade["live_shares"])
                live_amt = float(trade.get("live_amount") or 0.0)
                live_payout = live_shares * 1.0
                trade["live_profit_loss"] = live_payout - live_amt
                log_message(f"WIN: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Live Profit: ${trade['live_profit_loss']:.2f} (Paper Profit: ${trade['paper_profit_loss']:.2f})")
                await send_telegram(f"🏆 *WIN: {trade['side']}*\n• Live Profit: `+${trade['live_profit_loss']:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")
                await _redeem_win(trade, market, up_index, down_index, winning_index)
            else:
                trade["live_profit_loss"] = None
                log_message(f"WIN (Paper): {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Paper Profit: ${trade['paper_profit_loss']:.2f}")
                await send_telegram(f"🏆 *WIN (Paper): {trade['side']}*\n• Paper Profit: `+${trade['paper_profit_loss']:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")
        else:
            paper_amt = float(trade.get("paper_amount") or 0.0)
            trade["paper_profit_loss"] = -paper_amt
            if trade.get("live_status") == "FILLED" and trade.get("live_shares"):
                live_amt = float(trade.get("live_amount") or 0.0)
                trade["live_profit_loss"] = -live_amt
                log_message(f"LOSS: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Live Loss: ${trade['live_profit_loss']:.2f} (Paper Loss: -${paper_amt:.2f})")
                await send_telegram(f"❌ *LOSS: {trade['side']}*\n• Live Loss: `-${live_amt:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")
            else:
                trade["live_profit_loss"] = None
                log_message(f"LOSS (Paper): {trade['side']} on {trade['market_slug']}: {dir_txt} "
                            f"[{trade['resolution']}]. Paper Loss: -${paper_amt:.2f}")
                await send_telegram(f"❌ *LOSS (Paper): {trade['side']}*\n• Paper Loss: `-${paper_amt:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")

        trade["status"] = "CLOSED"
        trade["exit_reason"] = trade.get("exit_reason") or "settled"
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        state["trade_history"].append(_archive(trade))
        trades_changed = True

    # PATCH: longshot — trades entered while this pass was waiting on the network (Gamma poll,
    # cancels, redeems) were not in the snapshot; keep them instead of overwriting them away.
    _snap_ids = {id(t) for t in active_snapshot}
    state["active_trades"] = remaining_active + [t for t in state["active_trades"] if id(t) not in _snap_ids]
    if trades_changed:
        save_state()
        _sync_active_trades_to_latest_data()
        # Controlled GC invocation: cleanup expired trade objects during quiet settlement period
        import gc
        gc.collect()

# ── Capital Extractor (Auto-Withdrawal State Machine) ─────────────────────────
async def maybe_auto_withdraw(equity: float, poly_snapshot: Dict[str, Any]):
    if not settings.AUTO_WITHDRAW_ENABLED:
        state["withdraw_state"] = "idle"
        return
    if not settings.PRIVATE_KEY:
        return
    raw_dest = (settings.WITHDRAW_ADDRESS or "").strip()
    if raw_dest.startswith("0x") and len(raw_dest) == 42:
        dest_address = raw_dest
    else:
        dest_address = clob_trader.get_eoa_address() if clob_trader else None
    if not dest_address or settings.WITHDRAW_AMOUNT <= 0:
        return

    now_ts = time.time()
    st = state["withdraw_state"]

    if st == "idle":
        if equity >= settings.WITHDRAW_TRIGGER_BALANCE:
            if not state["active_trades"]:
                log_message(f"CAPITAL EXTRACTOR: Trigger reached (${equity:.2f} >= ${settings.WITHDRAW_TRIGGER_BALANCE:.2f}). Initiating withdrawal of ${settings.WITHDRAW_AMOUNT:.2f}...")
                state["withdraw_state"] = "in_progress"
                state["withdraw_submitted_at"] = now_ts
                state["withdraw_locked_market"] = poly_snapshot.get("market", {}).get("id") if poly_snapshot.get("ok") else None
                
                res = await asyncio.to_thread(clob_trader.withdraw_pusd, dest_address, settings.WITHDRAW_AMOUNT)
                if res.get("ok"):
                    tx = res.get("tx")
                    state["last_withdrawal"] = {
                        "amount": settings.WITHDRAW_AMOUNT,
                        "recipient": dest_address,
                        "timestamp": datetime.now().isoformat(),
                        "tx": tx,
                        "status": "submitted"
                    }
                    save_state()
                    log_message(f"CAPITAL EXTRACTOR: Withdrawal tx submitted ({tx}). Waiting confirmation...")
                    await send_telegram(f"💸 *Capital Extractor*\nWithdrew `${settings.WITHDRAW_AMOUNT:.2f}` pUSD to `{dest_address}`\nTx: `{tx}`")
                else:
                    log_message(f"CAPITAL EXTRACTOR FAILED: {res.get('error')}")
                    state["withdraw_state"] = "cooldown"
                    state["withdraw_submitted_at"] = now_ts

    elif st == "in_progress":
        last_w = state.get("last_withdrawal")
        tx = (last_w or {}).get("tx")
        confirmed = False
        if tx:
            confirmed = await asyncio.to_thread(clob_trader.is_tx_confirmed, tx)
        if confirmed or (now_ts - (state["withdraw_submitted_at"] or now_ts) > 45):
            if last_w:
                last_w["status"] = "confirmed"
                save_state()
            log_message(f"CAPITAL EXTRACTOR: Withdrawal confirmed! Entering cooldown.")
            state["withdraw_state"] = "cooldown"
            state["withdraw_submitted_at"] = now_ts

    elif st == "cooldown":
        resume_after = (settings.WITHDRAW_RESUME_AFTER or "flat").lower()
        if not settings.WITHDRAW_AUTO_RESUME:
            return

        can_resume = False
        sub_at = state.get("withdraw_submitted_at") or now_ts
        if resume_after in ("flat", "confirmed"):
            if now_ts - sub_at > 30:
                can_resume = True
        elif resume_after == "submitted":
            can_resume = True
        elif resume_after == "next_window":
            cur_mkt_id = poly_snapshot.get("market", {}).get("id") if poly_snapshot.get("ok") else None
            if cur_mkt_id and cur_mkt_id != state.get("withdraw_locked_market"):
                can_resume = True
        else:
            if now_ts - sub_at > 30:
                can_resume = True

        if can_resume:
            log_message("CAPITAL EXTRACTOR: Resuming trading operations.")
            state["withdraw_state"] = "idle"
            state["withdraw_locked_market"] = None

# ── Event-driven early evaluation & watcher ───────────────────────────────────
def entry_block_reason(ctx: Dict[str, Any]) -> Optional[str]:
    if not state["running"]:
        return "stopped"
    if state["withdraw_state"] == "in_progress":
        return "withdraw_in_progress"
    if not ctx:
        return "no_context"
    _mkt = ctx.get("market") or {}
    _mid = str(_mkt.get("id") or "")
    _mslug = str(_mkt.get("slug") or "")
    _sms = ctx.get("start_ms")

    timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
    if _sms is not None and int(_sms) != int(timing["startMs"]):
        return "window_expired"

    # In-flight check for this market
    if (_mid and _mid in _in_flight_markets) or (_mslug and _mslug in _in_flight_markets):
        return "slot_busy"

    # Check if THIS specific window is already busy in active_trades
    for t in state["active_trades"]:
        if _is_longshot(t):  # PATCH: longshot — a long shot doesn't take the regular slot
            continue
        if _mid and str(t.get("market_id")) == _mid:
            return "slot_busy"
        if _mslug and t.get("market_slug") == _mslug:
            return "slot_busy"
        if _sms is not None and t.get("window_start_ms") == int(_sms):
            return "slot_busy"

    if not ctx.get("price_is_fresh"):
        return "stale_feed"
    if ctx.get("strike_open") is None:
        return "no_strike"
    if not ctx.get("market_ok"):
        return "market_not_ready"
    return None

async def evaluate_entry(reason: str = "event"):
    global _last_eval_ts
    if _entry_lock.locked():
        return
    async with _entry_lock:
        now = time.time()
        if now - _last_eval_ts < MIN_EVAL_INTERVAL_S:
            return
        _last_eval_ts = now

        ctx = state.get("trade_ctx")
        if not ctx:
            return

        timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
        cur_start_ms = timing["startMs"]

        # Strict window check: never evaluate trades using stale previous window context!
        if ctx.get("start_ms") != cur_start_ms:
            state["trade_ctx"] = None
            return

        # Strict market expiry check
        mkt = ctx.get("market") or {}
        end_date = mkt.get("endDate")
        if end_date:
            try:
                end_ts = datetime.fromisoformat(str(end_date).replace('Z', '+00:00')).timestamp()
                if now >= end_ts - 5.0:
                    state["trade_ctx"] = None
                    return
            except Exception:
                pass

        if not ctx.get("strike_open") or not ctx.get("target_open"):
            state["trade_ctx"] = None
            return

        if now - ctx.get("built_at", 0) > CTX_MAX_AGE_S:
            return

        block = entry_block_reason(ctx)
        if block:
            return

        spot_price = binance_stream.get_last().get("price")
        if not spot_price:
            return

        # PATCH: longshot — model.time_mode ("steps" = original whole 5-minute steps)
        mc_steps = _model_steps(max(0.0, (ctx.get("end_ts") or now) - now) / 60.0
                                if settings.MODEL_TIME_MODE == "exact" else ctx["time_left_min"])
        fair_up = indicators.fair_prob_up(
            spot_price, ctx["target_open"], mc_steps, ctx["sigma_5m"], drift_per_step=ctx["drift_5m"]
        )

        market_up = ctx["prices"]["up"]
        market_down = ctx["prices"]["down"]

        up_ws_sum = polymarket_clob_ws.get_summary(ctx["token_ids"].get("up"), max_age_s=settings.MAX_BOOK_AGE_S)
        down_ws_sum = polymarket_clob_ws.get_summary(ctx["token_ids"].get("down"), max_age_s=settings.MAX_BOOK_AGE_S)
        if up_ws_sum and up_ws_sum.get("bestAsk"):
            market_up = up_ws_sum["bestAsk"]
        if down_ws_sum and down_ws_sum.get("bestAsk"):
            market_down = down_ws_sum["bestAsk"]

        decision = engines.decide_ev({
            "mcProbUp": fair_up,
            "priceUp": market_up,
            "priceDown": market_down,
            "minProb": settings.MIN_PROB_EV,
            "evThreshold": settings.EV_THRESHOLD,
            "rsi": ctx["rsi"],
            "haExhaustedGreen": ctx["ha_exhausted_green"],
            "haExhaustedRed": ctx["ha_exhausted_red"],
        })

        if decision.get("action") == "ENTER":
            market_prices = {"up": market_up, "down": market_down}
            exec_res = await execute_trade(
                decision, market_prices, ctx["market"], ctx["strike_open"],
                ctx["token_ids"], ctx.get("orderbook", {}),
                strike_source=ctx["strike_source"], window_start_ms=ctx["start_ms"],
                open_reason=f"event_entry_{reason}"
            )
            state["event_exec"] = {
                "ts": datetime.now().isoformat(),
                "side": decision["side"],
                "reason": reason,
                "result": exec_res
            }
            if exec_res == "entered":
                await broadcast_state()

# ── Long shots (PATCH: longshot) ──────────────────────────────────────────────
def _settle_price_now():
    """(price, age in seconds, source) of the latest settlement-feed price."""
    pw = polymarket_ws_stream.get_last()
    if pw.get("price") and pw.get("updatedAt"):
        return float(pw["price"]), max(0.0, time.time() - float(pw["updatedAt"]) / 1000.0), "polymarket_ws"
    cl = chainlink_ws_stream.get_last()
    if cl.get("price") and cl.get("updatedAt"):
        return float(cl["price"]), max(0.0, time.time() - float(cl["updatedAt"]) / 1000.0), "chainlink_rpc"
    return None, None, None


def _ls_price_to_beat(ctx: Dict[str, Any], now: float):
    """(value, source): the sampled 60-second average at the window start, or the bot's own strike."""
    if settings.LONGSHOT.get("price_to_beat") == "sampled_twap":
        v = longshot.price_to_beat(int(ctx["start_ms"]) / 1000.0, now)
        if v is not None:
            return v, "sampled_twap"
    so = ctx.get("strike_open")
    return (float(so), "bot_strike") if so else (None, None)


def _ls_books(ctx: Dict[str, Any]):
    """Best ask + dollars in the top 5 ask levels per side (WebSocket book, else the loop's snapshot)."""
    books, raw = {}, {}
    for side, key in (("UP", "up"), ("DOWN", "down")):
        tok = (ctx.get("token_ids") or {}).get(key)
        summ = polymarket_clob_ws.get_summary(tok, max_age_s=settings.MAX_BOOK_AGE_S) if tok else None
        if not summ or not summ.get("bestAsk"):
            summ = (ctx.get("orderbook") or {}).get(key) or {}
        try:
            ask = float(summ["bestAsk"]) if summ.get("bestAsk") is not None else None
        except (TypeError, ValueError):
            ask = None
        liq_sh = summ.get("askLiquidity")
        try:
            liq_usd = float(liq_sh) * ask if (liq_sh is not None and ask) else None
        except (TypeError, ValueError):
            liq_usd = None
        books[side] = {"ask": ask, "liq_usd": liq_usd}
        raw[key] = {"bestAsk": ask, "askLiquidity": liq_sh}
    return books, raw


_ls_last_error_ts = 0.0


async def evaluate_longshot(reason: str = "event"):
    """Long-shot check: runs on the same events as evaluate_entry (Binance trades, the Chainlink
    feed, and order-book updates in the long-shot look window) and once per update_loop pass."""
    global _ls_last_error_ts
    cfg = settings.LONGSHOT
    if not cfg.get("enabled") or _ls_lock.locked():
        return
    async with _ls_lock:
        now = time.time()
        try:
            ctx = state.get("ls_ctx")
            if not ctx or not state["running"] or state["withdraw_state"] == "in_progress":
                return
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
            if int(ctx.get("start_ms") or 0) != int(timing["startMs"]):
                state["ls_ctx"] = None
                return
            if now - ctx.get("built_at", 0) > CTX_MAX_AGE_S:
                return
            end_ts = float(ctx["end_ts"])
            secs_left = end_ts - now
            # PATCH: longshot maker — the check also runs while only the maker bids look
            if secs_left < longshot.eval_cutoff_s(cfg) or secs_left > longshot.eval_look_s(cfg):
                return

            model = cfg.get("model", "twap")
            info: Dict[str, Any] = {"model": model, "secs_left": round(secs_left, 2)}
            status = {"ts": now, "secs_left": round(secs_left, 2), "model": model}
            if model == "twap":
                price_now, age, src = _settle_price_now()
                if price_now is None or age is None or age > float(cfg.get("max_price_age_s", 3.0)):
                    state["ls_last"] = {**status, "result": f"stale_price ({'none' if age is None else f'{age:.1f}s'})"}
                    return
                ptb, ptb_src = _ls_price_to_beat(ctx, now)
                if ptb is None:
                    state["ls_last"] = {**status, "result": "no_price_to_beat"}
                    return
                vol = longshot.vol_usd_per_sqrt_s(cfg, price_now, ctx.get("sigma_5m"))
                r = longshot.prob_up_twap(now, end_ts, ptb, price_now, vol, float(cfg.get("sd_floor_usd", 3.0)))
                p_up = r["p_up"]
                info.update(price_to_beat=round(ptb, 2), ptb_source=ptb_src, price_now=price_now,
                            price_source=src, expected_avg=round(r["expected_avg"], 2), sd=round(r["sd"], 3),
                            vol_usd_per_sqrt_s=round(vol, 3))
                trade_strike, strike_src = ptb, ptb_src
            else:
                spot = binance_stream.get_last().get("price")
                tgt = ctx.get("target_open")
                if not spot or not tgt:
                    state["ls_last"] = {**status, "result": "no_spot_or_open"}
                    return
                if model == "exact":
                    p_up = longshot.prob_up_exact(spot, tgt, secs_left, ctx.get("sigma_5m"), ctx.get("drift_5m") or 0.0)
                else:
                    p_up = indicators.fair_prob_up(spot, tgt, _model_steps(secs_left / 60.0), ctx.get("sigma_5m"),
                                                   drift_per_step=ctx.get("drift_5m") or 0.0)
                info.update(spot=spot, model_open=tgt)
                trade_strike, strike_src = ctx.get("strike_open"), ctx.get("strike_source") or "chainlink_ws"

            books, raw_ob = _ls_books(ctx)
            if longshot.maker_on(cfg):   # PATCH: longshot maker — the resting bids follow every check
                _toks = ctx.get("token_ids") or {}
                await ls_maker_mgr.on_view({
                    "ts": now, "start_ms": int(ctx["start_ms"]), "end_ts": end_ts, "p_up": p_up,
                    "token_ids": {"UP": _toks.get("up"), "DOWN": _toks.get("down")},
                    "meta": {"market": ctx["market"], "strike": trade_strike, "strike_src": strike_src, "info": dict(info)}})
            ceiling = longshot.max_band_ask(cfg)
            if secs_left <= longshot.max_look_s(cfg):
                for side in ("UP", "DOWN"):
                    longshot.note_candidate(int(ctx["start_ms"]), side, p_up if side == "UP" else 1.0 - p_up,
                                            books[side]["ask"], secs_left, ceiling,
                                            float(cfg.get("taker_fee_rate", 0.0) or 0.0))
            decision = longshot.decide(p_up, books, secs_left, cfg)
            state["ls_last"] = {**status, "p_up": round(p_up, 4), "up_ask": books["UP"]["ask"],
                                "down_ask": books["DOWN"]["ask"], "result": decision.get("reason"), **info}
            if longshot.maker_on(cfg):
                state["ls_last"]["maker"] = ls_maker_mgr.active()
            if decision.get("action") != "ENTER":
                return

            decision["longshot"] = info
            res = await execute_trade(
                decision, {"up": books["UP"]["ask"], "down": books["DOWN"]["ask"]}, ctx["market"], trade_strike,
                ctx.get("token_ids") or {}, raw_ob, strike_source=strike_src, window_start_ms=ctx["start_ms"],
                open_reason=f"longshot_{decision.get('band')}", kind="longshot")
            state["ls_last"]["exec"] = res
            if res == "entered":
                _ls_entered_windows.add(int(ctx["start_ms"]))
                await broadcast_state()
        except Exception as e:
            if now - _ls_last_error_ts > 60:
                _ls_last_error_ts = now
                log_message(f"LONGSHOT check error: {type(e).__name__}: {e}")


def _ls_window_summary(start_ms: Optional[int]):
    """One log line per finished window when long shots are on and none was entered."""
    if start_ms is None:
        return
    best = longshot.pop_window_best(int(start_ms))
    entered = int(start_ms) in _ls_entered_windows
    _ls_entered_windows.discard(int(start_ms))
    if not settings.LONGSHOT.get("enabled") or entered:
        return
    label = datetime.fromtimestamp(int(start_ms) / 1000).strftime('%H:%M')
    if best:
        log_message(f"LONGSHOT {label} window: no entry — closest {best['side']} ask {best['ask'] * 100:.1f}¢, "
                    f"chance {best['prob']:.3f} (×{best['multiple']:.1f}) with {best['secs_left']:.0f}s left")
    else:
        log_message(f"LONGSHOT {label} window: no entry — no ask inside the bands during the look window")


def _on_book_update():
    """PATCH: longshot — an order-book change wakes the checks, but only in the long-shot look
    window (the cheap side's ask is what a long shot waits for); elsewhere nothing changes."""
    cfg = settings.LONGSHOT
    if not cfg.get("enabled"):
        return
    ctx = state.get("ls_ctx")
    if ctx and 0.0 <= float(ctx.get("end_ts") or 0.0) - time.time() <= longshot.eval_look_s(cfg):  # PATCH: longshot maker
        _market_event.set()


def _sweep_block_reason() -> Optional[str]:
    if not settings.PRIVATE_KEY:
        return "no private key"
    if not settings.RELAYER_API_KEY:
        return "no relayer API key; gasless redeem needs one"
    return None


async def entry_watcher():
    while True:
        try:
            await _market_event.wait()
            _market_event.clear()
            await evaluate_entry(reason="trade_tick")
            await evaluate_longshot(reason="trade_tick")  # PATCH: longshot
        except Exception:
            await asyncio.sleep(0.1)


async def live_balance_updater():
    """Background task to continuously poll the CLOB USDC balance (every 2.5s)
    so the bot always has the live balance in RAM, and precomputes the next
    live trade stake with 0ms latency at trade time."""
    while True:
        try:
            if settings.PRIVATE_KEY:
                bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                if bal is not None and bal >= 0:
                    state["live_balance"] = bal
                    state["last_balance_refresh"] = time.time()

                    # Precompute the exact trade amount for the next live trade
                    risk_type = (settings.RISK_TYPE or "percent").lower()
                    if risk_type == "fixed":
                        stake = float(settings.RISK_VALUE)
                    else:
                        stake = (float(settings.RISK_VALUE) / 100.0) * bal

                    if bal >= 1.0:
                        stake = max(1.0, min(stake, bal))
                    else:
                        stake = 0.0

                    state["precomputed_live_stake"] = round(stake, 2)
        except Exception:
            pass
        await asyncio.sleep(2.5)

_seeding_klines = False
async def seed_kline_buffers():
    global _seeding_klines
    if globals().get("_seeding_klines"):
        return
    _seeding_klines = True
    try:
        k1m, k5m = await asyncio.gather(
            data.fetch_klines(settings.SYMBOL, "1m", 240),
            data.fetch_klines(settings.SYMBOL, "5m", 200)
        )
        if k1m:
            binance_kline_1m.set_candles(k1m)
            if not state.get("last_spot_price"):
                last_c = k1m[-1]["close"]
                state["last_spot_price"] = last_c
                prices = state.setdefault("latest_data", {}).setdefault("prices", {})
                prices["spot"] = last_c
            if len(k1m) >= 15:
                closes = [c["close"] for c in k1m if c.get("close")]
                state["last_rsi"] = indicators.compute_rsi(closes, settings.RSI_PERIOD)
                state.setdefault("latest_data", {}).setdefault("indicators", {})["rsi"] = state["last_rsi"]
            if len(k1m) >= 2:
                state["last_ha_1m"] = indicators.count_consecutive(indicators.compute_heiken_ashi(k1m))
                state.setdefault("latest_data", {}).setdefault("indicators", {})["heiken"] = state["last_ha_1m"]
        if k5m:
            binance_kline_5m.set_candles(k5m)
            if len(k5m) >= 2:
                state["last_ha_5m"] = indicators.count_consecutive(indicators.compute_heiken_ashi(k5m))
                state.setdefault("latest_data", {}).setdefault("indicators", {})["heiken_5m"] = state["last_ha_5m"]
        log_message(f"Seeded Binance kline buffers (1m: {len(k1m) if k1m else 0}, 5m: {len(k5m) if k5m else 0}) for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")
    finally:
        _seeding_klines = False

# ── Main update loop ──────────────────────────────────────────────────────────
async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        "recommendation", "reason", "exec_result"
    ]

    while True:
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)
            cur_window_start_ms = int(timing["startMs"])
            prev_window_start_ms = state.get("last_window_start")

            # In the last minute of a window, look up the next window's market (once; again
            # after 10 s if it wasn't listed yet) so nothing has to be fetched when it opens.
            _next_start = int(timing["endMs"])
            if (timing["remainingMs"] < NEXT_MARKET_PREFETCH_MS and not settings.POLYMARKET_SLUG
                    and _next_market.get("start_ms") != _next_start and not _next_market_busy
                    and time.time() - _prefetch_tries.get(_next_start, 0.0) > 10.0):
                _prefetch_tries.clear()
                _prefetch_tries[_next_start] = time.time()
                asyncio.create_task(_prefetch_next_market(_next_start))

            # ── WINDOW ROLLOVER DETECTED ──
            # A new 15-minute window has begun (:00, :15, :30, :45).
            # Purge all stale context, cached market, and previous window odds immediately
            # so the bot NEVER uses old window data to calculate signals or enter trades!
            if prev_window_start_ms is not None and cur_window_start_ms != prev_window_start_ms:
                state["trade_ctx"] = None
                _cached_market = None
                _cached_market_ts = 0.0
                state["last_poly_up"] = None
                state["last_poly_down"] = None
                # (books are no longer cleared here: the new window's were subscribed before it
                # opened, and books are looked up by token, so the old ones can't be mixed in)
                log_message(f"Window rollover detected: new window started at {datetime.fromtimestamp(cur_window_start_ms / 1000).strftime('%H:%M:%S')}. Cleared old window context.")
                state["ls_ctx"] = None              # PATCH: longshot
                _ls_window_summary(prev_window_start_ms)

            binance_ws = binance_stream.get_last()
            if not binance_ws.get("price"):
                poly_ws_last = polymarket_ws_stream.get_last()
                cl_ws_last = chainlink_ws_stream.get_last()
                binance_ws["price"] = poly_ws_last.get("price") or cl_ws_last.get("price")
            poly_ws = polymarket_ws_stream.get_last()
            cl_ws = chainlink_ws_stream.get_last()

            spot_cand = binance_ws.get("price") if binance_ws and binance_ws.get("price") else state.get("last_spot_price")
            need_rest_spot = spot_cand is None
            # The on-chain Chainlink RPC price is only used when neither websocket nor an earlier
            # price is there (see "Settle feed" below); skip the RPC call otherwise: this gather
            # waits for its slowest call.
            need_rest_chainlink = not (poly_ws.get("price") or cl_ws.get("price") or state.get("last_chainlink_price"))
            results = await asyncio.gather(
                data.fetch_last_price(settings.SYMBOL) if need_rest_spot else asyncio.sleep(0, result=None),
                chainlink.chainlink_fetcher.fetch_chainlink_btc_usd() if need_rest_chainlink else asyncio.sleep(0, result={}),
                fetch_polymarket_snapshot(),
                return_exceptions=True
            )

            last_price = results[0] if not isinstance(results[0], Exception) else None
            chainlink_data = results[1] if not isinstance(results[1], Exception) else {}
            poly_snapshot = results[2] if not isinstance(results[2], Exception) else {"ok": False}

            klines_1m = binance_kline_1m.get_candles()
            klines_5m = binance_kline_5m.get_candles()

            spot_price = binance_ws.get("price") if binance_ws and binance_ws.get("price") else last_price
            if spot_price:
                state["last_spot_price"] = spot_price
            else:
                spot_price = state.get("last_spot_price")

            # Auto-heal kline buffers if empty or low
            if len(klines_1m) < 15 or len(klines_5m) < 15:
                now_t = time.time()
                if now_t - state.get("_last_seed_kline_ts", 0) > 10.0:
                    state["_last_seed_kline_ts"] = now_t
                    asyncio.create_task(seed_kline_buffers())

            mc_steps = _model_steps(timing["remainingMinutes"])  # PATCH: longshot — model.time_mode

            # Settle feed & freshness
            current_price = None
            price_source = None
            price_is_fresh = True
            poly_updated = poly_ws.get("updatedAt") or 0
            if poly_ws.get("price"):
                current_price = poly_ws["price"]
                price_source = "Polymarket WS"
                if poly_updated > 0:
                    age_ms = (time.time() * 1000) - poly_updated
                    if age_ms > POLY_WS_MAX_AGE_MS:
                        price_is_fresh = False
            elif cl_ws.get("price"):
                current_price = cl_ws["price"]
                price_source = "Chainlink RPC WS"
            elif state.get("last_chainlink_price"):
                current_price = state["last_chainlink_price"]
                price_source = "Chainlink RPC WS"
            elif chainlink_data.get("price"):
                current_price = chainlink_data["price"]
                price_source = "Chainlink RPC REST"

            # PATCH: longshot — when the Polymarket WS isn't the source, record the fallback price too
            if current_price and price_source != "Polymarket WS":
                longshot.record_price(time.time(), current_price)

            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
            event_start_ms = None
            if poly_snapshot.get("ok"):
                _mkt = poly_snapshot["market"]
                _esr = _mkt.get("eventStartTime") or _mkt.get("gameStartTime")
                if _esr:
                    try:
                        event_start_ms = int(datetime.fromisoformat(str(_esr).replace('Z', '+00:00')).timestamp() * 1000)
                    except Exception:
                        event_start_ms = None
                if event_start_ms is None and _mkt.get("endDate"):
                    try:
                        event_start_ms = int(datetime.fromisoformat(_mkt["endDate"].replace('Z', '+00:00')).timestamp() * 1000) - window_ms
                    except Exception:
                        event_start_ms = None
            if event_start_ms is None:
                event_start_ms = int(timing["startMs"])

            start_ms = event_start_ms
            win = mark_window_open(start_ms, window_ms, current_price, spot_price, price_source, price_is_fresh=price_is_fresh,
                                   price_ts_ms=(poly_updated or None) if price_source == "Polymarket WS" else None)

            strike_open = win["chainlink"]
            strike_source = "chainlink_ws"

            model_open = None
            for c in reversed(klines_5m):
                if c["openTime"] == start_ms or (c["openTime"] <= start_ms < c["openTime"] + 300_000):
                    model_open = c["open"]
                    break
            model_open = model_open or win.get("binance") or spot_price
            target_open = model_open if strike_open is not None else None

            settlement_ms = None
            if poly_snapshot.get("ok") and poly_snapshot.get("market") and poly_snapshot["market"].get("endDate"):
                try:
                    settlement_ms = datetime.fromisoformat(poly_snapshot["market"]["endDate"].replace('Z', '+00:00')).timestamp() * 1000
                    if settlement_ms <= time.time() * 1000:
                        poly_snapshot["ok"] = False
                except Exception:
                    settlement_ms = None

            now_ms = time.time() * 1000
            if settlement_ms is not None:
                time_left_min = max(0.0, (settlement_ms - now_ms) / 60_000)
            else:
                time_left_min = max(0.0, timing["remainingMinutes"])

            closes = [c["close"] for c in klines_1m if c.get("close")]
            rsi_now = indicators.compute_rsi(closes, settings.RSI_PERIOD) if len(closes) >= 15 else state.get("last_rsi")
            if rsi_now is not None:
                state["last_rsi"] = rsi_now

            consec = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_1m)) if len(klines_1m) >= 2 else (state.get("last_ha_1m") or {"color": None, "count": 0})
            if consec.get("color"):
                state["last_ha_1m"] = consec

            consec_5m = {"color": None, "count": 0}
            if len(klines_5m) >= 2:
                consec_5m = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_5m))
                if consec_5m.get("color"):
                    state["last_ha_5m"] = consec_5m
            elif state.get("last_ha_5m"):
                consec_5m = state["last_ha_5m"]

            market_up = poly_snapshot.get("prices", {}).get("up") if poly_snapshot.get("ok") else None
            market_down = poly_snapshot.get("prices", {}).get("down") if poly_snapshot.get("ok") else None
            if market_up is not None:
                state["last_poly_up"] = market_up
            else:
                market_up = state.get("last_poly_up")

            if market_down is not None:
                state["last_poly_down"] = market_down
            else:
                market_down = state.get("last_poly_down")

            market_implied_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)

            EB = engines.EXHAUSTION_BARS
            def _is(color, count, want):
                return color == want and (count or 0) >= EB

            ha_exhausted_green = _is(consec["color"], consec["count"], "green") or _is(consec_5m["color"], consec_5m["count"], "green")
            ha_exhausted_red = _is(consec["color"], consec["count"], "red") or _is(consec_5m["color"], consec_5m["count"], "red")

            since_start = (time.time() * 1000) - start_ms
            if strike_open is None or target_open is None:
                fair_up = 0.50
                drift_5m, sigma_5m = (0.0, None)
                if since_start < MARK_CAPTURE_WINDOW_MS:
                    decision = {"action": "NO_TRADE", "side": None, "phase": "EV", "strength": "EV", "reason": "waiting_new_window_strike"}
                else:
                    decision = {"action": "NO_TRADE", "side": None, "phase": "EV", "strength": "EV", "reason": "no_strike_window_skipped"}
            else:
                drift_5m, sigma_5m = indicators.realized_drift_vol(klines_5m, lookback=300)
                fair_up = indicators.fair_prob_up(spot_price or 0, target_open, mc_steps, sigma_5m, drift_per_step=drift_5m or 0.0)
                decision = engines.decide_ev({
                    "mcProbUp": fair_up,
                    "priceUp": market_up,
                    "priceDown": market_down,
                    "minProb": settings.MIN_PROB_EV,
                    "evThreshold": settings.EV_THRESHOLD,
                    "rsi": rsi_now,
                    "haExhaustedGreen": ha_exhausted_green,
                    "haExhaustedRed": ha_exhausted_red,
                })

            fair_data = {
                "prob_up": fair_up,
                "prob_down": 1.0 - fair_up,
                "bias": "BULLISH" if fair_up > 0.6 else "BEARISH" if fair_up < 0.4 else "NEUTRAL",
                "steps": mc_steps,
                "sigma_5m": sigma_5m,
            }

            edge = {
                "marketUp": market_implied_up,
                "marketDown": (1 - market_implied_up) if market_implied_up is not None else None,
                "edgeUp": (fair_up - market_implied_up) if market_implied_up is not None else None,
                "edgeDown": ((1 - fair_up) - (1 - market_implied_up)) if market_implied_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_up, "adjustedDown": 1 - fair_up}

            # Publish trade_ctx ONLY IF new window strike is genuinely captured and market is valid!
            if (poly_snapshot.get("ok") and strike_open is not None and target_open is not None
                    and poly_snapshot.get("market") and time_left_min > 0.1):
                tok_up = poly_snapshot.get("token_ids", {}).get("up")
                tok_down = poly_snapshot.get("token_ids", {}).get("down")
                if tok_up:
                    clob_trader.warm_token(tok_up)
                if tok_down:
                    clob_trader.warm_token(tok_down)

                mkt_end_date = poly_snapshot["market"].get("endDate")
                mkt_end_ts = None
                if mkt_end_date:
                    try:
                        mkt_end_ts = datetime.fromisoformat(str(mkt_end_date).replace('Z', '+00:00')).timestamp()
                    except Exception:
                        mkt_end_ts = None
                if not mkt_end_ts:
                    mkt_end_ts = time.time() + (time_left_min * 60.0)

                state["trade_ctx"] = {
                    "built_at": time.time(),
                    "end_ts": mkt_end_ts,
                    "start_ms": start_ms,
                    "target_open": target_open,
                    "strike_open": strike_open,
                    "strike_source": strike_source,
                    "time_left_min": time_left_min,
                    "sigma_5m": sigma_5m,
                    "drift_5m": drift_5m or 0.0,
                    "rsi": rsi_now,
                    "ha_exhausted_green": ha_exhausted_green,
                    "ha_exhausted_red": ha_exhausted_red,
                    "prices": poly_snapshot["prices"],
                    "token_ids": poly_snapshot["token_ids"],
                    "orderbook": poly_snapshot.get("orderbook", {}),
                    "market": poly_snapshot["market"],
                    "market_ok": True,
                    "price_is_fresh": price_is_fresh,
                    "slot_busy": any(str(t.get("market_id")) == str(poly_snapshot["market"].get("id")) and not _is_longshot(t)
                                     for t in state["active_trades"]),
                }
            else:
                state["trade_ctx"] = None

            # PATCH: longshot — context for evaluate_longshot (valid until end - cutoff_s)
            ls_ctx = None
            if settings.LONGSHOT.get("enabled") and poly_snapshot.get("ok") and poly_snapshot.get("market"):
                ls_end = None
                try:
                    ls_end = datetime.fromisoformat(str(poly_snapshot["market"].get("endDate")).replace('Z', '+00:00')).timestamp()
                except Exception:
                    ls_end = None
                if not ls_end:
                    ls_end = time.time() + time_left_min * 60.0
                if ls_end - time.time() > longshot.eval_cutoff_s(settings.LONGSHOT):  # PATCH: longshot maker
                    ls_ctx = {
                        "built_at": time.time(), "end_ts": ls_end, "start_ms": start_ms,
                        "strike_open": strike_open, "strike_source": strike_source, "target_open": target_open,
                        "sigma_5m": sigma_5m, "drift_5m": drift_5m or 0.0,
                        "prices": poly_snapshot.get("prices", {}), "token_ids": poly_snapshot.get("token_ids", {}),
                        "orderbook": poly_snapshot.get("orderbook", {}), "market": poly_snapshot["market"],
                    }
            state["ls_ctx"] = ls_ctx
            if ls_ctx:
                await evaluate_longshot(reason="loop")

            current_prices_dict = {"spot": spot_price, "chainlink": current_price}

            exec_result = None
            if poly_snapshot.get("ok") and state["running"] and state["withdraw_state"] != "in_progress":
                flipped = await maybe_flip_position(decision, poly_snapshot, time_left_min)
                exec_result = await execute_trade(
                    decision, poly_snapshot["prices"], poly_snapshot["market"], strike_open,
                    poly_snapshot.get("token_ids", {}), poly_snapshot.get("orderbook", {}),
                    strike_source=strike_source, window_start_ms=start_ms,
                    open_reason="flip_entry" if flipped else "ev_entry")
            elif not state["running"]:
                exec_result = "stopped"
            elif state["withdraw_state"] == "in_progress":
                exec_result = "withdraw_in_progress"

            # Check if event executor fired
            if state.get("event_exec"):
                ee = state.pop("event_exec")
                if not exec_result or exec_result in ("no_trade", "stopped"):
                    exec_result = f"event({ee.get('reason')}:{ee.get('result')})"

            await update_trades(current_prices_dict)

            # Mark open positions (live only, when filled)
            live_open_value = 0.0
            for t in state["active_trades"]:
                mark = None
                if poly_snapshot.get("ok") and poly_snapshot.get("market") and str(t.get("market_id")) == str(poly_snapshot["market"].get("id")):
                    ob = (poly_snapshot.get("orderbook") or {}).get("up" if t.get("side") == "UP" else "down") or {}
                    raw_mark = ob.get("bestBid") or (market_up if t.get("side") == "UP" else market_down)
                    try:
                        mark = float(raw_mark) if raw_mark is not None else None
                    except (ValueError, TypeError):
                        mark = None

                if t.get("live_status") == "FILLED" and t.get("live_shares"):
                    try:
                        shares = float(t.get("live_shares") or 0.0)
                        entry_px = float(t.get("live_entry_price") or 0.0)
                        amt = float(t.get("live_amount") if t.get("live_amount") is not None else (shares * entry_px))
                    except (ValueError, TypeError):
                        shares = 0.0
                        amt = 0.0

                    if mark is not None and shares > 0:
                        t["live_mark_price"] = mark
                        t["live_unrealized_pl"] = (shares * mark) - amt
                        live_open_value += shares * mark
                    else:
                        t["live_mark_price"] = mark
                        t["live_unrealized_pl"] = None
                        live_open_value += amt
                else:
                    t["live_mark_price"] = None
                    t["live_unrealized_pl"] = None

            # Live balance refresh: every 30 seconds directly in update_loop (safe, debounced, never hangs)
            if settings.PRIVATE_KEY:
                now_ts = time.time()
                if now_ts - state.get("last_balance_refresh", 0) > 30.0:
                    state["last_balance_refresh"] = now_ts
                    real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                    if real_bal is not None:
                        state["live_balance"] = real_bal
                        risk_type = (settings.RISK_TYPE or "percent").lower()
                        if risk_type == "fixed":
                            stake = float(settings.RISK_VALUE)
                        else:
                            stake = (float(settings.RISK_VALUE) / 100.0) * real_bal
                        state["precomputed_live_stake"] = round(max(1.0, min(stake, real_bal)) if real_bal >= 1.0 else 0.0, 2)

            live_balance = float(state.get("live_balance") or 0.0)
            total_equity = live_balance + live_open_value
            await maybe_auto_withdraw(total_equity, poly_snapshot)

            signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            try:
                utils.append_csv_row(SIGNALS_PATH, csv_header, [
                    datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                    signal_label, fair_up, 1 - fair_up, market_up, market_down,
                    edge["edgeUp"], edge["edgeDown"], f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                    decision.get("reason", ""), exec_result or ""
                ])
            except Exception:
                pass

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": "hybrid",
                    "running": state["running"],
                    "balance": live_balance if settings.PRIVATE_KEY else None,
                    "equity": total_equity if settings.PRIVATE_KEY else None,
                    "open_value": live_open_value if settings.PRIVATE_KEY else None,
                    "has_live_creds": bool(settings.PRIVATE_KEY),
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL,
                    "withdraw_state": state["withdraw_state"],
                    "last_withdrawal": state.get("last_withdrawal")
                },
                "prices": {
                    "spot": spot_price,
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "poly_up": market_up,
                    "poly_down": market_down,
                    "window_open": strike_open,
                    "window_open_source": strike_source,
                    "model_open": model_open,
                    "window_start_ms": start_ms,
                    "price_is_fresh": price_is_fresh,
                    "book_source": poly_snapshot.get("book_source") if poly_snapshot.get("ok") else None
                },
                "indicators": {
                    "rsi": rsi_now,
                    "heiken": consec,
                    "heiken_5m": consec_5m,
                    "fair": fair_data
                },
                "analysis": {
                    "probability": prob_view, "edge": edge, "decision": decision,
                    "longshot": state.get("ls_last") if settings.LONGSHOT.get("enabled") else None,  # PATCH: longshot
                }
            }
            state["last_update_ts"] = time.time()

            await broadcast_state()

        except Exception as e:
            print(f"Error in update loop: {e}")

        await _wait_next_pass()


async def _wait_next_pass():
    """Sleep until update_loop's next pass: POLL_INTERVAL_MS as before, but never past the
    next window opening (the pass at the open used to come up to POLL_INTERVAL_MS late),
    and right away when _loop_wake is set (the new window's first Chainlink price)."""
    poll_s = settings.POLL_INTERVAL_MS / 1000
    w_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
    to_open_s = (w_ms - (time.time() * 1000) % w_ms) / 1000 + 0.002
    if not _loop_wake.is_set():
        try:
            await asyncio.wait_for(_loop_wake.wait(), timeout=max(0.0, min(poll_s, to_open_s)))
        except asyncio.TimeoutError:
            pass
    _loop_wake.clear()


async def order_conn_keeper():
    """Keeps the Rust order sender's HTTPS connection to Polymarket open: a tiny request
    every ORDER_CONN_PING_S, and one 1.5 s before each window opens. Orders go out about
    once a window, so the connection used to be closed by then and every order first waited
    for a new one (DNS lookup, TCP and TLS handshakes: several round trips)."""
    last = 0.0
    pre_open_done = None
    while True:
        try:
            now = time.time()
            w = settings.CANDLE_WINDOW_MINUTES * 60.0
            next_open = (now // w + 1) * w
            pre_open_due = now >= next_open - 1.5 and pre_open_done != next_open
            if pre_open_due or now - last >= ORDER_CONN_PING_S:
                if settings.PRIVATE_KEY and getattr(clob_trader, "ready", False):
                    await asyncio.to_thread(clob_trader.keep_warm)
                last = time.time()
                if pre_open_due:
                    pre_open_done = next_open
                continue
            next_pre_open = next_open - 1.5 if pre_open_done != next_open else next_open + w - 1.5
            # sleep at most 1 s at a time, so a clock change can't make it miss an opening
            await asyncio.sleep(min(max(min(last + ORDER_CONN_PING_S, next_pre_open) - now, 0.01), 1.0))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(5.0)

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()

    # Pre-seed cached spot price from last run if available
    if not state.get("last_spot_price"):
        cached_p = state.get("latest_data", {}).get("prices", {}).get("spot")
        if cached_p:
            state["last_spot_price"] = cached_p

    # Pre-seed full historical kline buffers so indicators are ready on very first render
    await seed_kline_buffers()

    # Start all background tasks
    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_1m.start()),
        asyncio.create_task(binance_kline_5m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(polymarket_clob_ws.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(update_loop()),
        asyncio.create_task(telegram_poller()),
        asyncio.create_task(entry_watcher()),
        asyncio.create_task(order_conn_keeper())
    ]


    # PATCH: longshot — redeems anything the Data API lists as redeemable for the trading wallet
    tasks.append(asyncio.create_task(redeem_sweep.run_forever(
        get_cfg=lambda: settings.REDEEM_SWEEP, log=log_message,
        get_wallet=clob_trader.get_funder, redeem=clob_trader.redeem,
        can_run=_sweep_block_reason)))
    tasks.append(asyncio.create_task(ls_maker_mgr.run_forever()))  # PATCH: longshot maker
    log_message("LONGSHOT: " + (("on — " + longshot.describe(settings.LONGSHOT)) if settings.LONGSHOT.get("enabled") else "off")
                + f" | settlement: {settings.SETTLEMENT_MODE} | model clock: {settings.MODEL_TIME_MODE}"
                + f" | redeem sweep: {'on' if settings.REDEEM_SWEEP.get('enabled') else 'off'}")


    # Pre-warm CLOB trader client & on-chain verification in background thread if private key set
    if settings.PRIVATE_KEY:
        async def _warm_live():
            try:
                await asyncio.to_thread(clob_trader.ensure_ready)
                if settings.RELAYER_API_KEY:
                    try:
                        setup_res = await asyncio.to_thread(clob_trader.ensure_setup)
                        log_message(f"Startup on-chain wallet verification: {setup_res}")
                    except Exception as e:
                        log_message(f"Startup on-chain wallet verification note: {e}")
            except Exception as e:
                log_message(f"Live trader pre-warm note: {e}")
        asyncio.create_task(_warm_live())

    import gc
    gc.collect()

    yield

    # PATCH: longshot maker — no maker bid stays on the book after the bot stops
    try:
        await asyncio.wait_for(ls_maker_mgr.cancel_all("bot shutting down"), timeout=10.0)
    except Exception:
        pass

    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    polymarket_ws_stream.close()
    polymarket_clob_ws.close()
    chainlink_ws_stream.close()

app = FastAPI(title="Polymarket BTC 15m Assistant [GTD]", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        init_msg = json.dumps({
            "type": "state_update",
            "data": state["latest_data"],
            "logs": state["logs"][-30:],
            "log_seq": state["log_seq"],
        })
        await websocket.send_text(init_msg)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _ws_clients.discard(websocket)

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return templates.TemplateResponse(request=request, name="settings.html")

@app.get("/api/latest")
async def get_latest():
    return state["latest_data"]

@app.get("/api/logs")
async def get_logs():
    if os.path.exists(LOG_FILE_PATH):
        try:
            with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="ignore") as f:
                lines = [line.rstrip("\r\n") for line in f if line.strip()]
                if lines:
                    return lines
        except Exception:
            pass
    return state["logs"]

DOWNLOADABLE = {
    "signals": (SIGNALS_PATH, "text/csv"),
    "trades": (STATE_PATH, "application/json"),
    "logs": (LOG_FILE_PATH, "text/plain"),
}

@app.get("/api/files")
async def list_files():
    out = []
    for key, (path, _) in DOWNLOADABLE.items():
        exists = os.path.exists(path)
        out.append({
            "key": key,
            "name": os.path.basename(path),
            "exists": exists,
            "size": os.path.getsize(path) if exists else 0,
            "rows": (max(0, sum(1 for _ in open(path, encoding="utf-8", errors="ignore")) - 1)
                     if exists and path.endswith(".csv") else None),
            "modified": (datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                         if exists else None),
        })
    return out

@app.get("/api/download/{key}")
async def download_file(key: str):
    entry = DOWNLOADABLE.get(key)
    if not entry:
        return JSONResponse({"error": "unknown_file"}, status_code=404)
    path, media = entry
    if not os.path.exists(path):
        return JSONResponse({"error": "not_generated_yet", "path": path}, status_code=404)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base, ext = os.path.splitext(os.path.basename(path))
    return FileResponse(path, media_type=media, filename=f"15m-{base}-{stamp}{ext}")

def _reflect_running_now():
    ts = state["latest_data"].get("trading_state")
    if isinstance(ts, dict):
        ts["running"] = state["running"]

@app.post("/api/start")
async def start_trading():
    state["running"] = True
    _reflect_running_now()
    log_message("Trading STARTED by user")
    await send_telegram("🟢 *Trading STARTED by user*")
    await broadcast_state()
    return {"ok": True, "running": True}

@app.post("/api/stop")
async def stop_trading():
    state["running"] = False
    _reflect_running_now()
    for t in state.get("active_trades", []):
        if t.get("live_status") == "GTD_OPEN" and t.get("live_order_id"):
            log_message(f"Cancelling resting GTD order {t['live_order_id']} on trading stop")
            asyncio.create_task(asyncio.to_thread(clob_trader.cancel_order, t["live_order_id"]))
            t["live_status"] = "CANCELLED"
    asyncio.create_task(ls_maker_mgr.cancel_all("trading stopped"))  # PATCH: longshot maker
    log_message("Trading STOPPED by user")
    await send_telegram("🔴 *Trading STOPPED by user*")
    await broadcast_state()
    return {"ok": True, "running": False}

@app.get("/api/available-series")
async def get_available_series():
    try:
        return await data.fetch_available_15m_series()
    except Exception as e:
        print(f"Error fetching available series: {e}")
        return [
            {"series_id": "10192", "title": "Bitcoin Up or Down 15m"},
            {"series_id": "10212", "title": "Ethereum Up or Down 15m"}
        ]


@app.get("/api/telegram-subscribers")
async def get_telegram_subscribers():
    subs = state.get("telegram_subscribers", [])
    normalized = []
    for s in subs:
        norm = _normalize_subscriber(s)
        if norm:
            normalized.append(norm)
    return {"ok": True, "subscribers": normalized}

@app.post("/api/telegram-unsubscribe")
async def post_telegram_unsubscribe(req: Dict[str, Any]):
    chat_id = req.get("chat_id")
    if chat_id is not None:
        try:
            remove_telegram_subscriber(int(chat_id))
        except (ValueError, TypeError):
            pass
    return {"ok": True}

@app.post("/api/test-telegram")
async def post_test_telegram(req: Optional[Dict[str, Any]] = None):
    req = req or {}
    token = (req.get("bot_token") or "").strip()
    if not token or "..." in token or token.lower() == "set":
        token = settings.TELEGRAM_BOT_TOKEN
    
    if not token:
        return {"ok": False, "error": "No bot token provided. Enter your Telegram Bot Token from @BotFather."}
    
    url = f"https://api.telegram.org/bot{token}/getMe"
    proxy = ws_data.get_proxy_url_for(url)
    try:
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=8.0) as client:
            resp = await client.get(url)
            if resp.status_code == 401:
                return {
                    "ok": False,
                    "error": "Invalid Bot Token (401 Unauthorized from Telegram). Please verify the token from @BotFather."
                }
            if resp.status_code != 200:
                return {
                    "ok": False,
                    "error": f"Telegram API error (HTTP {resp.status_code}): {resp.text}"
                }
            bot_info = resp.json().get("result", {})
            username = bot_info.get("username", "bot")
            bot_name = bot_info.get("first_name", "Bot")
    except Exception as e:
        return {"ok": False, "error": f"Failed to connect to Telegram API: {e}"}

    subs = state.get("telegram_subscribers", [])
    if not subs:
        return {
            "ok": True,
            "count": 0,
            "bot_username": username,
            "message": f"Connected as @{username} ({bot_name})! No subscribers yet — open https://t.me/{username} and send /start to subscribe."
        }

    sent_count = 0
    errors = []
    send_url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=8.0) as client:
        for s in subs:
            cid = s.get("chat_id") if isinstance(s, dict) else s
            if not cid:
                continue
            try:
                s_resp = await client.post(send_url, json={
                    "chat_id": cid,
                    "text": f"🔔 *Test alert from Polymarket Assistant!*\nBot @{username} is connected and operational.",
                    "parse_mode": "Markdown"
                })
                if s_resp.status_code == 200:
                    sent_count += 1
                else:
                    errors.append(f"Chat {cid}: HTTP {s_resp.status_code}")
            except Exception as e:
                errors.append(f"Chat {cid}: {e}")

    if sent_count > 0:
        return {
            "ok": True,
            "count": sent_count,
            "bot_username": username,
            "message": f"Connected as @{username}! Sent test alert to {sent_count} subscriber(s)."
        }
    else:
        err_msg = ", ".join(errors) if errors else "failed to send message"
        return {
            "ok": False,
            "bot_username": username,
            "error": f"Connected as @{username}, but failed to deliver to subscribers: {err_msg}"
        }

@app.get("/api/settings")
async def get_settings():
    def mask(v: str) -> str:
        return v[:6] + "..." + v[-4:] if v and len(v) > 10 else v

    masked_pk = mask(settings.PRIVATE_KEY)

    return {
        "has_live_creds": bool(settings.PRIVATE_KEY),
        "private_key": masked_pk,
        "live": {
            "relayer_api_key": mask(settings.RELAYER_API_KEY),
            "alchemy_api_key": mask(settings.ALCHEMY_API_KEY),
            "funder_address": settings.FUNDER_ADDRESS,
            "order_signer": settings.ORDER_SIGNER,
            "max_slippage": settings.CLOB_MAX_SLIPPAGE,
            "exit_max_retries": settings.EXIT_MAX_RETRIES,
            "allow_partial_fill": settings.CLOB_ALLOW_PARTIAL_FILL
        },
        "polymarket": {
            "series_id": settings.POLYMARKET_SERIES_ID,
            "gamma_base_url": settings.GAMMA_BASE_URL,
            "clob_base_url": settings.CLOB_BASE_URL,
            "live_ws_url": settings.POLYMARKET_LIVE_DATA_WS_URL,
            "up_label": settings.POLYMARKET_UP_LABEL,
            "down_label": settings.POLYMARKET_DOWN_LABEL
        },
        "trading": {
            "symbol": settings.SYMBOL,
            "risk_type": settings.RISK_TYPE,
            "risk_value": settings.RISK_VALUE
        },
        "ev": {
            "ev_threshold": settings.EV_THRESHOLD,
            "min_prob": settings.MIN_PROB_EV,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD,
            "max_book_age_s": settings.MAX_BOOK_AGE_S
        },
        "flip": {
            "enabled": settings.FLIP_ENABLED,
            "min_conviction": settings.FLIP_MIN_CONVICTION,
            "min_minutes_left": settings.FLIP_MIN_MINUTES_LEFT
        },
        "capital_extractor": {
            "enabled": settings.AUTO_WITHDRAW_ENABLED,
            "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
            "withdraw_amount": settings.WITHDRAW_AMOUNT,
            "recipient_address": settings.WITHDRAW_ADDRESS,
            "withdraw_address": settings.WITHDRAW_ADDRESS,
            "auto_resume": settings.WITHDRAW_AUTO_RESUME,
            "auto_resume_after_withdrawal": settings.WITHDRAW_AUTO_RESUME,
            "resume_after": settings.WITHDRAW_RESUME_AFTER,
            "default_destination": (clob_trader.get_eoa_address() if clob_trader else "") or ""
        },
        "telegram": {
            "enabled": settings.TELEGRAM_ENABLED,
            "bot_token": mask(settings.TELEGRAM_BOT_TOKEN)
        },
        # PATCH: longshot
        "longshot": settings.LONGSHOT,
        "settlement": {
            "mode": settings.SETTLEMENT_MODE,
            "official_wait_s": settings.OFFICIAL_SETTLE_WAIT_S,
            "official_min_price": settings.OFFICIAL_MIN_PRICE,
            "official_require_closed": settings.OFFICIAL_REQUIRE_CLOSED,
            "fallback": settings.SETTLEMENT_FALLBACK,
            "price_threshold": settings.PRICE_SETTLE_THRESHOLD,
            "authoritative_settle_wait_s": settings.AUTHORITATIVE_SETTLE_WAIT_S,
        },
        "model": {"time_mode": settings.MODEL_TIME_MODE},
        "redeem_sweep": settings.REDEEM_SWEEP,
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m
    old_symbol = settings.SYMBOL

    lv_in = new_settings.get("live") if isinstance(new_settings.get("live"), dict) else None
    _new_signer = None
    if lv_in is not None and "order_signer" in lv_in:
        from bot.config import normalize_order_signer
        v = normalize_order_signer(lv_in.get("order_signer"))
        if not v:
            return {"status": "error", "error": "live.order_signer must be auto, wallet or eoa"}
        lv_in["order_signer"] = v
        _new_signer = v

    # PATCH: longshot — check the long-shot / settlement / model / sweep sections before
    # anything is written; a bad value returns an error and changes nothing.
    from bot.config import normalize_settlement, apply_settlement

    def _merge_into(base, override):
        out = json.loads(json.dumps(base))
        for k, v in (override or {}).items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = _merge_into(out[k], v)
            else:
                out[k] = v
        return out

    new_ls = new_st = new_rs = new_tm = None
    try:
        for _sec in ("longshot", "settlement", "model", "redeem_sweep"):
            if _sec in new_settings and not isinstance(new_settings[_sec], dict):
                raise ValueError(f"{_sec} must be an object")
        if isinstance(new_settings.get("longshot"), dict):
            new_ls = longshot.normalize(_merge_into(settings.LONGSHOT, new_settings["longshot"]))
            new_settings["longshot"] = new_ls
        if isinstance(new_settings.get("settlement"), dict):
            new_st = normalize_settlement(new_settings["settlement"])
            new_settings["settlement"] = {**new_settings["settlement"], **new_st}
        if isinstance(new_settings.get("model"), dict) and "time_mode" in new_settings["model"]:
            new_tm = new_settings["model"]["time_mode"]
            if new_tm not in ("steps", "exact"):
                raise ValueError("model.time_mode must be steps or exact")
        if isinstance(new_settings.get("redeem_sweep"), dict):
            new_rs = redeem_sweep.normalize({**settings.REDEEM_SWEEP, **new_settings["redeem_sweep"]})
            new_settings["redeem_sweep"] = new_rs
    except ValueError as e:
        return {"status": "error", "error": f"invalid_settings: {e}"}

    # (PATCH: longshot — the checks above run before anything is applied, so a rejected save
    # changes nothing: not the private key, not the order signer)
    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk:
        from bot.config import normalize_private_key
        try:
            _pk = normalize_private_key(new_pk)
        except Exception as e:
            return {"status": "error", "error": f"invalid_private_key: {e}"}
        settings.PRIVATE_KEY = _pk
        new_settings["private_key"] = settings.PRIVATE_KEY
    if _new_signer:
        settings.ORDER_SIGNER = _new_signer


    cfg_file = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
    existing_cfg = {}
    if os.path.exists(cfg_file):
        try:
            with open(cfg_file, "r") as f:
                existing_cfg = json.load(f)
        except Exception:
            existing_cfg = {}

    def deep_merge(base, override):
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    # Pre-sync live relayer & alchemy keys into config dictionaries before dumping to disk
    if "live" in new_settings:
        lv = new_settings["live"]
        rk = lv.get("relayer_api_key")
        if rk and "..." not in rk:
            settings.RELAYER_API_KEY = rk
            new_settings.setdefault("relayer", {})["api_key"] = rk
            existing_cfg.setdefault("relayer", {})["api_key"] = rk
        elif rk:
            lv["relayer_api_key"] = settings.RELAYER_API_KEY
        fa = lv.get("funder_address") if "funder_address" in lv else new_settings.get("funder_address")
        if fa is not None:
            from bot.config import normalize_funder_address
            settings.FUNDER_ADDRESS = normalize_funder_address(fa)
            lv["funder_address"] = settings.FUNDER_ADDRESS
            existing_cfg.setdefault("live", {})["funder_address"] = settings.FUNDER_ADDRESS
            existing_cfg["funder_address"] = settings.FUNDER_ADDRESS
        ak = lv.get("alchemy_api_key")
        if ak and "..." not in ak:
            settings.ALCHEMY_API_KEY = ak
            new_settings.setdefault("chainlink", {})["alchemy_api_key"] = ak
            existing_cfg.setdefault("chainlink", {})["alchemy_api_key"] = ak
        elif ak:
            lv["alchemy_api_key"] = settings.ALCHEMY_API_KEY

    merged_cfg = deep_merge(existing_cfg, new_settings)
    if settings.RELAYER_API_KEY:
        merged_cfg.setdefault("relayer", {})["api_key"] = settings.RELAYER_API_KEY
    if settings.ALCHEMY_API_KEY:
        merged_cfg.setdefault("chainlink", {})["alchemy_api_key"] = settings.ALCHEMY_API_KEY
    if settings.FUNDER_ADDRESS:
        merged_cfg.setdefault("live", {})["funder_address"] = settings.FUNDER_ADDRESS
        merged_cfg["funder_address"] = settings.FUNDER_ADDRESS

    # Remove obsolete keys if present in file
    merged_cfg.pop("mode", None)
    merged_cfg.pop("paper_balance_usd", None)

    with open(CONFIG_PATH, "w") as f:
        json.dump(merged_cfg, f, indent=2)

    # PATCH: longshot — apply the checked sections first: they can't fail, so the
    # running bot matches config.json even if an original section below raises
    if new_ls is not None:
        was_on = bool(settings.LONGSHOT.get("enabled"))
        settings.LONGSHOT = new_ls
        if new_ls.get("enabled"):
            log_message("LONGSHOT: on — " + longshot.describe(new_ls))
        elif was_on:
            log_message("LONGSHOT: off")
    if new_st is not None:
        apply_settlement(settings, new_st)
    if new_tm is not None:
        settings.MODEL_TIME_MODE = new_tm
    if new_rs is not None:
        settings.REDEEM_SWEEP = new_rs

    ct = merged_cfg.get("copy_trader", {})
    if ct:
        if "retry_interval_ms" in ct: settings.COPY_RETRY_INTERVAL_MS = int(ct["retry_interval_ms"])
        if "max_retries" in ct: settings.COPY_MAX_RETRIES = int(ct["max_retries"])

    if "trading" in new_settings:
        t = new_settings["trading"]
        settings.SYMBOL = t.get("symbol", settings.SYMBOL)
        settings.RISK_TYPE = t.get("risk_type", settings.RISK_TYPE)
        settings.RISK_VALUE = float(t.get("risk_value", settings.RISK_VALUE))

    if "ev" in new_settings:
        e = new_settings["ev"]
        settings.EV_THRESHOLD = float(e.get("ev_threshold", settings.EV_THRESHOLD))
        settings.MIN_PROB_EV = float(e.get("min_prob", settings.MIN_PROB_EV))
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))
        if "max_book_age_s" in e:
            settings.MAX_BOOK_AGE_S = float(e["max_book_age_s"])

    if "flip" in new_settings:
        f = new_settings["flip"]
        if "enabled" in f:
            settings.FLIP_ENABLED = bool(f["enabled"])
        settings.FLIP_MIN_CONVICTION = float(f.get("min_conviction", settings.FLIP_MIN_CONVICTION))
        settings.FLIP_MIN_MINUTES_LEFT = float(f.get("min_minutes_left", settings.FLIP_MIN_MINUTES_LEFT))

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "live" in new_settings:
        lv = new_settings["live"]
        if "max_slippage" in lv:
            settings.CLOB_MAX_SLIPPAGE = float(lv["max_slippage"])
        if "exit_max_retries" in lv:
            settings.EXIT_MAX_RETRIES = int(lv["exit_max_retries"])
        if "allow_partial_fill" in lv:
            settings.CLOB_ALLOW_PARTIAL_FILL = bool(lv["allow_partial_fill"])

        rk = lv.get("relayer_api_key")
        if rk and "..." not in rk:
            settings.RELAYER_API_KEY = rk
            new_settings.setdefault("relayer", {})["api_key"] = rk
        elif rk:
            lv["relayer_api_key"] = settings.RELAYER_API_KEY
        ak = lv.get("alchemy_api_key")
        if ak and "..." not in ak:
            settings.ALCHEMY_API_KEY = ak
            new_settings.setdefault("chainlink", {})["alchemy_api_key"] = ak
        elif ak:
            lv["alchemy_api_key"] = settings.ALCHEMY_API_KEY

    if "capital_extractor" in new_settings:
        ce = new_settings["capital_extractor"]
        if "enabled" in ce: settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
        if "trigger_balance" in ce: settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
        if "withdraw_amount" in ce: settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
        recip = ce.get("recipient_address") if "recipient_address" in ce else ce.get("withdraw_address")
        if recip is not None:
            recip_str = str(recip).strip()
            # If browser autofill sent "admin" or invalid string, sanitize to blank
            if recip_str and (not recip_str.startswith("0x") or len(recip_str) != 42):
                recip_str = ""
            settings.WITHDRAW_ADDRESS = recip_str
            ce["recipient_address"] = recip_str
            ce["withdraw_address"] = recip_str
        auto_res = ce.get("auto_resume") if "auto_resume" in ce else ce.get("auto_resume_after_withdrawal")
        if auto_res is not None:
            settings.WITHDRAW_AUTO_RESUME = bool(auto_res)
            ce["auto_resume"] = settings.WITHDRAW_AUTO_RESUME
        if "resume_after" in ce: settings.WITHDRAW_RESUME_AFTER = str(ce["resume_after"]).strip()

    if "telegram" in new_settings:
        tg = new_settings["telegram"]
        if "enabled" in tg: settings.TELEGRAM_ENABLED = bool(tg["enabled"])
        tok = tg.get("bot_token")
        if tok and "..." not in tok and tok.lower() != "set":
            settings.TELEGRAM_BOT_TOKEN = str(tok).strip()

    clob_trader.reset()

    if settings.SYMBOL != old_symbol:
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
        asyncio.create_task(binance_stream.start())

        binance_kline_1m.close()
        binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
        asyncio.create_task(binance_kline_1m.start())

        binance_kline_5m.close()
        binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)
        asyncio.create_task(binance_kline_5m.start())

        await seed_kline_buffers()

        polymarket_ws_stream.close()
        polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
            ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
            symbol_includes=get_ws_symbol_filter(settings.SYMBOL),
            on_update=_on_poly_chainlink_tick
        )
        longshot.clear_samples()  # PATCH: longshot — old symbol's prices are useless
        asyncio.create_task(polymarket_ws_stream.start())

        chainlink_ws_stream.close()
        chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL), on_update=_on_chainlink_tick)
        asyncio.create_task(chainlink_ws_stream.start())

    await broadcast_state()
    return {"status": "ok"}

@app.post("/api/setup-wallet")
async def setup_wallet(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from bot.config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    rk = body.get("relayer_api_key")
    if rk and "..." not in rk:
        settings.RELAYER_API_KEY = rk
    ak = body.get("alchemy_api_key")
    if ak and "..." not in ak:
        settings.ALCHEMY_API_KEY = ak
    fa = body.get("funder_address")
    if fa is not None and "..." not in str(fa):
        from bot.config import normalize_funder_address
        settings.FUNDER_ADDRESS = normalize_funder_address(fa)
    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.ensure_setup)
        if result.get("ok"):
            if result.get("skipped"):
                log_message("Wallet setup: already done this session")
            else:
                log_message(f"Wallet setup complete ({result.get('approvals', 0)} approvals)")
        else:
            log_message(f"Wallet setup failed: {result.get('error')}")
    except Exception as e:
        log_message(f"Wallet setup error: {e}")
        result = {"ok": False, "error": str(e)}

    eoa_addr = clob_trader.get_eoa_address() if clob_trader else None
    if not eoa_addr and settings.PRIVATE_KEY:
        try:
            from eth_account import Account
            eoa_addr = Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            pass
    result["eoa"] = eoa_addr
    result["funder"] = (clob_trader.get_funder_address() if clob_trader else None) or eoa_addr
    return result

@app.post("/api/test-connection")
async def test_connection(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from bot.config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    rk = body.get("relayer_api_key")
    if rk and "..." not in rk:
        settings.RELAYER_API_KEY = rk
    ak = body.get("alchemy_api_key")
    if ak and "..." not in ak:
        settings.ALCHEMY_API_KEY = ak
    fa = body.get("funder_address")
    if fa is not None and "..." not in str(fa):
        from bot.config import normalize_funder_address
        settings.FUNDER_ADDRESS = normalize_funder_address(fa)

    # Persist tested credentials to config.json so they survive server restarts
    cfg_file = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
    try:
        cfg = {}
        if os.path.exists(cfg_file):
            with open(cfg_file, "r") as f:
                cfg = json.load(f)
        changed = False
        if pk and "..." not in str(pk):
            cfg["private_key"] = settings.PRIVATE_KEY
            changed = True
        if rk and "..." not in str(rk):
            cfg.setdefault("relayer", {})["api_key"] = settings.RELAYER_API_KEY
            changed = True
        if ak and "..." not in str(ak):
            cfg.setdefault("chainlink", {})["alchemy_api_key"] = settings.ALCHEMY_API_KEY
            changed = True
        if fa is not None and "..." not in str(fa):
            cfg.setdefault("live", {})["funder_address"] = settings.FUNDER_ADDRESS
            cfg["funder_address"] = settings.FUNDER_ADDRESS
            changed = True
        if changed:
            with open(cfg_file, "w") as f:
                json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to persist keys in test_connection: {e}")

    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.test_connection)
        if result.get("ok"):
            log_message(f"Connection OK — EOA {result.get('eoa')}, trading from "
                        f"{result.get('funder')} (sig type {result.get('chosen_signature_type')}, "
                        f"{result.get('wallet_source') or 'auto'})")
            if result.get("wallet_warning"):
                log_message(result["wallet_warning"])
        else:
            log_message(f"Connection test: {result.get('error')}")
    except Exception as e:
        result = {"ok": False, "error": str(e)}

    # Always guarantee EOA address resolution if key is present
    eoa_addr = result.get("eoa") or (clob_trader.get_eoa_address() if clob_trader else None)
    if not eoa_addr and settings.PRIVATE_KEY:
        try:
            from eth_account import Account
            eoa_addr = Account.from_key(settings.PRIVATE_KEY).address
            result["eoa"] = eoa_addr
        except Exception:
            pass

    # Resolve withdrawal destination details
    withdraw_addr = (body.get("withdraw_address") or settings.WITHDRAW_ADDRESS or "").strip()
    is_valid_eth = bool(withdraw_addr.startswith("0x") and len(withdraw_addr) == 42)
    is_blank = not bool(withdraw_addr) or not is_valid_eth
    dest = eoa_addr if is_blank else withdraw_addr

    result["withdraw_address"] = withdraw_addr
    result["withdraw_destination"] = dest
    result["withdraw_is_eoa"] = is_blank
    result["withdraw_enabled"] = bool(body.get("withdraw_enabled", settings.AUTO_WITHDRAW_ENABLED))
    result["withdraw_amount"] = float(body.get("withdraw_amount", settings.WITHDRAW_AMOUNT))
    result["withdraw_trigger"] = float(body.get("withdraw_trigger", settings.WITHDRAW_TRIGGER_BALANCE))

    return result

@app.post("/api/enable-auto-redeem")
async def enable_auto_redeem(body: Optional[Dict[str, Any]] = None):
    body = body or {}
    pk = body.get("private_key")
    if pk and "..." not in pk:
        from bot.config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(pk)
        except Exception:
            pass
    clob_trader.reset()
    try:
        result = await asyncio.to_thread(clob_trader.enable_auto_redeem)
        log_message("Auto-redeem enabled" if result.get("ok")
                    else f"Auto-redeem failed: {result.get('error')}")
    except Exception as e:
        result = {"ok": False, "error": str(e)}

    eoa_addr = clob_trader.get_eoa_address() if clob_trader else None
    if not eoa_addr and settings.PRIVATE_KEY:
        try:
            from eth_account import Account
            eoa_addr = Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            pass
    result["eoa"] = eoa_addr
    result["funder"] = (clob_trader.get_funder_address() if clob_trader else None) or eoa_addr
    return result

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"],
            "running": state["running"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8050))
    uvicorn.run(app, host="0.0.0.0", port=port)
