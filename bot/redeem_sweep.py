"""
Redeem sweep (PATCH: longshot).

The bot redeems a position right after it labels the trade a WIN. Anything it labels
wrongly, or that settles while the bot is down, is never claimed. The sweep asks
Polymarket's Data API which of the wallet's positions are redeemable and redeems them,
every `interval_s` seconds.

Settings live in config.json under "redeem_sweep" (see DEFAULTS).
"""

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional

import httpx

DATA_API = "https://data-api.polymarket.com"

DEFAULTS: Dict[str, Any] = {
    "enabled": True,
    "interval_s": 300,      # seconds between sweeps
    "retry_after_s": 900,   # don't touch the same market again for this long (the API lags)
    "max_per_pass": 10,     # redeem at most this many markets per sweep
    "min_shares": 0.01,     # ignore dust
}


def merged(user_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(DEFAULTS)
    out.update({k: v for k, v in (user_cfg or {}).items() if k in DEFAULTS})
    return out


def normalize(user_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cfg = merged(user_cfg)
    errs = []
    try:
        from bot.longshot import as_bool
        cfg["enabled"] = as_bool(cfg["enabled"], "redeem_sweep.enabled")
    except ValueError as e:
        errs.append(str(e))
    for key, lo, hi, as_int in (("interval_s", 30, 86400, True), ("retry_after_s", 0, 86400 * 7, True),
                                ("max_per_pass", 1, 500, True), ("min_shares", 0, 1e9, False)):
        try:
            v = int(cfg[key]) if as_int else float(cfg[key])
            if v < lo or v > hi:
                raise ValueError
            cfg[key] = v
        except (TypeError, ValueError):
            errs.append(f"redeem_sweep.{key} must be between {lo} and {hi}")
    if errs:
        raise ValueError("; ".join(errs))
    return cfg


def _pick(row: Dict[str, Any], *names, default=None):
    for n in names:
        if n in row and row[n] not in (None, ""):
            return row[n]
    return default


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


async def fetch_redeemable(wallet: str, client: Optional[httpx.AsyncClient] = None,
                           base: str = DATA_API, limit: int = 100) -> List[Dict[str, Any]]:
    """Every REDEEMABLE position of `wallet` from /v2/positions (follows pagination.next_cursor)."""
    own = client is None
    client = client or httpx.AsyncClient(timeout=15.0, headers={"Accept": "application/json"})
    rows: List[Dict[str, Any]] = []
    try:
        cursor, seen = None, set()
        while True:
            params = {"user": wallet, "status": "REDEEMABLE", "limit": limit,
                      "filter_type": "TOKENS", "filter_amount": 0, "include_archived": "true"}
            if cursor:
                params["cursor"] = cursor
            r = await client.get(f"{base}/v2/positions", params=params)
            r.raise_for_status()
            doc = r.json()
            if isinstance(doc, list):
                rows.extend(doc)
                break
            page = doc.get("data") or []
            rows.extend(page if isinstance(page, list) else [page])
            pag = doc.get("pagination") or {}
            cursor = pag.get("next_cursor")
            if not cursor or pag.get("has_more") is False or not page or cursor in seen:
                break
            seen.add(cursor)
    finally:
        if own:
            await client.aclose()
    return rows


def group_by_condition(rows: List[Dict[str, Any]], min_shares: float) -> Dict[str, Dict[str, Any]]:
    """{condition_id: {"amounts": [outcome0, outcome1], "neg_risk": bool, "slug": str, "value": float}}"""
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        cond = str(_pick(row, "condition_id", "conditionId", default="") or "")
        if not cond:
            continue
        size = _num(_pick(row, "current_size", "size", default=0))
        if size < min_shares:
            continue
        value = _pick(row, "current_value", "currentValue")
        if value is not None and _num(value) <= 0:
            continue                      # a losing leg: nothing to claim
        idx = int(_num(_pick(row, "outcome_index", "outcomeIndex", default=0)))
        g = out.setdefault(cond, {"amounts": [0.0, 0.0], "neg_risk": False, "slug": "", "value": 0.0})
        while len(g["amounts"]) <= idx:
            g["amounts"].append(0.0)
        g["amounts"][idx] += size
        g["neg_risk"] = g["neg_risk"] or bool(_pick(row, "negative_risk", "negativeRisk", "neg_risk", default=False))
        g["slug"] = g["slug"] or str(_pick(row, "slug", "event_slug", "title", default="") or "")
        g["value"] += _num(value) if value is not None else size
    return out


_attempted: Dict[str, float] = {}   # condition_id -> unix time of the last attempt


def mark_attempted(condition_id: Optional[str]) -> None:
    """Called by the per-trade redeem so the sweep doesn't send the same redeem again."""
    if condition_id:
        _attempted[str(condition_id)] = time.time()


async def sweep_once(cfg: Dict[str, Any], log: Callable[[str], None],
                     get_wallet: Callable[[], Optional[str]],
                     redeem: Callable[[str, List[float], bool], Dict[str, Any]],
                     fetch=fetch_redeemable) -> Dict[str, Any]:
    """One pass. Returns {"wallet", "found", "redeemed", "failed", "skipped"}."""
    wallet = await asyncio.to_thread(get_wallet)
    if not wallet:
        return {"wallet": None, "found": 0, "redeemed": 0, "failed": 0, "skipped": 0}
    rows = await fetch(wallet)
    groups = group_by_condition(rows, float(cfg.get("min_shares", 0.01)))
    now = time.time()
    res = {"wallet": wallet, "found": len(groups), "redeemed": 0, "failed": 0, "skipped": 0}
    done = 0
    for cond, g in groups.items():
        if now - _attempted.get(cond, 0.0) < float(cfg.get("retry_after_s", 900)):
            res["skipped"] += 1
            continue
        if done >= int(cfg.get("max_per_pass", 10)):
            break
        done += 1
        _attempted[cond] = now
        try:
            out = await asyncio.to_thread(redeem, cond, g["amounts"], g["neg_risk"])
        except Exception as e:
            out = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        label = g["slug"] or cond[:12]
        if out.get("ok"):
            res["redeemed"] += 1
            log(f"REDEEM SWEEP: claimed {label} ({sum(g['amounts']):.2f} shares, ~${g['value']:.2f}) tx {out.get('tx')}")
        else:
            res["failed"] += 1
            log(f"REDEEM SWEEP: {label} not redeemed: {out.get('error')} (next try in {int(cfg.get('retry_after_s', 900))}s)")
    for k in [k for k, t in _attempted.items() if now - t > 86400 * 3]:
        del _attempted[k]
    return res


async def run_forever(get_cfg: Callable[[], Dict[str, Any]], log: Callable[[str], None],
                      get_wallet: Callable[[], Optional[str]],
                      redeem: Callable[[str, List[float], bool], Dict[str, Any]],
                      can_run: Callable[[], Optional[str]]) -> None:
    """Background task. can_run() returns None when a sweep may run, else the reason it can't."""
    await asyncio.sleep(20)
    last_block = None
    while True:
        cfg = get_cfg()
        try:
            block = None if cfg.get("enabled") else "off"
            block = block or can_run()
            if block:
                if block != last_block and block != "off":
                    log(f"REDEEM SWEEP: waiting ({block})")
            else:
                await sweep_once(cfg, log, get_wallet, redeem)
            last_block = block
        except Exception as e:
            log(f"REDEEM SWEEP error: {type(e).__name__}: {e}")
        await asyncio.sleep(max(30, int(cfg.get("interval_s", 300))))
