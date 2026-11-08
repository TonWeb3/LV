"""
Long-shot mode (PATCH: longshot).

A second entry path, separate from the regular EV strategy. It only looks at the end of
each window and buys a cheap side when the model's chance for that side is high enough
against its ask. It has its own slot (it can trade in a window where the regular strategy
already holds a position), its own stake, its own price cap and its own model.

Everything in this file is plain functions plus a small price recorder, so it runs and
tests without the network. main.py wires it in (evaluate_longshot, execute_trade,
_continuous_live_copy_fill).

Settings live in config.json under "longshot". Missing keys fall back to DEFAULTS;
normalize() checks types and ranges before anything is saved.

PATCH: longshot maker — the bands take the ask, so they pay Polymarket's taker fee:
their chance ÷ price and chance − price use the price plus that fee (taker_fee_rate).
The "maker" block is a third way in: a resting post-only bid (no fee) that bot/ls_maker.py
keeps on the book while the model gives that side at least maker.min_prob.
"""

import copy
import math
import threading
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

# ── Settings ──────────────────────────────────────────────────────────────────
DEFAULTS: Dict[str, Any] = {
    "enabled": False,          # master switch for the whole mode
    "live": True,              # False = long shots are paper-only (no live orders)
    "model": "twap",           # "twap" | "exact" | "regular" (see probability() below)
    "price_to_beat": "sampled_twap",  # "sampled_twap" | "bot_strike"
    "pick": "multiple",        # when several bands fire: "multiple" (chance / ask) or "ev" (chance - ask)
    "stake_type": "",          # "" = regular bot stake (RISK_TYPE & RISK_VALUE); or "fixed" | "percent" | "growth"
    "stake_value": None,
    "growth_cap_pct": 30.0,    # growth stake: never more than this % of the balance
    "max_per_window": 1,       # long-shot trades per 15m window (the regular slot is separate)
    "cutoff_s": 3.0,           # no new long shots in the last N seconds
    "min_ask_liquidity_usd": 2.0,  # dollars resting in the top 5 ask levels, at least
    "max_price_age_s": 3.0,    # skip if the settlement-feed price is older than this
    "slippage": 0.01,          # limit = ask + this (probability units), never above the band's max_price
    "order_type": "FAK",       # "FAK" = fill-and-kill: buys what's offered up to the limit at once,
                               #         the rest is cancelled (nothing rests on the book)
                               # "GTD" = rests resting_s seconds, then cancel and re-quote
    "max_attempts": 3,         # live orders per long shot (FAK: more orders for what's still unfilled)
    "resting_s": 4.0,          # GTD only: seconds a resting order waits before it is cancelled / re-quoted
    "vol_usd_per_sqrt_s": 0.0,  # BTC volatility in $ per sqrt(second); 0 = derive from the 5m candles
    "vol_fallback_usd_per_sqrt_s": 6.0,  # used when the candles give nothing
    "sd_floor_usd": 3.0,       # smallest uncertainty the twap model allows for the closing average
    "taker_fee_rate": 0.07,    # Polymarket's crypto taker fee: shares x rate x price x (1 - price).
                               # The bands' chance / price and chance - price count it (0 = ignore fees)
    # PATCH: longshot maker — resting post-only bids (makers pay no fee), next to the bands
    "maker": {
        "enabled": False,      # the maker bids' own switch (long shots must be on too)
        "live": False,         # False = paper: no order is sent, fills are estimated from the book and trades
        "price": 0.01,         # the most a bid pays: 0.1¢ steps below 4¢, whole cents from 4¢
        "min_prob": 0.20,      # a bid rests only while the model gives its side at least this
        "max_ask": 0.30,       # ... and the side's ask is at or under this (the cheap side; no ask = allowed)
        "look_s": 60,          # ... and only in the last N seconds of the window
        "cutoff_s": 3.0,       # every bid is cancelled N seconds before the close
        "max_fills_per_window": 1,  # bids that got a fill; once reached no new bids that window
        "stake_type": "",      # "" = the long-shot stake; or fixed / percent / growth like a band
        "stake_value": None,
        "growth_cap_pct": None,
        "requote_s": 2.0,      # a side's bid is placed at most once per N seconds (cancel/re-place churn)
        "poll_s": 1.0,         # live: how often a resting bid's fills are read
    },
    "bands": {
        # Cheap but already favoured by the model.
        "favored": {"enabled": True, "look_s": 60, "ask_min": 0.07, "ask_max": 0.10,
                    "max_price": 0.15, "min_prob": 0.55, "min_multiple": 0.0,
                    "stake_type": "", "stake_value": None, "growth_cap_pct": None},
        # Very cheap; fires when chance / ask is at least min_multiple.
        "final":   {"enabled": True, "look_s": 60, "ask_min": 0.01, "ask_max": 0.03,
                    "max_price": 0.04, "min_prob": 0.0, "min_multiple": 10.0,
                    "stake_type": "", "stake_value": None, "growth_cap_pct": None},
        # A band's own stake: stake_type "" = use the long-shot stake above; otherwise
        # "fixed" / "percent" with stake_value, or "growth" with growth_cap_pct (blank = the cap above).
    },
}

BAND_TEMPLATE = {"enabled": True, "look_s": 60, "ask_min": 0.0, "ask_max": 0.0,
                 "max_price": 0.0, "min_prob": 0.0, "min_multiple": 0.0,
                 "stake_type": "", "stake_value": None, "growth_cap_pct": None}

MAKER_DEFAULTS = DEFAULTS["maker"]

STAKE_TYPES = ("fixed", "percent", "growth", "regular", "")
ORDER_TYPES = ("FAK", "GTD")

MODELS = ("twap", "exact", "regular")
PTB_MODES = ("sampled_twap", "bot_strike")


def as_bool(v: Any, label: str = "value") -> bool:
    """true/false from JSON or a hand edit: "false", "0", "no", "off" are False (bool("false") is True)."""
    if v is None or isinstance(v, bool):
        return bool(v)
    if isinstance(v, (int, float)) and v in (0, 1):
        return bool(v)
    if isinstance(v, str):
        t = v.strip().lower()
        if t in ("true", "1", "yes", "on"):
            return True
        if t in ("false", "0", "no", "off", ""):
            return False
    raise ValueError(f"{label} must be true or false")


def merged(user_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """DEFAULTS overlaid with the user's settings (bands merged one level deep)."""
    out = copy.deepcopy(DEFAULTS)
    for k, v in (user_cfg or {}).items():
        if k == "bands" and isinstance(v, dict):
            for name, band in v.items():
                base = out["bands"].get(name, dict(BAND_TEMPLATE))
                if isinstance(band, dict):
                    base = {**base, **band}
                out["bands"][name] = base
        elif k == "maker" and isinstance(v, dict):   # PATCH: longshot maker — missing keys keep defaults
            out["maker"] = {**out["maker"], **v}
        else:
            out[k] = v
    return out


def normalize(user_cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """merged() plus type/range checks. Raises ValueError listing every bad field."""
    cfg = merged(user_cfg)
    errs: List[str] = []

    def num(obj, key, label, lo=None, hi=None, as_int=False):
        try:
            v = int(obj[key]) if as_int else float(obj[key])
        except (TypeError, ValueError):
            errs.append(f"{label} must be a number")
            return
        if (lo is not None and v < lo) or (hi is not None and v > hi):
            errs.append(f"{label} must be between {lo} and {hi}")
            return
        obj[key] = v

    for k in ("enabled", "live"):
        try:
            cfg[k] = as_bool(cfg.get(k), k)
        except ValueError as e:
            errs.append(str(e))
    if cfg.get("model") not in MODELS:
        errs.append(f"model must be one of {', '.join(MODELS)}")
    if cfg.get("price_to_beat") not in PTB_MODES:
        errs.append(f"price_to_beat must be one of {', '.join(PTB_MODES)}")
    if cfg.get("pick") not in ("multiple", "ev"):
        errs.append("pick must be multiple or ev")
    cfg["order_type"] = str(cfg.get("order_type") or "FAK").upper()
    if cfg["order_type"] not in ORDER_TYPES:
        errs.append("order_type must be FAK or GTD")
    cfg["stake_type"] = str(cfg.get("stake_type", "") or "").lower()
    if cfg["stake_type"] not in STAKE_TYPES:
        errs.append("stake_type must be fixed, percent, growth or regular")
    if cfg["stake_type"] in ("regular", ""):
        cfg["stake_value"] = None
    else:
        num(cfg, "stake_value", "stake", 0, 1_000_000)
    num(cfg, "growth_cap_pct", "growth cap", 0, 100)
    num(cfg, "max_per_window", "max per window", 0, 100, as_int=True)
    num(cfg, "cutoff_s", "cutoff", 0, 900)
    num(cfg, "min_ask_liquidity_usd", "min ask liquidity", 0, 1_000_000)
    num(cfg, "max_price_age_s", "max price age", 0.1, 3600)
    num(cfg, "slippage", "slippage", 0, 0.99)
    num(cfg, "max_attempts", "max attempts", 1, 50, as_int=True)
    num(cfg, "resting_s", "resting time", 0.2, 600)
    num(cfg, "vol_usd_per_sqrt_s", "volatility", 0, 10_000)
    num(cfg, "vol_fallback_usd_per_sqrt_s", "fallback volatility", 0.0001, 10_000)
    num(cfg, "sd_floor_usd", "sd floor", 0, 100_000)
    num(cfg, "taker_fee_rate", "taker fee rate", 0, 0.5)

    # PATCH: longshot maker
    mk = cfg.get("maker")
    if not isinstance(mk, dict):
        errs.append("maker must be an object")
    else:
        for k in ("enabled", "live"):
            try:
                mk[k] = as_bool(mk.get(k), f"maker.{k}")
            except ValueError as e:
                errs.append(str(e))
        num(mk, "price", "maker.price", 0.001, 0.5)
        num(mk, "min_prob", "maker.min_prob", 0, 1)
        num(mk, "max_ask", "maker.max_ask", 0.001, 1)
        num(mk, "look_s", "maker.look_s", 1, 900)
        num(mk, "cutoff_s", "maker.cutoff_s", 0, 900)
        num(mk, "max_fills_per_window", "maker.max_fills_per_window", 0, 100, as_int=True)
        num(mk, "requote_s", "maker.requote_s", 0.2, 600)
        num(mk, "poll_s", "maker.poll_s", 0.2, 60)
        if isinstance(mk.get("price"), float) and not on_price_grid(mk["price"]):
            errs.append("maker.price must be in 0.1¢ steps below 4¢ and whole cents from 4¢")
        if (isinstance(mk.get("cutoff_s"), float) and isinstance(mk.get("look_s"), float)
                and mk["cutoff_s"] >= mk["look_s"]):
            errs.append("maker.cutoff_s must be below maker.look_s")
        st = mk.get("stake_type")
        st = "" if st in (None, "", "inherit", "default", "regular") else str(st).lower()
        if st and st not in STAKE_TYPES:
            errs.append("maker.stake_type must be blank, fixed, percent, growth or regular")
        mk["stake_type"] = st
        for key, label, hi in (("stake_value", "stake", 1_000_000), ("growth_cap_pct", "growth cap", 100)):
            if mk.get(key) in (None, ""):
                mk[key] = None
            else:
                num(mk, key, f"maker.{label}", 0, hi)
        if st in ("fixed", "percent") and mk.get("stake_value") is None:
            errs.append(f"maker: a {st} stake needs a stake value")

    bands = cfg.get("bands")
    if not isinstance(bands, dict):
        errs.append("bands must be an object")
    else:
        for name, b in bands.items():
            if not isinstance(b, dict):
                errs.append(f"band {name} must be an object")
                continue
            try:
                b["enabled"] = as_bool(b.get("enabled", True), f"{name}.enabled")
            except ValueError as e:
                errs.append(str(e))
            num(b, "look_s", f"{name}.look_s", 0, 900)
            num(b, "ask_min", f"{name}.ask_min", 0, 1)
            num(b, "ask_max", f"{name}.ask_max", 0, 1)
            num(b, "max_price", f"{name}.max_price", 0, 1)
            num(b, "min_prob", f"{name}.min_prob", 0, 1)
            num(b, "min_multiple", f"{name}.min_multiple", 0, 1_000_000)
            try:
                if float(b["ask_min"]) > float(b["ask_max"]):
                    errs.append(f"{name}: ask_min is above ask_max")
            except (TypeError, ValueError, KeyError):
                pass
            # the band's own stake (blank = use the long-shot stake)
            st = b.get("stake_type")
            st = "" if st in (None, "", "inherit", "default", "regular") else str(st).lower()
            if st and st not in STAKE_TYPES:
                errs.append(f"{name}.stake_type must be blank, fixed, percent, growth or regular")
            b["stake_type"] = st
            for key, label, hi in (("stake_value", "stake", 1_000_000), ("growth_cap_pct", "growth cap", 100)):
                if b.get(key) in (None, ""):
                    b[key] = None
                else:
                    num(b, key, f"{name}.{label}", 0, hi)
            if st in ("fixed", "percent") and b.get("stake_value") is None:
                errs.append(f"{name}: a {st} band stake needs a stake value")
    if errs:
        raise ValueError("; ".join(errs))
    return cfg


def describe(cfg: Dict[str, Any]) -> str:
    """One log line with the active settings."""
    parts = []
    for name, b in (cfg.get("bands") or {}).items():
        if not b.get("enabled", True):
            continue
        cond = []
        if b.get("min_prob"):
            cond.append(f"p≥{b['min_prob']:.2f}")
        if b.get("min_multiple"):
            cond.append(f"×{b['min_multiple']:g}")
        own = f" stake {stake_text(*band_stake(cfg, name))}" if b.get("stake_type") else ""
        parts.append(f"{name} {b['ask_min']*100:g}–{b['ask_max']*100:g}¢ cap {b['max_price']*100:g}¢ "
                     f"{' '.join(cond) or 'no gate'} last {b['look_s']:g}s{own}")
    stake = stake_text(*band_stake(cfg))
    mk = cfg.get("maker") or {}
    maker = ""
    if mk.get("enabled"):   # PATCH: longshot maker
        maker = (f" | maker bids {float(mk['price']) * 100:g}¢ while chance ≥{float(mk['min_prob']):.2f} "
                 f"and ask ≤{float(mk['max_ask']) * 100:g}¢, "
                 f"last {float(mk['look_s']):g}s to {float(mk['cutoff_s']):g}s, stake {stake_text(*_stake_of(cfg, mk))}, "
                 f"max {mk.get('max_fills_per_window')} filled/window, {'live' if maker_live(cfg) else 'paper'}")
    return (f"model {cfg.get('model')}, price to beat {cfg.get('price_to_beat')}, stake {stake}, "
            f"{'live ' + str(cfg.get('order_type', 'FAK')) if cfg.get('live') else 'paper only'}, "
            f"max {cfg.get('max_per_window')}/window, taker fee {float(cfg.get('taker_fee_rate') or 0):g} | "
            + ("; ".join(parts) or "no bands enabled") + maker)


def max_look_s(cfg: Dict[str, Any]) -> float:
    vals = [float(b.get("look_s", 0)) for b in (cfg.get("bands") or {}).values() if b.get("enabled", True)]
    return max(vals) if vals else 0.0


# PATCH: longshot maker — the checks run while either the bands or the maker bids look
def maker_on(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("enabled") and (cfg.get("maker") or {}).get("enabled"))


def maker_live(cfg: Dict[str, Any]) -> bool:
    """Maker bids are real orders only when both long shots and the maker block are set live."""
    return bool(cfg.get("live") and (cfg.get("maker") or {}).get("live"))


def eval_look_s(cfg: Dict[str, Any]) -> float:
    look = max_look_s(cfg)
    if maker_on(cfg):
        look = max(look, float(cfg["maker"].get("look_s", 0)))
    return look


def eval_cutoff_s(cfg: Dict[str, Any]) -> float:
    cut = float(cfg.get("cutoff_s", 3.0))
    if maker_on(cfg):
        cut = min(cut, float(cfg["maker"].get("cutoff_s", cut)))
    return cut


def max_band_ask(cfg: Dict[str, Any]) -> float:
    vals = [float(b.get("ask_max", 0)) for b in (cfg.get("bands") or {}).values() if b.get("enabled", True)]
    return max(vals) if vals else 0.0


# ── Settlement-feed price recorder (for the 60-second average) ────────────────
_lock = threading.Lock()
_samples: Deque[Tuple[float, float]] = deque(maxlen=5000)   # (unix seconds, price)


def record_price(ts: float, price: Optional[float]) -> None:
    """Store a settlement-feed price. Out-of-order or duplicate timestamps are ignored."""
    try:
        ts, price = float(ts), float(price or 0)
    except (TypeError, ValueError):
        return
    if price <= 0:
        return
    with _lock:
        if _samples and ts <= _samples[-1][0]:
            return
        _samples.append((ts, price))
        cutoff = ts - 1200.0
        while _samples and _samples[0][0] < cutoff:
            _samples.popleft()


def samples() -> List[Tuple[float, float]]:
    with _lock:
        return list(_samples)


def clear_samples() -> None:
    with _lock:
        _samples.clear()
    _ptb_cache.clear()


def time_weighted_mean(pts: List[Tuple[float, float]], a: float, b: float) -> Tuple[Optional[float], float]:
    """Mean over [a, b] of the step line through pts (each price holds until the next one),
    and how many seconds of [a, b] that line covers (nothing before the first sample)."""
    if b <= a or not pts:
        return None, 0.0
    total = covered = 0.0
    cur_t = cur_p = None
    for t, p in pts:
        if t <= a:
            cur_t, cur_p = t, p
            continue
        if t >= b:
            break
        if cur_p is not None:
            seg = t - max(a, cur_t)
            if seg > 0:
                total += cur_p * seg
                covered += seg
        cur_t, cur_p = t, p
    if cur_p is not None:
        seg = b - max(a, cur_t)
        if seg > 0:
            total += cur_p * seg
            covered += seg
    return (total / covered, covered) if covered > 0 else (None, 0.0)


def sampled_twap(end_ts: float, pts: Optional[List[Tuple[float, float]]] = None,
                 min_cover_s: float = 50.0) -> Optional[float]:
    """The 60-second average ending at end_ts, from recorded prices; None if under min_cover_s covered."""
    mean, cov = time_weighted_mean(samples() if pts is None else pts, end_ts - 60.0, end_ts)
    return mean if (mean is not None and cov >= min_cover_s) else None


_ptb_cache: Dict[int, Optional[float]] = {}


def price_to_beat(start_ts: float, now_ts: float) -> Optional[float]:
    """Sampled 60-second average at the window start. Cached once the start is 5 s past."""
    key = int(round(start_ts))
    if key in _ptb_cache:
        return _ptb_cache[key]
    val = sampled_twap(start_ts)
    if now_ts >= start_ts + 5.0:
        _ptb_cache[key] = val
        for k in [k for k in _ptb_cache if k < key - 7200]:
            del _ptb_cache[k]
    return val


# ── Models ────────────────────────────────────────────────────────────────────
def _phi(z: float) -> float:
    return 0.5 * math.erfc(-z / math.sqrt(2.0))


def vol_usd_per_sqrt_s(cfg: Dict[str, Any], price: float, sigma_5m: Optional[float]) -> float:
    fixed = float(cfg.get("vol_usd_per_sqrt_s") or 0.0)
    if fixed > 0:
        return fixed
    if sigma_5m and sigma_5m > 0 and price and price > 0:
        return sigma_5m * price / math.sqrt(300.0)
    return float(cfg.get("vol_fallback_usd_per_sqrt_s") or 6.0)


def prob_up_twap(now_ts: float, end_ts: float, strike: float, price_now: float,
                 sigma_usd: float, sd_floor: float,
                 pts: Optional[List[Tuple[float, float]]] = None) -> Dict[str, float]:
    """Chance that the closing 60-second average ends at or above `strike`.

    N seconds before the close (N <= 60), (60-N)/60 of the average is already fixed: it is
    taken from the recorded prices. The rest is a random walk from price_now, whose average
    has standard deviation sigma * sqrt(N^3/3) / 60. With more than a minute left the whole
    average is still ahead: sigma * sqrt(N - 40)."""
    n = max(0.0, end_ts - now_ts)
    if n <= 60.0:
        elapsed = 60.0 - n
        obs, cov = time_weighted_mean(samples() if pts is None else pts, end_ts - 60.0, now_ts)
        if obs is None or cov < 0.5 * elapsed:
            obs = price_now          # too little history: treat the elapsed part as flat at price_now
        exp_avg = (obs * elapsed + price_now * n) / 60.0
        sd = sigma_usd * math.sqrt(n ** 3 / 3.0) / 60.0
    else:
        exp_avg = price_now
        sd = sigma_usd * math.sqrt(n - 40.0)
    sd = max(sd, float(sd_floor or 0.0), 1e-9)
    p = _phi((exp_avg - strike) / sd)
    return {"p_up": p, "expected_avg": exp_avg, "sd": sd, "secs_left": n}


def prob_up_exact(spot: float, strike: float, secs_left: float,
                  sigma_5m: Optional[float], drift_5m: float = 0.0) -> float:
    """The bot's own GBM formula, run on the real time left (fractions of a 5-minute step)."""
    if not spot or not strike or spot <= 0 or strike <= 0:
        return 0.5
    if not sigma_5m or sigma_5m <= 0:
        return 1.0 if spot > strike else (0.0 if spot < strike else 0.5)
    n = max(secs_left, 0.5) / 300.0
    mu = (drift_5m - 0.5 * sigma_5m ** 2) * n
    sd = sigma_5m * math.sqrt(n)
    z = (math.log(strike / spot) - mu) / sd
    return float(min(1.0, max(0.0, 0.5 * math.erfc(z / math.sqrt(2)))))


# ── Decision ──────────────────────────────────────────────────────────────────
def decide(p_up: float, books: Dict[str, Dict[str, Optional[float]]], secs_left: float,
           cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Pick at most one (side, band). books = {"UP": {"ask": .., "liq_usd": ..}, "DOWN": {..}}.

    A band fires on a side when: the band is enabled; secs_left is within its look_s and
    not under cutoff_s; ask_min <= ask <= ask_max and ask <= max_price; the side's chance
    >= min_prob; chance / ask >= min_multiple (0 = off); and the top-5 ask levels hold at
    least min_ask_liquidity_usd (skipped when the book doesn't say). When several fire,
    `pick` chooses: highest chance/ask ("multiple") or highest chance - ask ("ev").
    PATCH: longshot maker — "ask" in chance/ask and chance - ask is the ask plus the taker
    fee (taker_fee_rate): what a share really costs. The ask ranges and max_price stay on the
    book's own price."""
    cutoff = float(cfg.get("cutoff_s", 3.0))
    if secs_left < cutoff:
        return {"action": "NO_TRADE", "reason": "longshot_past_cutoff"}
    min_liq = float(cfg.get("min_ask_liquidity_usd", 0.0))
    fee = float(cfg.get("taker_fee_rate", 0.0) or 0.0)
    by_ev = cfg.get("pick") == "ev"
    best, reasons = None, []
    for side in ("UP", "DOWN"):
        p = p_up if side == "UP" else 1.0 - p_up
        bk = books.get(side) or {}
        ask = bk.get("ask")
        if ask is None or ask <= 0:
            continue
        liq = bk.get("liq_usd")
        for name, band in (cfg.get("bands") or {}).items():
            if not band.get("enabled", True) or secs_left > float(band.get("look_s", 60)):
                continue
            if not (float(band.get("ask_min", 0)) <= ask <= float(band.get("ask_max", 0))):
                continue
            if ask > float(band.get("max_price", 1.0)):
                continue
            eff = fee_adjusted(ask, fee)
            mult = p / eff
            if p < float(band.get("min_prob", 0.0)):
                reasons.append(f"{name} {side} p {p:.3f}<{float(band['min_prob']):.2f}")
                continue
            if float(band.get("min_multiple", 0.0)) > 0 and mult < float(band["min_multiple"]):
                reasons.append(f"{name} {side} ×{mult:.1f}<{float(band['min_multiple']):g}")
                continue
            if min_liq > 0 and liq is not None and liq < min_liq:
                reasons.append(f"{name} {side} thin ${liq:.2f}")
                continue
            cand = {"action": "ENTER", "side": side, "band": name, "prob": p, "price": ask, "eff_price": eff,
                    "multiple": mult, "ev": p - eff, "max_price": float(band.get("max_price", ask)),
                    "phase": "LONGSHOT", "strength": "LONGSHOT", "reason": f"longshot_{name}"}
            key = "ev" if by_ev else "multiple"
            if best is None or cand[key] > best[key]:
                best = cand
    if best:
        return best
    return {"action": "NO_TRADE", "reason": "; ".join(reasons[:4]) or "longshot_no_band"}


# ── Per-window "best seen" (for the end-of-window log line) ───────────────────
_window_best: Dict[int, Dict[str, Any]] = {}


def note_candidate(start_ms: int, side: str, p: float, ask: Optional[float], secs_left: float,
                   ask_ceiling: float, fee_rate: float = 0.0) -> None:
    if not ask or ask <= 0 or ask > ask_ceiling:
        return
    m = p / fee_adjusted(ask, fee_rate)
    cur = _window_best.get(start_ms)
    if cur is None or m > cur["multiple"]:
        _window_best[start_ms] = {"side": side, "prob": p, "ask": ask, "multiple": m,
                                  "secs_left": secs_left}


def pop_window_best(start_ms: int) -> Optional[Dict[str, Any]]:
    for k in [k for k in _window_best if k < start_ms - 7_200_000]:
        del _window_best[k]
    return _window_best.pop(start_ms, None)


# ── Pricing and sizing ────────────────────────────────────────────────────────
def tick_for(price: float) -> float:
    """Polymarket's tick is 0.1¢ below 4¢ and above 96¢, 1¢ in between."""
    return 0.001 if (price < 0.04 or price > 0.96) else 0.01


def _whole_cent(x: float) -> bool:
    return abs(x * 100.0 - round(x * 100.0)) < 1e-6


def tick_size_str(price: float) -> str:
    """Rounding used when the order is signed: "0.01" only for whole-cent prices, so signing
    never moves the price (a 4.6¢ limit signed with "0.01" would become 5¢)."""
    return "0.01" if _whole_cent(price) else "0.001"


def limit_price(ask: float, slippage: float, max_price: float) -> float:
    """ask + slippage, capped at max_price, rounded down to the book's tick, never below the
    ask. The book is on the 0.1¢ tick when the ask is under 4¢ / over 96¢ or not a whole cent."""
    ask = float(ask)
    raw = min(float(max_price), ask + max(0.0, float(slippage)))
    t = 0.001 if (tick_for(ask) < 0.01 or not _whole_cent(ask)) else 0.01
    px = math.floor(raw / t + 1e-9) * t
    if px < ask - 1e-9:
        px = ask
    return round(px, 3)


def growth_fraction(prob: Optional[float], price: Optional[float]) -> float:
    """Growth-maximizing share of the balance for a $1 payout bought at `price` with win chance
    `prob`: (prob - price) / (1 - price). 0 when there's no edge."""
    if prob is None or not price or price <= 0 or price >= 1:
        return 0.0
    return max(0.0, (float(prob) - float(price)) / (1.0 - float(price)))


def _stake_of(cfg: Dict[str, Any], b: Optional[Dict[str, Any]]) -> Tuple[str, float, float]:
    """(stake_type, stake_value, growth_cap_pct) of a band or the maker block: its own stake if it
    has a stake_type, else the long-shot stake (or bot's regular stake if blank)."""
    b = b or {}
    own = bool(b.get("stake_type"))
    raw_typ = b.get("stake_type") if own else cfg.get("stake_type")
    raw_typ = "" if raw_typ in (None, "", "inherit", "default", "regular") else str(raw_typ).lower()
    try:
        from bot.config import settings
        default_typ = (getattr(settings, "RISK_TYPE", "percent") or "percent").lower()
        default_val = float(getattr(settings, "RISK_VALUE", 10.0) or 10.0)
    except Exception:
        default_typ, default_val = "percent", 10.0

    typ = raw_typ if raw_typ else default_typ
    if own and b.get("stake_value") is not None and raw_typ:
        val = float(b["stake_value"])
    elif cfg.get("stake_value") is not None and raw_typ:
        val = float(cfg["stake_value"])
    else:
        val = default_val

    cap = b.get("growth_cap_pct") if own and b.get("growth_cap_pct") is not None else cfg.get("growth_cap_pct", 30.0)
    return typ, float(val or 0.0), float(cap or 0.0)


def band_stake(cfg: Dict[str, Any], band: Optional[str] = None) -> Tuple[str, float, float]:
    """(stake_type, stake_value, growth_cap_pct) for a band: its own stake if it has a stake_type,
    else the long-shot stake. A band's blank growth cap falls back to the long-shot cap."""
    return _stake_of(cfg, (cfg.get("bands") or {}).get(band))


def stake_text(typ: str, val: float, cap: float) -> str:
    return f"${val:g}" if typ == "fixed" else f"{val:g}%" if typ == "percent" else f"growth-max ≤{cap:g}%"


def _stake_amount(typ: str, val: float, cap: float, balance: float,
                  prob: Optional[float], price: Optional[float]) -> float:
    bal = max(0.0, float(balance or 0.0))
    if typ == "fixed":
        amt = val
    elif typ == "percent":
        amt = val / 100.0 * bal
    else:
        amt = min(growth_fraction(prob, price), cap / 100.0) * bal
    return max(0.0, min(amt, bal))


def stake_for(balance: float, cfg: Dict[str, Any], band: Optional[str] = None,
              prob: Optional[float] = None, price: Optional[float] = None) -> float:
    """Dollar stake for one long shot: fixed dollars, a % of the balance, or the growth-maximizing
    share of the balance for this chance and price, capped at growth_cap_pct. Never above the balance."""
    return _stake_amount(*band_stake(cfg, band), balance, prob, price)


def maker_stake(balance: float, cfg: Dict[str, Any], prob: Optional[float] = None,
                price: Optional[float] = None) -> float:
    """PATCH: longshot maker — stake_for() with the maker block's own stake (blank = long-shot stake).
    A maker pays no fee, so growth uses the bid price as it is."""
    return _stake_amount(*_stake_of(cfg, cfg.get("maker")), balance, prob, price)


def stake_usd(balance: float, cfg: Dict[str, Any]) -> float:
    """The long-shot stake without a band or chance (a growth stake needs both, so it is 0 here)."""
    return stake_for(balance, cfg)


def share_step(px: float) -> int:
    """Smallest whole number of shares whose cost at `px` is a whole number of cents: 1 at a
    whole-cent price, 2, 5 or 10 at a 0.1¢ price (2.3¢ -> 10 shares = 23¢)."""
    m = int(round(float(px) * 1000))
    return 10 // math.gcd(m, 10) if m > 0 else 1


def order_shares(usd: float, px: float, max_usd: Optional[float] = None) -> float:
    """Whole shares for about `usd` dollars at the limit price `px`: at least 5 shares and $1
    (Polymarket's minimums), in steps of share_step(px), so shares x price is a whole number of
    cents. Fill-and-kill orders need that (their dollar amount may have at most 2 decimals), and
    the amounts are then exact, so signing never rounds them. With max_usd (the balance), steps
    down to stay within it while the minimums allow; the caller checks the result against it."""
    if px <= 0:
        return 0.0
    step = share_step(px)
    shares = max(5, math.ceil(max(float(usd), 1.0) / px - 1e-9))
    shares = math.ceil(shares / step) * step
    while shares * px < 1.0 - 1e-9:
        shares += step
    if max_usd is not None:
        while (shares * px > float(max_usd) + 1e-9 and shares - step >= 5
               and (shares - step) * px >= 1.0 - 1e-9):
            shares -= step
    return float(shares)


# ── Fees and maker bids (PATCH: longshot maker) ───────────────────────────────
def fee_adjusted(price: Optional[float], rate: float) -> float:
    """What a share bought by taking the ask really costs: price + Polymarket's taker fee
    (shares x rate x price x (1 - price), so per share price x (1 + rate x (1 - price)))."""
    if not price or price <= 0:
        return float(price or 0.0)
    p = float(price)
    return p * (1.0 + max(0.0, float(rate or 0.0)) * max(0.0, 1.0 - p))


def taker_price(cfg: Dict[str, Any], ask: Optional[float]) -> float:
    """fee_adjusted() with the long-shot settings' taker_fee_rate."""
    return fee_adjusted(ask, float(cfg.get("taker_fee_rate", 0.0) or 0.0))


def on_price_grid(px: float) -> bool:
    """True for prices Polymarket accepts on the finest grid: 0.1¢ steps below 4¢, whole cents from 4¢."""
    m = float(px) * 1000.0
    if abs(m - round(m)) > 1e-6 or round(m) <= 0:
        return False
    return float(px) < 0.04 or int(round(m)) % 10 == 0


def maker_bid_price(cap: float, ask: Optional[float]) -> Optional[float]:
    """Where a maker bid rests: at `cap`, or one tick under the ask when the ask is at or under
    `cap` (a post-only bid at or above the ask would be refused, it would take the ask).
    Sub-cent prices need the 0.1¢ tick, which the book only uses while prices are under 4¢:
    with the ask at 4¢ or more (or no ask) a sub-cent price is rounded down to whole cents.
    None = no bid (nothing left above zero)."""
    cap = float(cap)
    px = cap
    if ask is not None and ask > 0 and float(ask) <= cap + 1e-9:
        px = float(ask) - tick_for(float(ask))
    fine = ask is not None and 0 < float(ask) < 0.04
    if not fine:
        px = math.floor(px * 100.0 + 1e-6) / 100.0
    px = math.floor(px * 1000.0 + 1e-6) / 1000.0
    return round(px, 3) if px >= 0.001 - 1e-9 else None


def maker_shares(usd: float, px: float, max_usd: Optional[float] = None) -> float:
    """Whole shares for about `usd` dollars at `px` for a resting bid: at least 5 (Polymarket's
    minimum size), in steps of share_step(px) so the cost is whole cents. A resting bid has no
    $1 minimum. With max_usd, steps down to stay within it while 5 shares allow."""
    if px <= 0:
        return 0.0
    step = share_step(px)
    shares = max(5, math.ceil(max(0.0, float(usd)) / px - 1e-9))
    shares = math.ceil(shares / step) * step
    if max_usd is not None:
        while shares * px > float(max_usd) + 1e-9 and shares - step >= 5:
            shares -= step
    return float(shares)


def ask_size_at_or_below(asks: List[Tuple[float, float]], px: float) -> float:
    """Shares offered at or below `px` (asks as (price, size) pairs)."""
    total = 0.0
    for a_px, a_sz in asks or []:
        try:
            if float(a_px) <= float(px) + 1e-9:
                total += max(0.0, float(a_sz))
        except (TypeError, ValueError):
            continue
    return total
