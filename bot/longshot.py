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
    "max_per_window": 1,       # long-shot trades per 15m window
    "look_s": 60,              # final minute look window in seconds
    "cutoff_s": 3.0,           # no new long shots in the last N seconds
    "maker": {
        "price": 0.01,         # resting maker bid price (1¢)
    },
    "bands": {
        "favored": {
            "ask_min": 0.07,
            "ask_max": 0.10,
            "max_price": 0.15,
            "min_prob": 0.55,
            "min_multiple": 0.0,
        },
        "final": {
            "ask_min": 0.01,
            "ask_max": 0.03,
            "max_price": 0.04,
            "min_prob": 0.0,
            "min_multiple": 10.0,
        },
    },
}

BAND_TEMPLATE = {
    "ask_min": 0.01,
    "ask_max": 0.03,
    "max_price": 0.04,
    "min_prob": 0.0,
    "min_multiple": 0.0,
}

MAKER_DEFAULTS = DEFAULTS["maker"]

ORDER_TYPES = ("GTD",)


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

    num(cfg, "max_per_window", "max per window", 0, 100, as_int=True)
    num(cfg, "look_s", "look window", 1, 900)
    cfg["order_type"] = "GTD"

    mk = cfg.get("maker")
    if isinstance(mk, dict):
        if "price" in mk:
            num(mk, "price", "maker.price", 0.001, 0.5)

    bands = cfg.get("bands")
    if isinstance(bands, dict):
        for name, b in bands.items():
            if isinstance(b, dict):
                if "ask_min" in b: num(b, "ask_min", f"{name}.ask_min", 0, 1)
                if "ask_max" in b: num(b, "ask_max", f"{name}.ask_max", 0, 1)
                if "max_price" in b: num(b, "max_price", f"{name}.max_price", 0, 1)
                if "min_prob" in b: num(b, "min_prob", f"{name}.min_prob", 0, 1)
                if "min_multiple" in b: num(b, "min_multiple", f"{name}.min_multiple", 0, 1000)
                if "ask_min" in b and "ask_max" in b and float(b["ask_min"]) > float(b["ask_max"]):
                    errs.append(f"{name}: ask_min is above ask_max")

    if errs:
        raise ValueError("; ".join(errs))
    return cfg


def describe(cfg: Dict[str, Any]) -> str:
    """One log line with the active settings."""
    mk = cfg.get("maker") or {}
    maker_str = f"maker {float(mk.get('price', 0.01))*100:g}¢ bid"
    band_parts = []
    for name, b in (cfg.get("bands") or {}).items():
        if not b.get("enabled", True):
            continue
        p_str = f", prob>={float(b['min_prob'])*100:g}%" if float(b.get("min_prob") or 0) > 0 else ""
        m_str = f", ×{float(b['min_multiple']):g}" if float(b.get("min_multiple") or 0) > 0 else ""
        band_parts.append(f"{name} {float(b.get('ask_min', 0))*100:g}–{float(b.get('ask_max', 0))*100:g}¢ (cap {float(b.get('max_price', 0))*100:g}¢{p_str}{m_str})")
    bands_str = " | ".join(band_parts)
    prefix = f"stake {stake_text()}, GTD, max {cfg.get('max_per_window', 1)}/window, last {cfg.get('look_s', 60)}s"
    if bands_str:
        return f"{prefix} | {bands_str} | {maker_str}"
    return f"{prefix} | {maker_str}"


def max_look_s(cfg: Dict[str, Any]) -> float:
    look = float(cfg.get("look_s", 60.0))
    for b in (cfg.get("bands") or {}).values():
        if b.get("enabled", True):
            look = max(look, float(b.get("look_s", look)))
    return look


# PATCH: longshot maker — the checks run while either the bands or the maker bids look
def maker_on(cfg: Dict[str, Any]) -> bool:
    mk = cfg.get("maker")
    if not isinstance(mk, dict):
        return False
    return float(mk.get("price", 0.0) or 0.0) > 0


def maker_live(cfg: Dict[str, Any]) -> bool:
    return True


def eval_look_s(cfg: Dict[str, Any]) -> float:
    look = max_look_s(cfg)
    if maker_on(cfg):
        look = max(look, float((cfg.get("maker") or {}).get("look_s", look)))
    return look


def eval_cutoff_s(cfg: Dict[str, Any]) -> float:
    return float(cfg.get("cutoff_s", 3.0))


def max_band_ask(cfg: Dict[str, Any]) -> float:
    vals = [float(b.get("ask_max", 0)) for b in (cfg.get("bands") or {}).values() if b.get("enabled", True)]
    return max(vals) if vals else 0.04


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
    """Pick at most one long-shot trade candidate (side, band) based on ask range and probability/multiple filters."""
    cutoff = float(cfg.get("cutoff_s", 3.0))
    if secs_left < cutoff:
        return {"action": "NO_TRADE", "reason": "longshot_past_cutoff"}
    best, reasons = None, []
    for side in ("UP", "DOWN"):
        p = p_up if side == "UP" else 1.0 - p_up
        bk = books.get(side) or {}
        ask = bk.get("ask")
        if ask is None or ask <= 0:
            continue
        for name, band in (cfg.get("bands") or {}).items():
            if not band.get("enabled", True):
                continue
            look = float(band.get("look_s") or cfg.get("look_s", 60.0))
            if secs_left > look:
                continue
            ask_min = float(band.get("ask_min", 0.01))
            ask_max = float(band.get("ask_max", 0.03))
            max_price = float(band.get("max_price", ask_max))
            if not (ask_min - 1e-9 <= ask <= ask_max + 1e-9):
                continue
            if ask > max_price + 1e-9:
                continue
            mult = p / ask if ask > 0 else 0.0

            # 1) Gating: min_multiple (e.g. 10x for final band: chance >= 10 * ask)
            min_mult = float(band.get("min_multiple") or 0.0)
            if min_mult > 0 and mult < min_mult - 1e-9:
                reasons.append(f"{side}_{name}_mult_{mult:.1f}<{min_mult:g}")
                continue

            # 2) Gating: min_prob (e.g. 0.55 for favored band: chance >= 55%)
            min_p = float(band.get("min_prob") or 0.0)
            if min_p > 0 and p < min_p - 1e-9:
                reasons.append(f"{side}_{name}_prob_{p:.3f}<{min_p:.2f}")
                continue

            cand = {
                "action": "ENTER",
                "side": side,
                "band": name,
                "prob": p,
                "price": ask,
                "multiple": mult,
                "max_price": max_price,
                "phase": "LONGSHOT",
                "strength": "LONGSHOT",
                "reason": f"longshot_{name}",
            }
            if best is None or cand["multiple"] > best["multiple"]:
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
    m = p / ask if ask > 0 else 0.0
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


def stake_text(*args, **kwargs) -> str:
    try:
        from bot.config import settings
        risk_type = (getattr(settings, "RISK_TYPE", "percent") or "percent").lower()
        risk_val = float(getattr(settings, "RISK_VALUE", 10.0) or 10.0)
    except Exception:
        risk_type, risk_val = "percent", 10.0
    return f"{risk_val:g}% balance" if risk_type == "percent" else f"${risk_val:g}"


def stake_for(balance: float, *args, **kwargs) -> float:
    """Dollar stake for one long shot: uses the bot's risk value at all times."""
    bal = max(0.0, float(balance or 0.0))
    if bal <= 0:
        return 0.0
    try:
        from bot.config import settings
        risk_type = (getattr(settings, "RISK_TYPE", "percent") or "percent").lower()
        risk_val = float(getattr(settings, "RISK_VALUE", 10.0) or 10.0)
    except Exception:
        risk_type, risk_val = "percent", 10.0
    if risk_type == "fixed":
        amt = risk_val
    else:
        amt = (risk_val / 100.0) * bal
    return max(1.0, min(amt, bal)) if bal >= 1.0 else 0.0


def maker_stake(balance: float, *args, **kwargs) -> float:
    """Maker stake: uses the bot's risk value at all times."""
    return stake_for(balance)


def stake_usd(balance: float, *args, **kwargs) -> float:
    return stake_for(balance)


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
    """Where a maker bid rests: at `cap` (default 1¢ = 0.01).
    If the ask is at or under `cap` (<= 0.01), placing a bid at 0.01 would take the ask
    (crossing the spread) and violate post-only, and bidding under 0.01 breaks Polymarket's
    0.01 minimum tick size rule. So no bid rests when ask <= cap.
    """
    cap = round(float(cap), 2)
    if cap <= 0:
        return None
    if ask is not None and float(ask) <= cap + 1e-9:
        return None
    return cap


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
