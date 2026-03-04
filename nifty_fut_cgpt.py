#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY FUT Automated Swing Trade Bot — ORB Breakout (Bullish + Bearish) with Scaling
===================================================================================
Production Railway Version:
✅ No Flask / No Plotly / No CSV
✅ Postgres logging (Railway DATABASE_URL)
✅ Kill switch from DB (trade_flag.live_nifty_orb_swing)
✅ Restart-safe open-trade recovery
✅ Stores ALL decision context (close, VWAP, ORB, pivots, SL/Target, risk/lot, etc.)

ENV VARS (Railway):
- API_KEY
- ACCESS_TOKEN
- DATABASE_URL
Optional:
- PAPER_TRADE=1  (default 1; set 0 for LIVE placing to Kite)

FIXES vs original:
  FIX-1  fetch_candles: guard against pre-market start (from_dt > to_dt crash)
  FIX-2  main(): pre-market wait loop — bot sleeps until 09:13 IST before init
  FIX-3  run(): safe exit time pushed to 15:20 so IDLE bot doesn't quit mid-session
         on an intra-day Railway restart
"""

import os, sys, time, datetime, threading, logging, json
from collections import defaultdict

from zoneinfo import ZoneInfo
IST = ZoneInfo("Asia/Kolkata")

from kiteconnect import KiteConnect, KiteTicker

import psycopg2
import psycopg2.extras

# =============================================================================
#  CONFIG
# =============================================================================

API_KEY      = os.getenv("API_KEY", "").strip()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if not API_KEY or not ACCESS_TOKEN or not DATABASE_URL:
    print("Missing env vars: API_KEY / ACCESS_TOKEN / DATABASE_URL")
    sys.exit(1)

BASE_SYMBOL      = "NIFTY"
EXCHANGE_FUT     = "NFO"
PRODUCT          = "MIS"
INTERVAL         = "minute"

ORB_MINUTES      = 15

PIVOT_LEFT        = 2
PIVOT_RIGHT       = 2
CHART_PIVOT_RIGHT = PIVOT_RIGHT
MIN_PIVOT_DIST    = 15

BREAK_BUFFER      = 5
RETEST_BUFFER     = 5
MIN_SETUP_POINTS  = 15

SL_BUFFER         = 10

VWAP_SL_POINTS        = 10
VWAP_CONSEC_CLOSES    = 3

LOTS              = 1
LOT_SIZE          = 50
ADD_LOTS          = 1
MAX_TOTAL_LOTS    = 6
MAX_SL_POINTS     = 300
MAX_TRADES_DAY    = 10

SQUARE_OFF_TIME   = (15, 15)

PAPER_TRADE       = (os.getenv("PAPER_TRADE", "1").strip().lower() not in ("0", "false", "no"))

# =============================================================================
#  LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s IST | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("orb_swing")

# =============================================================================
#  DB LAYER
# =============================================================================

def db_conn():
    return psycopg2.connect(DATABASE_URL)

def db_init():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_flag (
                id SERIAL PRIMARY KEY,
                live_nifty_orb_swing BOOLEAN DEFAULT TRUE
            );
            """)
            cur.execute("""
            INSERT INTO trade_flag (live_nifty_orb_swing)
            SELECT TRUE
            WHERE NOT EXISTS (SELECT 1 FROM trade_flag);
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS nifty_orb_swing_trades (
                id SERIAL PRIMARY KEY,
                trade_date DATE NOT NULL,
                trade_no INT NOT NULL,
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,

                bias TEXT,
                direction TEXT,

                entry_time TIMESTAMP,
                entry_price DOUBLE PRECISION,
                entry_close DOUBLE PRECISION,
                entry_vwap DOUBLE PRECISION,
                entry_qty INT,
                entry_lots INT,

                orb_high DOUBLE PRECISION,
                orb_low DOUBLE PRECISION,

                origin_type TEXT,
                origin_price DOUBLE PRECISION,
                origin_idx INT,
                origin_dt TIMESTAMP,

                trigger_pivot_type TEXT,
                trigger_pivot_price DOUBLE PRECISION,
                trigger_pivot_idx INT,
                trigger_pivot_dt TIMESTAMP,

                setup_size DOUBLE PRECISION,
                sl_price DOUBLE PRECISION,
                target_price DOUBLE PRECISION,
                risk_per_lot DOUBLE PRECISION,

                exit_time TIMESTAMP,
                exit_price DOUBLE PRECISION,
                exit_close DOUBLE PRECISION,
                exit_vwap DOUBLE PRECISION,
                exit_reason TEXT,

                pnl_points DOUBLE PRECISION,
                pnl_rs DOUBLE PRECISION,
                day_pnl_rs DOUBLE PRECISION,

                status TEXT DEFAULT 'OPEN',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            """)
            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orb_swing_trades_date_status
            ON nifty_orb_swing_trades (trade_date, status);
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS nifty_orb_swing_events (
                id SERIAL PRIMARY KEY,
                trade_date DATE NOT NULL,
                trade_no INT NOT NULL,
                symbol TEXT NOT NULL,
                ts TIMESTAMP NOT NULL,

                event_type TEXT NOT NULL,
                note TEXT,

                candle_dt TIMESTAMP,
                candle_idx INT,
                close DOUBLE PRECISION,
                vwap DOUBLE PRECISION,

                direction TEXT,
                qty INT,
                avg_entry DOUBLE PRECISION,
                sl DOUBLE PRECISION,
                target DOUBLE PRECISION,

                pivot_type TEXT,
                pivot_price DOUBLE PRECISION,
                pivot_idx INT,
                pivot_dt TIMESTAMP,

                origin_type TEXT,
                origin_price DOUBLE PRECISION,
                origin_idx INT,
                origin_dt TIMESTAMP,

                orb_high DOUBLE PRECISION,
                orb_low DOUBLE PRECISION,

                extra JSONB
            );
            """)
    log.info("[DB] Initialized tables")

def db_kill_switch_read() -> bool:
    """True => allowed to run. False => stop."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT live_nifty_orb_swing FROM trade_flag LIMIT 1;")
            row = cur.fetchone()
            return bool(row[0]) if row else True

def db_event(trade_date, trade_no, symbol, event_type, note=None, **kwargs):
    extra = kwargs.pop("extra", None)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO nifty_orb_swing_events (
                trade_date, trade_no, symbol, ts, event_type, note,
                candle_dt, candle_idx, close, vwap,
                direction, qty, avg_entry, sl, target,
                pivot_type, pivot_price, pivot_idx, pivot_dt,
                origin_type, origin_price, origin_idx, origin_dt,
                orb_high, orb_low,
                extra
            ) VALUES (
                %(trade_date)s, %(trade_no)s, %(symbol)s, %(ts)s, %(event_type)s, %(note)s,
                %(candle_dt)s, %(candle_idx)s, %(close)s, %(vwap)s,
                %(direction)s, %(qty)s, %(avg_entry)s, %(sl)s, %(target)s,
                %(pivot_type)s, %(pivot_price)s, %(pivot_idx)s, %(pivot_dt)s,
                %(origin_type)s, %(origin_price)s, %(origin_idx)s, %(origin_dt)s,
                %(orb_high)s, %(orb_low)s,
                %(extra)s
            );
            """, {
                "trade_date": trade_date,
                "trade_no": trade_no,
                "symbol": symbol,
                "ts": kwargs.get("ts") or datetime.datetime.now(IST).replace(tzinfo=None),
                "event_type": event_type,
                "note": note,
                "candle_dt": kwargs.get("candle_dt"),
                "candle_idx": kwargs.get("candle_idx"),
                "close": kwargs.get("close"),
                "vwap": kwargs.get("vwap"),
                "direction": kwargs.get("direction"),
                "qty": kwargs.get("qty"),
                "avg_entry": kwargs.get("avg_entry"),
                "sl": kwargs.get("sl"),
                "target": kwargs.get("target"),
                "pivot_type": kwargs.get("pivot_type"),
                "pivot_price": kwargs.get("pivot_price"),
                "pivot_idx": kwargs.get("pivot_idx"),
                "pivot_dt": kwargs.get("pivot_dt"),
                "origin_type": kwargs.get("origin_type"),
                "origin_price": kwargs.get("origin_price"),
                "origin_idx": kwargs.get("origin_idx"),
                "origin_dt": kwargs.get("origin_dt"),
                "orb_high": kwargs.get("orb_high"),
                "orb_low": kwargs.get("orb_low"),
                "extra": psycopg2.extras.Json(extra) if extra is not None else None
            })

def db_trade_open_insert(symbol: str, mode: str, trade_no: int, trade_date: datetime.date, payload: dict):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO nifty_orb_swing_trades (
                trade_date, trade_no, symbol, mode,
                bias, direction,
                entry_time, entry_price, entry_close, entry_vwap,
                entry_qty, entry_lots,
                orb_high, orb_low,
                origin_type, origin_price, origin_idx, origin_dt,
                trigger_pivot_type, trigger_pivot_price, trigger_pivot_idx, trigger_pivot_dt,
                setup_size, sl_price, target_price, risk_per_lot,
                status, updated_at
            )
            VALUES (
                %(trade_date)s, %(trade_no)s, %(symbol)s, %(mode)s,
                %(bias)s, %(direction)s,
                %(entry_time)s, %(entry_price)s, %(entry_close)s, %(entry_vwap)s,
                %(entry_qty)s, %(entry_lots)s,
                %(orb_high)s, %(orb_low)s,
                %(origin_type)s, %(origin_price)s, %(origin_idx)s, %(origin_dt)s,
                %(trigger_pivot_type)s, %(trigger_pivot_price)s, %(trigger_pivot_idx)s, %(trigger_pivot_dt)s,
                %(setup_size)s, %(sl_price)s, %(target_price)s, %(risk_per_lot)s,
                'OPEN', NOW()
            );
            """, payload)

def db_trade_close_update(trade_date: datetime.date, trade_no: int, payload: dict):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE nifty_orb_swing_trades
            SET
                exit_time=%(exit_time)s,
                exit_price=%(exit_price)s,
                exit_close=%(exit_close)s,
                exit_vwap=%(exit_vwap)s,
                exit_reason=%(exit_reason)s,
                pnl_points=%(pnl_points)s,
                pnl_rs=%(pnl_rs)s,
                day_pnl_rs=%(day_pnl_rs)s,
                status='CLOSED',
                updated_at=NOW()
            WHERE trade_date=%(trade_date)s AND trade_no=%(trade_no)s AND status='OPEN';
            """, {**payload, "trade_date": trade_date, "trade_no": trade_no})

def db_today_stats(trade_date: datetime.date):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT
              COALESCE(MAX(trade_no), 0) AS max_trade_no,
              COALESCE(SUM(CASE WHEN status='CLOSED' THEN pnl_rs ELSE 0 END), 0) AS pnl_closed
            FROM nifty_orb_swing_trades
            WHERE trade_date=%s;
            """, (trade_date,))
            row = cur.fetchone()
            return int(row[0] or 0), float(row[1] or 0.0)

def db_load_open_trade(trade_date: datetime.date):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
            SELECT *
            FROM nifty_orb_swing_trades
            WHERE trade_date=%s AND status='OPEN'
            ORDER BY id DESC
            LIMIT 1;
            """, (trade_date,))
            return cur.fetchone()

# =============================================================================
#  AUTO-DETECT CURRENT MONTH NIFTY FUT
# =============================================================================

_INSTR_CACHE = {"data": None}

def _get_instruments_cached(kite, exchange: str):
    if _INSTR_CACHE["data"] is None:
        log.info(f"[INSTR] Downloading instruments for {exchange} (first run only)...")
        _INSTR_CACHE["data"] = kite.instruments(exchange)
        log.info(f"[INSTR] Loaded {len(_INSTR_CACHE['data'])} rows.")
    return _INSTR_CACHE["data"]

def get_current_month_nifty_future(kite):
    today = datetime.datetime.now(IST).date()
    instruments = _get_instruments_cached(kite, EXCHANGE_FUT)

    futs = [
        i for i in instruments
        if i.get("segment") == "NFO-FUT"
        and i.get("name") == BASE_SYMBOL
        and i.get("expiry") is not None
        and i["expiry"] >= today
    ]
    if not futs:
        raise RuntimeError("No NIFTY FUT contracts found in instruments().")

    futs.sort(key=lambda x: x["expiry"])
    c = futs[0]

    log.info("=" * 62)
    log.info(f"[FUT] Selected: {c['tradingsymbol']}  Expiry: {c['expiry']}")
    log.info("=" * 62)

    return int(c["instrument_token"]), str(c["tradingsymbol"]), c["expiry"]

# =============================================================================
#  VWAP
# =============================================================================

def compute_vwap_series(candles: list):
    if not candles:
        return []
    cum_pv = 0.0
    cum_v  = 0.0
    out = []
    for c in candles:
        v = float(c.get("volume", 0) or 0)
        if v <= 0:
            v = 1.0
        tp = (float(c["high"]) + float(c["low"]) + float(c["close"])) / 3.0
        cum_pv += tp * v
        cum_v  += v
        out.append(cum_pv / cum_v if cum_v else float(c["close"]))
    return out

# =============================================================================
#  PIVOT DETECTION
# =============================================================================

def detect_pivots(candles: list, left: int, right: int, min_dist: float = None) -> dict:
    if min_dist is None:
        min_dist = MIN_PIVOT_DIST
    n = len(candles)
    raw_highs, raw_lows = [], []

    for i in range(left, n - right if right > 0 else n):
        hi  = candles[i]["high"]
        lo  = candles[i]["low"]
        lhi = [candles[j]["high"] for j in range(i - left, i)]
        rhi = [candles[j]["high"] for j in range(i + 1, i + right + 1)]
        llo = [candles[j]["low"]  for j in range(i - left, i)]
        rlo = [candles[j]["low"]  for j in range(i + 1, i + right + 1)]

        if (hi > max(lhi) if lhi else True) and (hi > max(rhi) if rhi else True):
            raw_highs.append({"dt": candles[i]["dt"], "price": hi, "idx": i})
        if (lo < min(llo) if llo else True) and (lo < min(rlo) if rlo else True):
            raw_lows.append({"dt": candles[i]["dt"], "price": lo, "idx": i})

    def _filter(pivots, keep="high", opposite=None):
        if not pivots or min_dist <= 0:
            return pivots
        opp_set = {p["idx"] for p in (opposite or [])}
        out = [pivots[0]]
        for p in pivots[1:]:
            last = out[-1]
            dist = abs(p["price"] - last["price"])
            has_opp = any(last["idx"] < oi < p["idx"] for oi in opp_set)
            if dist < min_dist and not has_opp:
                if keep == "high" and p["price"] > last["price"]:
                    out[-1] = p
                elif keep == "low" and p["price"] < last["price"]:
                    out[-1] = p
            else:
                out.append(p)
        return out

    all_highs = _filter(raw_highs, "high", opposite=raw_lows)
    all_lows  = _filter(raw_lows,  "low",  opposite=raw_highs)
    return {
        "last_high": all_highs[-1] if all_highs else None,
        "last_low":  all_lows[-1]  if all_lows  else None,
        "all_highs": all_highs,
        "all_lows":  all_lows,
    }

# =============================================================================
#  CANDLE BUILDER
# =============================================================================

class CandleBuilder:
    def __init__(self):
        self.current   = None
        self.completed = []

    def on_tick(self, price: float, ts: datetime.datetime):
        minute = ts.replace(second=0, microsecond=0, tzinfo=None)
        if self.current is None:
            self._open(minute, price); return None
        if minute == self.current["dt"]:
            self.current["high"]  = max(self.current["high"],  price)
            self.current["low"]   = min(self.current["low"],   price)
            self.current["close"] = price
            return None
        closed = dict(self.current)
        self.completed.append(closed)
        self._open(minute, price)
        return closed

    def _open(self, m, p):
        self.current = {"dt": m, "open": p, "high": p, "low": p, "close": p, "volume": 0}

    def get_all(self):
        return self.completed

# =============================================================================
#  PIVOT REGISTRY
# =============================================================================

class PivotRegistry:
    def __init__(self, pivot_type: str):
        self._type   = pivot_type
        self._pivots = []

    def update(self, all_pivots: list):
        known = {p["idx"] for p in self._pivots}
        for p in all_pivots:
            if p["idx"] not in known:
                self._pivots.append({"price": p["price"], "idx": p["idx"], "dt": p["dt"]})

    def next_entry_level(self, close: float, origin_idx: int, origin_price: float,
                         last_used_price: float, used_prices: set):
        if self._type == "high":
            candidates = [
                p for p in self._pivots
                if p["idx"] < origin_idx
                and p["price"] > last_used_price
                and p["price"] >= origin_price + MIN_SETUP_POINTS
                and close >= p["price"] - RETEST_BUFFER
                and p["price"] not in used_prices
            ]
            return min(candidates, key=lambda p: p["price"]) if candidates else None
        else:
            candidates = [
                p for p in self._pivots
                if p["idx"] < origin_idx
                and p["price"] < last_used_price
                and p["price"] <= origin_price - MIN_SETUP_POINTS
                and close <= p["price"] + RETEST_BUFFER
                and p["price"] not in used_prices
            ]
            return max(candidates, key=lambda p: p["price"]) if candidates else None

    def reset(self):
        self._pivots = []

# =============================================================================
#  TRADE STATE
# =============================================================================

class TradeState:
    IDLE = "IDLE"
    OPEN = "OPEN"

    def __init__(self):
        self.status           = self.IDLE
        self.direction        = None
        self.total_qty        = 0
        self.avg_entry_price  = None
        self.sl_trigger       = None
        self.target_price     = None
        self.entry_time       = None
        self.entry_candle     = None

        self.vwap_breach_count = 0
        self.last_vwap_side    = None

        self.used_prices_in_trade = set()
        self.last_added_price     = 0.0

        self.trades_today  = 0
        self.pnl_today     = 0.0

        self.day_bias  = "NONE"
        self.algo_dead = False

        self.orb_high  = None
        self.orb_low   = None

        self.armed        = False
        self.origin_price = None
        self.origin_idx   = None
        self.origin_type  = None
        self.origin_dt    = None

        self.sh_registry = PivotRegistry("high")
        self.sl_registry = PivotRegistry("low")

        self._last_skip_price = None

        # DB runtime
        self.trade_date = datetime.datetime.now(IST).date()
        self.trade_no = 0
        self.symbol = "NIFTYFUT"
        self.mode = "PAPER" if PAPER_TRADE else "LIVE"

    def arm(self, price: float, idx: int, pivot_type: str, dt: datetime.datetime):
        self.armed        = True
        self.origin_price = price
        self.origin_idx   = idx
        self.origin_type  = pivot_type
        self.origin_dt    = dt
        self._last_skip_price = None
        label = "swing LOW" if pivot_type == "low" else "swing HIGH"
        log.info(f"[ARM] {label} armed — origin {price:.2f}  (candle #{idx})")

    def disarm(self, reason: str = ""):
        self.armed        = False
        self.origin_price = None
        self.origin_idx   = None
        self.origin_type  = None
        self.origin_dt    = None
        self._last_skip_price = None
        if reason:
            log.info(f"[DISARM] {reason}")

    def reset_trade(self):
        self.status           = self.IDLE
        self.direction        = None
        self.total_qty        = 0
        self.avg_entry_price  = None
        self.sl_trigger       = None
        self.target_price     = None
        self.entry_time       = None
        self.entry_candle     = None
        self.used_prices_in_trade = set()
        self.last_added_price     = 0.0
        self.vwap_breach_count    = 0
        self.last_vwap_side       = None

# =============================================================================
#  ORB
# =============================================================================

def compute_orb(candles: list):
    if not candles:
        return None
    day   = candles[-1]["dt"].date()
    end_t = datetime.time(9, 15 + ORB_MINUTES)
    orb_c = [
        c for c in candles
        if c["dt"].date() == day
        and datetime.time(9, 15) <= c["dt"].time() < end_t
    ]
    if not orb_c:
        return None
    return {
        "high":   max(c["high"] for c in orb_c),
        "low":    min(orb_c[0]["open"], min(c["low"] for c in orb_c)),
        "open":   orb_c[0]["open"],
        "formed": len(orb_c) >= ORB_MINUTES,
        "day":    day,
    }

# =============================================================================
#  STRATEGY CORE
# =============================================================================

class StrategyCore:
    def __init__(self, state: TradeState, paper: bool = True, kite=None, symbol="NIFTYFUT"):
        self.state  = state
        self.paper  = paper
        self.kite   = kite
        self.symbol = symbol

    def on_candle_close(self, candles: list, candle_idx: int):
        candle = candles[candle_idx]
        s      = self.state
        t      = candle["dt"].time()
        close  = candle["close"]

        if candle_idx < PIVOT_LEFT + PIVOT_RIGHT + 1:
            return "WARMUP"
        if t < datetime.time(9, 15 + ORB_MINUTES):
            return "ORB_FORMING"

        orb = compute_orb(candles[:candle_idx + 1])
        if orb is None or not orb["formed"]:
            return "ORB_NOT_READY"

        if s.orb_high is None:
            s.orb_high = orb["high"]
            s.orb_low  = orb["low"]
            log.info(f"[ORB] Formed  H:{s.orb_high:.2f}  L:{s.orb_low:.2f}")
            db_event(s.trade_date, s.trade_no, s.symbol, "INFO", note="ORB_FORMED",
                     candle_dt=candle["dt"], candle_idx=candle_idx, close=close,
                     orb_high=s.orb_high, orb_low=s.orb_low)

        if s.algo_dead:
            return "ALGO_DEAD"

        pivots = detect_pivots(candles[:candle_idx + 1], PIVOT_LEFT, PIVOT_RIGHT)
        s.sh_registry.update(pivots["all_highs"])
        s.sl_registry.update(pivots["all_lows"])

        vwap_series = compute_vwap_series(candles[:candle_idx + 1])
        vwap = vwap_series[-1] if vwap_series else None

        # ---- OPEN POSITION FLOW ----
        if s.status == TradeState.OPEN:
            result = self._check_invalidation(candle, close, vwap, candle_idx)
            if result:
                return result

            vwap_res = self._check_vwap_stop(candle, close, vwap, candle_idx)
            if vwap_res:
                return vwap_res

            self._maybe_add(candle, candle_idx, close, pivots, vwap)
            return self._check_exit(candle, candle_idx, close, vwap)

        if s.trades_today >= MAX_TRADES_DAY:
            return "MAX_TRADES"

        # ---- SET DAY BIAS ON FIRST ORB BREAK ----
        if s.day_bias == "NONE":
            if close < orb["low"] - BREAK_BUFFER:
                s.day_bias = "BEARISH"
                log.info(f"[BIAS] BEARISH — ORB low {orb['low']:.2f} broken  (close {close:.2f})")
                db_event(s.trade_date, s.trade_no, s.symbol, "INFO", note="BIAS_SET_BEARISH",
                         candle_dt=candle["dt"], candle_idx=candle_idx, close=close, vwap=vwap,
                         orb_high=s.orb_high, orb_low=s.orb_low,
                         extra={"orb_low": orb["low"], "break_buffer": BREAK_BUFFER})
            elif close > orb["high"] + BREAK_BUFFER:
                s.day_bias = "BULLISH"
                log.info(f"[BIAS] BULLISH — ORB high {orb['high']:.2f} broken  (close {close:.2f})")
                db_event(s.trade_date, s.trade_no, s.symbol, "INFO", note="BIAS_SET_BULLISH",
                         candle_dt=candle["dt"], candle_idx=candle_idx, close=close, vwap=vwap,
                         orb_high=s.orb_high, orb_low=s.orb_low,
                         extra={"orb_high": orb["high"], "break_buffer": BREAK_BUFFER})
            else:
                return "WAITING_ORB_BREAK"

        # ---- BEARISH: arm on swing LOW -> entry at swing HIGH ----
        if s.day_bias == "BEARISH":
            last_low = pivots["last_low"]
            if last_low is None:
                return "NO_PIVOT_LOW"
            if s.origin_idx != last_low["idx"]:
                s.arm(last_low["price"], last_low["idx"], "low", last_low["dt"])
                return "SWING_LOW_ARMED"
            if s.armed:
                return self._check_sell_entry(candle, candle_idx, close, vwap)

        # ---- BULLISH: arm on swing HIGH -> entry at swing LOW ----
        if s.day_bias == "BULLISH":
            last_high = pivots["last_high"]
            if last_high is None:
                return "NO_PIVOT_HIGH"
            if s.origin_idx != last_high["idx"]:
                s.arm(last_high["price"], last_high["idx"], "high", last_high["dt"])
                return "SWING_HIGH_ARMED"
            if s.armed:
                return self._check_buy_entry(candle, candle_idx, close, vwap)

        return "WAITING_SETUP"

    # -------------------------------------------------------------------------
    #  VWAP STOP
    # -------------------------------------------------------------------------
    def _check_vwap_stop(self, candle, close: float, vwap: float, candle_idx: int):
        s = self.state
        if vwap is None:
            return None

        if s.direction == "BUY":
            threshold = vwap - VWAP_SL_POINTS
            if close < threshold:
                s.vwap_breach_count += 1
                s.last_vwap_side = "BELOW"
                log.info(f"[VWAP] BUY breach {s.vwap_breach_count}/{VWAP_CONSEC_CLOSES} close {close:.2f} < {threshold:.2f}")
            else:
                s.vwap_breach_count = 0
                s.last_vwap_side = None

            if s.vwap_breach_count >= VWAP_CONSEC_CLOSES:
                log.warning("[VWAP_SL] BUY -> EXIT + ALGO DEAD")
                self._exit(candle["dt"], close, "VWAP_SL", close=close, vwap=vwap, candle_idx=candle_idx)
                s.algo_dead = True
                return "VWAP_SL_EXIT_DEAD"

        elif s.direction == "SELL":
            threshold = vwap + VWAP_SL_POINTS
            if close > threshold:
                s.vwap_breach_count += 1
                s.last_vwap_side = "ABOVE"
                log.info(f"[VWAP] SELL breach {s.vwap_breach_count}/{VWAP_CONSEC_CLOSES} close {close:.2f} > {threshold:.2f}")
            else:
                s.vwap_breach_count = 0
                s.last_vwap_side = None

            if s.vwap_breach_count >= VWAP_CONSEC_CLOSES:
                log.warning("[VWAP_SL] SELL -> EXIT + ALGO DEAD")
                self._exit(candle["dt"], close, "VWAP_SL", close=close, vwap=vwap, candle_idx=candle_idx)
                s.algo_dead = True
                return "VWAP_SL_EXIT_DEAD"

        return None

    # -------------------------------------------------------------------------
    #  INVALIDATION
    # -------------------------------------------------------------------------
    def _check_invalidation(self, candle, close: float, vwap: float, candle_idx: int):
        s = self.state

        if s.day_bias == "BEARISH":
            if s.orb_high is not None and close > s.orb_high:
                log.warning(f"[INVALIDATION] BEARISH: close {close:.2f} > ORB HIGH {s.orb_high:.2f} — EXIT ALL, ALGO DEAD")
                self._exit(candle["dt"], close, "ORB_HIGH_BREAK_ALGO_DEAD", close=close, vwap=vwap, candle_idx=candle_idx)
                s.algo_dead = True
                return "INVALIDATION_DEAD"

        elif s.day_bias == "BULLISH":
            if s.orb_low is not None and close < s.orb_low:
                log.warning(f"[INVALIDATION] BULLISH: close {close:.2f} < ORB LOW {s.orb_low:.2f} — EXIT ALL, ALGO DEAD")
                self._exit(candle["dt"], close, "ORB_LOW_BREAK_ALGO_DEAD", close=close, vwap=vwap, candle_idx=candle_idx)
                s.algo_dead = True
                return "INVALIDATION_DEAD"

        return None

    # -------------------------------------------------------------------------
    #  ENTRY — BEARISH
    # -------------------------------------------------------------------------
    def _check_sell_entry(self, candle, candle_idx, close, vwap) -> str:
        s = self.state

        sh = s.sh_registry.next_entry_level(
            close=close,
            origin_idx=s.origin_idx,
            origin_price=s.origin_price,
            last_used_price=0.0,
            used_prices=set()
        )
        if sh is None:
            return "ARMED_WATCHING"

        setup_size = sh["price"] - s.origin_price
        if setup_size < MIN_SETUP_POINTS:
            return "SETUP_TOO_SMALL"

        sl_trigger = sh["price"] + SL_BUFFER
        sl_dist    = sl_trigger - close
        if sl_dist <= 0:
            return "SL_INVALID"
        if sl_dist > MAX_SL_POINTS:
            return "SL_TOO_WIDE"

        target = s.origin_price

        log.info("=" * 62)
        log.info(f"[SELL] BEARISH ENTRY [{candle['dt'].strftime('%H:%M')}] {self.symbol}")
        log.info(f"   Close      : {close:.2f}")
        log.info(f"   Swing High : {sh['price']:.2f} (formed {sh['dt'].strftime('%H:%M')})")
        log.info(f"   SL         : {sl_trigger:.2f}")
        log.info(f"   Target     : {target:.2f}")
        log.info(f"   Setup size : {setup_size:.1f} pts")
        log.info(f"   Risk/lot   : Rs.{sl_dist * LOT_SIZE:,.0f}")
        log.info("=" * 62)

        s.disarm("sell entry taken")
        self._place_order(
            candle=candle,
            direction="SELL",
            entry=close,
            sl_trigger=sl_trigger,
            target=target,
            candle_idx=candle_idx,
            is_add=False,
            pivot_price_used=sh["price"],
            pivot_idx=sh["idx"],
            pivot_dt=sh["dt"],
            pivot_type="high",
            setup_size=setup_size,
            close=close,
            vwap=vwap
        )
        return "SELL_ENTRY"

    # -------------------------------------------------------------------------
    #  ENTRY — BULLISH
    # -------------------------------------------------------------------------
    def _check_buy_entry(self, candle, candle_idx, close, vwap) -> str:
        s = self.state

        sl = s.sl_registry.next_entry_level(
            close=close,
            origin_idx=s.origin_idx,
            origin_price=s.origin_price,
            last_used_price=float("inf"),
            used_prices=set()
        )
        if sl is None:
            return "ARMED_WATCHING"

        setup_size = s.origin_price - sl["price"]
        if setup_size < MIN_SETUP_POINTS:
            return "SETUP_TOO_SMALL"

        sl_trigger = sl["price"] - SL_BUFFER
        sl_dist    = close - sl_trigger
        if sl_dist <= 0:
            return "SL_INVALID"
        if sl_dist > MAX_SL_POINTS:
            return "SL_TOO_WIDE"

        target = s.origin_price

        log.info("=" * 62)
        log.info(f"[BUY] BULLISH ENTRY [{candle['dt'].strftime('%H:%M')}] {self.symbol}")
        log.info(f"   Close      : {close:.2f}")
        log.info(f"   Swing Low  : {sl['price']:.2f} (formed {sl['dt'].strftime('%H:%M')})")
        log.info(f"   SL         : {sl_trigger:.2f}")
        log.info(f"   Target     : {target:.2f}")
        log.info(f"   Setup size : {setup_size:.1f} pts")
        log.info(f"   Risk/lot   : Rs.{sl_dist * LOT_SIZE:,.0f}")
        log.info("=" * 62)

        s.disarm("buy entry taken")
        self._place_order(
            candle=candle,
            direction="BUY",
            entry=close,
            sl_trigger=sl_trigger,
            target=target,
            candle_idx=candle_idx,
            is_add=False,
            pivot_price_used=sl["price"],
            pivot_idx=sl["idx"],
            pivot_dt=sl["dt"],
            pivot_type="low",
            setup_size=setup_size,
            close=close,
            vwap=vwap
        )
        return "BUY_ENTRY"

    # -------------------------------------------------------------------------
    #  SCALING
    # -------------------------------------------------------------------------
    def _maybe_add(self, candle, candle_idx, close, pivots, vwap):
        s = self.state
        current_lots = s.total_qty // LOT_SIZE if LOT_SIZE else 0
        if current_lots >= MAX_TOTAL_LOTS:
            return

        origin_price = s.target_price
        origin_idx   = s.entry_candle if s.entry_candle is not None else candle_idx

        if s.direction == "SELL":
            sh = s.sh_registry.next_entry_level(
                close=close,
                origin_idx=origin_idx,
                origin_price=origin_price,
                last_used_price=s.last_added_price + 0.0001,
                used_prices=s.used_prices_in_trade
            )
            if sh is None:
                return
            new_sl = sh["price"] + SL_BUFFER
            log.info(f"[ADD-SELL] SH {sh['price']:.2f} reached -> add {ADD_LOTS} lot(s) at {close:.2f} new SL:{new_sl:.2f}")
            self._place_order(
                candle=candle, direction="SELL", entry=close,
                sl_trigger=new_sl, target=s.target_price,
                candle_idx=candle_idx, is_add=True,
                pivot_price_used=sh["price"], pivot_idx=sh["idx"],
                pivot_dt=sh["dt"], pivot_type="high",
                setup_size=0.0, close=close, vwap=vwap
            )

        elif s.direction == "BUY":
            sl = s.sl_registry.next_entry_level(
                close=close,
                origin_idx=origin_idx,
                origin_price=origin_price,
                last_used_price=s.last_added_price - 0.0001,
                used_prices=s.used_prices_in_trade
            )
            if sl is None:
                return
            new_sl = sl["price"] - SL_BUFFER
            log.info(f"[ADD-BUY] SL {sl['price']:.2f} reached -> add {ADD_LOTS} lot(s) at {close:.2f} new SL:{new_sl:.2f}")
            self._place_order(
                candle=candle, direction="BUY", entry=close,
                sl_trigger=new_sl, target=s.target_price,
                candle_idx=candle_idx, is_add=True,
                pivot_price_used=sl["price"], pivot_idx=sl["idx"],
                pivot_dt=sl["dt"], pivot_type="low",
                setup_size=0.0, close=close, vwap=vwap
            )

    # -------------------------------------------------------------------------
    #  EXIT CHECK
    # -------------------------------------------------------------------------
    def _check_exit(self, candle, candle_idx: int, close: float, vwap: float) -> str:
        s = self.state

        if s.direction == "SELL":
            if close >= s.sl_trigger:
                log.warning(f"[SL] SELL [{candle['dt'].strftime('%H:%M')}] close {close:.2f} >= SL {s.sl_trigger:.2f}")
                self._exit(candle["dt"], close, "SL", close=close, vwap=vwap, candle_idx=candle_idx)
                return "SL_EXIT"
            if close <= s.target_price:
                log.info(f"[TARGET] SELL [{candle['dt'].strftime('%H:%M')}] close {close:.2f} <= T {s.target_price:.2f}")
                self._exit(candle["dt"], close, "TARGET", close=close, vwap=vwap, candle_idx=candle_idx)
                return "TARGET_EXIT"

        elif s.direction == "BUY":
            if close <= s.sl_trigger:
                log.warning(f"[SL] BUY [{candle['dt'].strftime('%H:%M')}] close {close:.2f} <= SL {s.sl_trigger:.2f}")
                self._exit(candle["dt"], close, "SL", close=close, vwap=vwap, candle_idx=candle_idx)
                return "SL_EXIT"
            if close >= s.target_price:
                log.info(f"[TARGET] BUY [{candle['dt'].strftime('%H:%M')}] close {close:.2f} >= T {s.target_price:.2f}")
                self._exit(candle["dt"], close, "TARGET", close=close, vwap=vwap, candle_idx=candle_idx)
                return "TARGET_EXIT"

        return "HOLDING"

    def check_tick_exit(self, price: float):
        s = self.state
        if s.status != TradeState.OPEN:
            return

        now_dt = datetime.datetime.now(IST).replace(tzinfo=None)

        if s.day_bias == "BEARISH" and s.orb_high and price > s.orb_high:
            log.warning(f"[INVALIDATION-TICK] ORB HIGH {price:.2f} > {s.orb_high:.2f}")
            self._exit(now_dt, price, "ORB_HIGH_BREAK_ALGO_DEAD", close=price, vwap=None, candle_idx=None)
            s.algo_dead = True
            return

        if s.day_bias == "BULLISH" and s.orb_low and price < s.orb_low:
            log.warning(f"[INVALIDATION-TICK] ORB LOW {price:.2f} < {s.orb_low:.2f}")
            self._exit(now_dt, price, "ORB_LOW_BREAK_ALGO_DEAD", close=price, vwap=None, candle_idx=None)
            s.algo_dead = True
            return

        if s.direction == "SELL" and price <= s.target_price:
            self._exit(now_dt, price, "TARGET_TICK", close=price, vwap=None, candle_idx=None)
        elif s.direction == "BUY" and price >= s.target_price:
            self._exit(now_dt, price, "TARGET_TICK", close=price, vwap=None, candle_idx=None)

    def square_off(self, price: float, reason="SQUARE_OFF"):
        if self.state.status == TradeState.OPEN:
            log.info(f"[SQOFF] Forced square-off at {price:.2f}")
            self._exit(datetime.datetime.now(IST).replace(tzinfo=None), price, reason, close=price, vwap=None, candle_idx=None)
        self.state.disarm("square-off time")

    # -------------------------------------------------------------------------
    #  ORDER PLACEMENT
    # -------------------------------------------------------------------------
    def _place_order(self, candle, direction: str, entry: float,
                     sl_trigger: float, target: float,
                     candle_idx: int, is_add: bool,
                     pivot_price_used: float, pivot_idx: int, pivot_dt: datetime.datetime,
                     pivot_type: str, setup_size: float,
                     close: float, vwap: float):

        s = self.state
        qty = (ADD_LOTS * LOT_SIZE) if is_add else (LOTS * LOT_SIZE)

        if not self.paper and self.kite:
            try:
                kite_tx = (self.kite.TRANSACTION_TYPE_SELL if direction == "SELL" else self.kite.TRANSACTION_TYPE_BUY)
                oid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=EXCHANGE_FUT,
                    tradingsymbol=self.symbol,
                    transaction_type=kite_tx,
                    quantity=qty,
                    product=PRODUCT,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                log.info(f"[ORDER] {direction}:{oid} qty:{qty}")
            except Exception as e:
                log.error(f"[ERR] Order failed: {e}")
                return
        else:
            log.info(f"[PAPER] {'ADD' if is_add else 'ENTRY'} {direction} qty:{qty}")

        if s.status != TradeState.OPEN:
            s.status          = TradeState.OPEN
            s.direction       = direction
            s.total_qty       = qty
            s.avg_entry_price = entry
            s.entry_time      = candle["dt"]
            s.entry_candle    = candle_idx
            s.target_price    = target
            s.sl_trigger      = sl_trigger
            s.trades_today   += 1

            s.used_prices_in_trade.add(pivot_price_used)
            s.last_added_price = pivot_price_used
            s.vwap_breach_count = 0
            s.last_vwap_side = None

            if s.trade_no <= 0:
                max_no, pnl_closed = db_today_stats(s.trade_date)
                s.trade_no = max_no + 1
                s.pnl_today = pnl_closed

            payload = {
                "trade_date": s.trade_date, "trade_no": s.trade_no,
                "symbol": s.symbol, "mode": s.mode,
                "bias": s.day_bias, "direction": s.direction,
                "entry_time": s.entry_time, "entry_price": s.avg_entry_price,
                "entry_close": close, "entry_vwap": vwap,
                "entry_qty": s.total_qty,
                "entry_lots": s.total_qty // LOT_SIZE if LOT_SIZE else 0,
                "orb_high": s.orb_high, "orb_low": s.orb_low,
                "origin_type": s.origin_type, "origin_price": s.origin_price,
                "origin_idx": s.origin_idx, "origin_dt": s.origin_dt,
                "trigger_pivot_type": pivot_type,
                "trigger_pivot_price": pivot_price_used,
                "trigger_pivot_idx": pivot_idx, "trigger_pivot_dt": pivot_dt,
                "setup_size": setup_size, "sl_price": s.sl_trigger,
                "target_price": s.target_price,
                "risk_per_lot": abs(s.sl_trigger - close) * LOT_SIZE,
            }

            db_trade_open_insert(s.symbol, s.mode, s.trade_no, s.trade_date, payload)
            db_event(
                s.trade_date, s.trade_no, s.symbol, "ENTRY", note="ENTRY_TAKEN",
                candle_dt=candle["dt"], candle_idx=candle_idx, close=close, vwap=vwap,
                direction=s.direction, qty=s.total_qty, avg_entry=s.avg_entry_price,
                sl=s.sl_trigger, target=s.target_price,
                pivot_type=pivot_type, pivot_price=pivot_price_used,
                pivot_idx=pivot_idx, pivot_dt=pivot_dt,
                origin_type=s.origin_type, origin_price=s.origin_price,
                origin_idx=s.origin_idx, origin_dt=s.origin_dt,
                orb_high=s.orb_high, orb_low=s.orb_low,
                extra={"setup_size": setup_size, "risk_per_lot": abs(s.sl_trigger - close) * LOT_SIZE}
            )
        else:
            if s.avg_entry_price is None or s.total_qty == 0:
                log.warning("[FIX] avg_entry_price was None during ADD — correcting")
                s.avg_entry_price = entry
                s.total_qty = qty
            else:
                new_total = s.total_qty + qty
                s.avg_entry_price = ((s.avg_entry_price * s.total_qty + entry * qty) / new_total)
                s.total_qty = new_total

            if direction == "SELL":
                s.sl_trigger       = max(s.sl_trigger, sl_trigger)
                s.last_added_price = max(s.last_added_price, pivot_price_used)
            else:
                s.sl_trigger       = min(s.sl_trigger, sl_trigger)
                s.last_added_price = min(s.last_added_price, pivot_price_used)

            s.used_prices_in_trade.add(pivot_price_used)
            db_event(
                s.trade_date, s.trade_no, s.symbol, "ADD", note="SCALE_IN",
                candle_dt=candle["dt"], candle_idx=candle_idx, close=close, vwap=vwap,
                direction=s.direction, qty=s.total_qty, avg_entry=s.avg_entry_price,
                sl=s.sl_trigger, target=s.target_price,
                pivot_type=pivot_type, pivot_price=pivot_price_used,
                pivot_idx=pivot_idx, pivot_dt=pivot_dt,
                origin_type=s.origin_type, origin_price=s.origin_price,
                origin_idx=s.origin_idx, origin_dt=s.origin_dt,
                orb_high=s.orb_high, orb_low=s.orb_low,
                extra={"added_qty": qty, "new_total_qty": s.total_qty}
            )

        log.info(f"[POS] {s.direction} qty:{s.total_qty} avg:{s.avg_entry_price:.2f} SL:{s.sl_trigger:.2f} T:{s.target_price:.2f}")

    # -------------------------------------------------------------------------
    #  EXIT
    # -------------------------------------------------------------------------
    def _exit(self, dt, price: float, reason: str, close: float, vwap: float, candle_idx):
        s = self.state
        qty = s.total_qty
        if qty <= 0:
            s.reset_trade()
            return

        if not self.paper and self.kite:
            try:
                kite_tx = (self.kite.TRANSACTION_TYPE_BUY if s.direction == "SELL" else self.kite.TRANSACTION_TYPE_SELL)
                xid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=EXCHANGE_FUT,
                    tradingsymbol=self.symbol,
                    transaction_type=kite_tx,
                    quantity=qty,
                    product=PRODUCT,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                log.info(f"[ORDER] exit:{xid} qty:{qty}")
            except Exception as e:
                log.error(f"[ERR] Exit order failed: {e} — EXIT MANUALLY")

        if s.direction == "SELL":
            pnl     = (s.avg_entry_price - price) * qty
            pnl_pts = (s.avg_entry_price - price)
        else:
            pnl     = (price - s.avg_entry_price) * qty
            pnl_pts = (price - s.avg_entry_price)

        s.pnl_today += pnl
        dt = dt.replace(tzinfo=None) if getattr(dt, "tzinfo", None) else dt

        db_trade_close_update(s.trade_date, s.trade_no, {
            "exit_time": dt, "exit_price": price,
            "exit_close": close, "exit_vwap": vwap,
            "exit_reason": reason, "pnl_points": pnl_pts,
            "pnl_rs": pnl, "day_pnl_rs": s.pnl_today
        })
        db_event(
            s.trade_date, s.trade_no, s.symbol, "EXIT", note=reason,
            candle_dt=dt, candle_idx=candle_idx, close=close, vwap=vwap,
            direction=s.direction, qty=qty, avg_entry=s.avg_entry_price,
            sl=s.sl_trigger, target=s.target_price,
            origin_type=s.origin_type, origin_price=s.origin_price,
            origin_idx=s.origin_idx, origin_dt=s.origin_dt,
            orb_high=s.orb_high, orb_low=s.orb_low,
            extra={"pnl_rs": pnl, "pnl_points": pnl_pts, "day_pnl_rs": s.pnl_today}
        )

        log.info("=" * 62)
        log.info(f"[EXIT] {s.direction} reason={reason} bias={s.day_bias}")
        log.info(f"   AvgEntry {s.avg_entry_price:.2f} -> Exit {price:.2f}")
        log.info(f"   Qty {qty} TradePnL Rs.{pnl:+,.0f} DayPnL Rs.{s.pnl_today:+,.0f}")
        log.info("=" * 62)

        s.reset_trade()

# =============================================================================
#  KITE DATA FETCH  ← FIX-1: guard against pre-market start
# =============================================================================

def fetch_candles(kite, instrument_token, live=False):
    now     = datetime.datetime.now(IST)
    from_dt = now.replace(hour=9, minute=15, second=0, microsecond=0, tzinfo=IST)
    to_dt   = now.replace(tzinfo=IST)

    # ✅ FIX-1: If bot starts before market opens, from_dt > to_dt → Kite throws
    #           "from date cannot be after to date" which silently kills preload.
    if to_dt <= from_dt:
        log.info(f"[PRELOAD] Pre-market ({now.strftime('%H:%M')} IST) — no candles to fetch yet.")
        return []

    records = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt, to_date=to_dt,
        interval=INTERVAL, continuous=False, oi=False
    )

    out = []
    for r in records:
        dt = r["date"] if isinstance(r["date"], datetime.datetime) else \
             datetime.datetime.fromisoformat(str(r["date"]))
        dt = dt.replace(tzinfo=None, second=0, microsecond=0)
        if datetime.time(9, 15) <= dt.time() <= datetime.time(15, 30):
            out.append({
                "dt": dt, "open": r["open"], "high": r["high"],
                "low": r["low"], "close": r["close"],
                "volume": r.get("volume", 0)
            })
    return out

# =============================================================================
#  LIVE BOT
# =============================================================================

class LiveBot:
    def __init__(self, kite):
        self.kite    = kite
        self.builder = CandleBuilder()
        self.state   = TradeState()
        self._lock   = threading.Lock()

        self.instrument_token, self.tradingsymbol, self.expiry = \
            get_current_month_nifty_future(kite)
        self.state.symbol = self.tradingsymbol
        self.strategy = StrategyCore(self.state, paper=PAPER_TRADE,
                                     kite=kite, symbol=self.tradingsymbol)

        self._ks_last_check    = 0.0
        self._ks_cached_allowed = True
        self._ks_every_s       = 5.0

        self._restore_from_db()
        self._preload_and_replay()

        self.ticker = KiteTicker(API_KEY, ACCESS_TOKEN)
        self.ticker.on_connect = self._on_connect
        self.ticker.on_ticks   = self._on_ticks
        self.ticker.on_close   = lambda ws, c, r: log.warning(f"WS closed: {r}")
        self.ticker.on_error   = lambda ws, c, r: log.error(f"WS error: {r}")

    def _restore_from_db(self):
        today = datetime.datetime.now(IST).date()
        self.state.trade_date = today

        max_no, pnl_closed = db_today_stats(today)
        self.state.trade_no     = max_no
        self.state.pnl_today    = pnl_closed
        self.state.trades_today = max_no

        open_trade = db_load_open_trade(today)
        if open_trade:
            self.state.status         = TradeState.OPEN
            self.state.direction      = open_trade["direction"]
            self.state.avg_entry_price = float(open_trade["entry_price"])
            self.state.total_qty      = int(open_trade["entry_qty"] or 0)
            self.state.sl_trigger     = float(open_trade["sl_price"])
            self.state.target_price   = float(open_trade["target_price"])
            self.state.entry_time     = open_trade["entry_time"]
            self.state.day_bias       = open_trade.get("bias") or "NONE"
            self.state.orb_high       = open_trade.get("orb_high")
            self.state.orb_low        = open_trade.get("orb_low")
            self.state.origin_type    = open_trade.get("origin_type")
            self.state.origin_price   = open_trade.get("origin_price")
            self.state.origin_idx     = open_trade.get("origin_idx")
            self.state.origin_dt      = open_trade.get("origin_dt")
            self.state.trade_no       = int(open_trade["trade_no"])
            log.warning("=" * 62)
            log.warning("[RESTORE] Found OPEN trade in DB -> restored into memory")
            log.warning(f"  trade_no={self.state.trade_no} dir={self.state.direction} qty={self.state.total_qty}")
            log.warning(f"  avg={self.state.avg_entry_price} sl={self.state.sl_trigger} tgt={self.state.target_price}")
            log.warning("=" * 62)

    def _preload_and_replay(self):
        log.info("=" * 62)
        log.info("[PRELOAD] Fetching today's FUT candles and replaying strategy...")
        log.info("=" * 62)
        try:
            candles = fetch_candles(self.kite, self.instrument_token, live=True)
            if not candles:
                log.warning("[PRELOAD] No candles returned — starting fresh (normal if pre-market)")
                return

            sq_time = datetime.time(*SQUARE_OFF_TIME)
            for c in candles[:-1]:
                self.builder.completed.append(c)

            completed = self.builder.get_all()
            log.info(f"[PRELOAD] Loaded {len(completed)} completed candles")

            log.info("[PRELOAD] Replaying strategy...")
            for i in range(len(completed)):
                if completed[i]["dt"].time() >= sq_time:
                    log.info("[PRELOAD] Square-off time reached during replay — stopping")
                    break
                if i < PIVOT_LEFT + PIVOT_RIGHT + 1:
                    continue
                self.strategy.on_candle_close(completed, i)

            log.info("=" * 62)
            log.info("[PRELOAD] State after replay:")
            log.info(f"   ORB          : H={self.state.orb_high}  L={self.state.orb_low}")
            log.info(f"   Day Bias     : {self.state.day_bias}")
            log.info(f"   Algo Dead    : {self.state.algo_dead}")
            log.info(f"   Armed        : {self.state.armed}")
            log.info(f"   Position     : {self.state.status}")
            if self.state.status == TradeState.OPEN:
                log.info(f"   Direction    : {self.state.direction}")
                log.info(f"   Avg Entry    : {self.state.avg_entry_price}")
                log.info(f"   SL           : {self.state.sl_trigger}")
                log.info(f"   Target       : {self.state.target_price}")
                log.info(f"   Qty          : {self.state.total_qty}")
            log.info(f"   Trades today : {self.state.trades_today}")
            log.info(f"   PnL today    : Rs.{self.state.pnl_today:+,.0f}")
            log.info("=" * 62)

        except Exception as e:
            log.warning(f"[PRELOAD] Failed: {e} — starting with empty state")

    def _kill_switch_allowed(self) -> bool:
        now = time.time()
        if now - self._ks_last_check >= self._ks_every_s:
            self._ks_last_check = now
            try:
                self._ks_cached_allowed = db_kill_switch_read()
            except Exception as e:
                log.warning(f"[KILL] DB read failed, default allow=True: {e}")
                self._ks_cached_allowed = True
        return self._ks_cached_allowed

    def _on_connect(self, ws, _):
        log.info("WebSocket connected")
        ws.subscribe([self.instrument_token])
        ws.set_mode(ws.MODE_LTP, [self.instrument_token])

    def _on_ticks(self, ws, ticks):
        for t in ticks:
            if t["instrument_token"] != self.instrument_token:
                continue
            with self._lock:
                self._process(t["last_price"], datetime.datetime.now(IST))

    def _process(self, price, ts):
        if not self._kill_switch_allowed():
            if self.state.status == TradeState.OPEN:
                self.strategy.square_off(price, reason="KILL_SWITCH")
                self.state.algo_dead = True
            return

        if ts.time() >= datetime.time(*SQUARE_OFF_TIME):
            if self.state.status == TradeState.OPEN:
                self.strategy.square_off(price, reason="SQUARE_OFF")
            return
        if ts.time() < datetime.time(9, 15):
            return

        closed = self.builder.on_tick(price, ts)
        if closed:
            candles = self.builder.get_all()
            self.strategy.on_candle_close(candles, len(candles) - 1)

        if self.state.status == TradeState.OPEN:
            self.strategy.check_tick_exit(price)

    def run(self):
        log.info("=" * 62)
        log.info(f"[RUN] {self.tradingsymbol} | PAPER={PAPER_TRADE} | SquareOff={SQUARE_OFF_TIME[0]:02d}:{SQUARE_OFF_TIME[1]:02d}")
        log.info("=" * 62)

        self.ticker.connect(threaded=True)
        try:
            while True:
                time.sleep(30)
                now = datetime.datetime.now(IST)
                # ✅ FIX-3: Only quit AFTER 15:20 IST (5-min buffer past square-off).
                #    Original used SQUARE_OFF_TIME (15:15) which could cause premature
                #    exit on an intra-day Railway restart where status is still IDLE.
                safe_done = datetime.time(SQUARE_OFF_TIME[0],
                                          min(SQUARE_OFF_TIME[1] + 5, 59))
                if now.time() > safe_done and self.state.status == TradeState.IDLE:
                    log.info(f"Session done — trades:{self.state.trades_today} PnL:Rs.{self.state.pnl_today:+,.0f}")
                    break
        finally:
            try:
                self.ticker.close()
            except Exception:
                pass

# =============================================================================
#  MAIN  ← FIX-2: pre-market wait loop
# =============================================================================

def main():
    # ✅ FIX-2: If Railway starts the container before market hours (common when
    #    deploying at night or after a crash restart), spin here instead of
    #    crashing inside fetch_candles or creating zombie state.
    #    We wake up at 09:13 IST — 2 minutes before market open — so the bot
    #    is fully initialised and WebSocket connected by 09:15.
    WAKE_TIME = datetime.time(9, 13)
    while True:
        now = datetime.datetime.now(IST)
        if now.time() >= WAKE_TIME:
            break
        wait_secs = (
            datetime.datetime.combine(now.date(), WAKE_TIME, tzinfo=IST) - now
        ).seconds
        log.info(f"[STARTUP] Pre-market — sleeping {wait_secs}s until 09:13 IST "
                 f"(now {now.strftime('%H:%M:%S')} IST)")
        time.sleep(min(wait_secs, 60))

    db_init()

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    bot = LiveBot(kite)
    bot.run()

if __name__ == "__main__":
    main()
