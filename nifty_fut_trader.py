#!/usr/bin/env python3
"""
NIFTY FUT Automated Swing Trade Bot  —  ORB Breakout (Bullish + Bearish) with Scaling
=====================================================================================
Railway.app deployment version
- No CSV, No Flask, No Plotly
- Live/Paper trades  -> trades, swing_pivots, orb_snapshots, session_summaries
- Backtest/Replay    -> backtest_trades, backtest_swing_pivots,
                        backtest_orb_snapshots, backtest_session_summaries
- Config via environment variables
- Set FORCE_REPLAY=true in Railway Variables to trigger a backtest run
"""

import sys, time, datetime, threading, logging, os, argparse
from collections import defaultdict as _dd

try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        print("Run:  pip install backports.zoneinfo"); sys.exit(1)

missing = []
try:    from kiteconnect import KiteConnect, KiteTicker
except: missing.append('kiteconnect')
try:    import pandas as pd
except: missing.append('pandas')

try:
    from sqlalchemy import (
        create_engine, Column, Integer, Float, String,
        DateTime, Boolean, Text, Index, event
    )
    from sqlalchemy.orm import declarative_base, Session
    from sqlalchemy.pool import NullPool
except ImportError:
    missing.append('sqlalchemy')

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv optional; env vars may be set directly in Railway Variables

if missing:
    print(f"\n[ERR]  Run:  pip install {' '.join(missing)}\n"); sys.exit(1)

IST = ZoneInfo("Asia/Kolkata")

# =============================================================================
#  CONFIG  (all overridable via Railway environment variables)
# =============================================================================

def _env(key, default):
    return os.environ.get(key, default)

API_KEY          = _env("KITE_API_KEY",      "YOUR_API_KEY")
ACCESS_TOKEN     = _env("KITE_ACCESS_TOKEN", "YOUR_ACCESS_TOKEN")
DATABASE_URL     = _env("DATABASE_URL",      "sqlite:///trades.db")  # Railway sets automatically

BASE_SYMBOL      = "NIFTY"
EXCHANGE_FUT     = "NFO"
PRODUCT          = "MIS"
INTERVAL         = "minute"

ORB_MINUTES      = int(_env("ORB_MINUTES",       "15"))
PIVOT_LEFT       = int(_env("PIVOT_LEFT",         "2"))
PIVOT_RIGHT      = int(_env("PIVOT_RIGHT",        "2"))
MIN_PIVOT_DIST   = int(_env("MIN_PIVOT_DIST",     "15"))
BREAK_BUFFER     = int(_env("BREAK_BUFFER",       "5"))
RETEST_BUFFER    = int(_env("RETEST_BUFFER",      "5"))
MIN_SETUP_POINTS = int(_env("MIN_SETUP_POINTS",   "15"))
SL_BUFFER        = int(_env("SL_BUFFER",          "10"))

VWAP_SL_POINTS     = int(_env("VWAP_SL_POINTS",     "10"))
VWAP_CONSEC_CLOSES = int(_env("VWAP_CONSEC_CLOSES",  "3"))

LOTS           = int(_env("LOTS",           "1"))
LOT_SIZE       = int(_env("LOT_SIZE",       "50"))
ADD_LOTS       = int(_env("ADD_LOTS",       "1"))
MAX_TOTAL_LOTS = int(_env("MAX_TOTAL_LOTS", "6"))
MAX_SL_POINTS  = int(_env("MAX_SL_POINTS",  "300"))
MAX_TRADES_DAY = int(_env("MAX_TRADES_DAY", "10"))

SQUARE_OFF_TIME = (int(_env("SQUARE_OFF_HOUR", "15")),
                   int(_env("SQUARE_OFF_MIN",  "15")))

PAPER_TRADE  = _env("PAPER_TRADE",  "true").lower() == "true"

# ── Backtest controls ──────────────────────────────────────────────────────────
# Toggle FORCE_REPLAY=true / false in Railway Variables — no redeploy needed.
FORCE_REPLAY   = _env("FORCE_REPLAY", "false").lower() == "true"
FROM_DATE      = _env("FROM_DATE", "2026-02-12")
TO_DATE        = _env("TO_DATE",   "2026-02-12")
REPLAY_DELAY_S = float(_env("REPLAY_DELAY_S", "0.0"))   # 0 = run as fast as possible

LOG_FILE = _env("LOG_FILE", "trader.log")

# =============================================================================
#  LOGGING
# =============================================================================

import io
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

handlers = [logging.StreamHandler(sys.stdout)]
if LOG_FILE:
    try:
        handlers.append(logging.FileHandler(LOG_FILE, encoding='utf-8'))
    except Exception:
        pass

logging.basicConfig(
    level   = logging.INFO,
    format  = '%(asctime)s IST  %(levelname)-8s  %(message)s',
    datefmt = '%H:%M:%S',
    handlers= handlers
)
log = logging.getLogger(__name__)

# =============================================================================
#  DATABASE MODELS
#
#  Live / Paper  ->  trades, swing_pivots, orb_snapshots, session_summaries
#  Backtest      ->  backtest_trades, backtest_swing_pivots,
#                    backtest_orb_snapshots, backtest_session_summaries
#
#  Schema is identical in both sets. The DB class routes writes to the correct
#  set based on the is_backtest flag so the two datasets are never mixed.
# =============================================================================

Base = declarative_base()

# ── LIVE / PAPER tables ───────────────────────────────────────────────────────

class Trade(Base):
    __tablename__ = 'trades'

    id           = Column(Integer, primary_key=True, autoincrement=True)
    trade_num    = Column(Integer,    nullable=False)
    session_date = Column(String(10), nullable=False, index=True)
    mode         = Column(String(10), nullable=False)              # PAPER / LIVE

    direction    = Column(String(4),  nullable=False)              # BUY / SELL
    bias         = Column(String(10), nullable=False)              # BULLISH / BEARISH

    entry_time   = Column(DateTime, nullable=False)
    entry_price  = Column(Float,    nullable=False)
    exit_time    = Column(DateTime, nullable=True)
    exit_price   = Column(Float,    nullable=True)

    sl_trigger   = Column(Float,    nullable=False)
    target       = Column(Float,    nullable=False)
    setup_size   = Column(Float,    nullable=True)

    qty          = Column(Integer,  nullable=False)
    lots         = Column(Integer,  nullable=False)
    pnl_points   = Column(Float,    nullable=True)
    pnl_rs       = Column(Float,    nullable=True)

    reason       = Column(String(50), nullable=True)
    day_pnl_rs   = Column(Float,    nullable=True)

    symbol       = Column(String(30), nullable=True)
    expiry       = Column(String(12), nullable=True)
    orb_high     = Column(Float,    nullable=True)
    orb_low      = Column(Float,    nullable=True)
    created_at   = Column(DateTime, default=datetime.datetime.utcnow)


class SwingPivot(Base):
    __tablename__ = 'swing_pivots'

    id           = Column(Integer, primary_key=True, autoincrement=True)
    session_date = Column(String(10), nullable=False, index=True)
    pivot_type   = Column(String(4),  nullable=False)   # HIGH / LOW
    candle_time  = Column(DateTime,   nullable=False)
    candle_idx   = Column(Integer,    nullable=False)
    price        = Column(Float,      nullable=False)
    symbol       = Column(String(30), nullable=True)
    created_at   = Column(DateTime,   default=datetime.datetime.utcnow)


class OrbSnapshot(Base):
    __tablename__ = 'orb_snapshots'

    id           = Column(Integer, primary_key=True, autoincrement=True)
    session_date = Column(String(10), nullable=False, index=True)
    symbol       = Column(String(30), nullable=True)
    orb_high     = Column(Float,      nullable=False)
    orb_low      = Column(Float,      nullable=False)
    orb_open     = Column(Float,      nullable=False)
    formed_at    = Column(DateTime,   nullable=False)
    created_at   = Column(DateTime,   default=datetime.datetime.utcnow)


class SessionSummary(Base):
    __tablename__ = 'session_summaries'

    id             = Column(Integer, primary_key=True, autoincrement=True)
    session_date   = Column(String(10), nullable=False, index=True)
    symbol         = Column(String(30), nullable=True)
    mode           = Column(String(10), nullable=False)
    day_bias       = Column(String(10), nullable=True)
    total_trades   = Column(Integer,    nullable=False, default=0)
    winning_trades = Column(Integer,    nullable=False, default=0)
    losing_trades  = Column(Integer,    nullable=False, default=0)
    total_pnl_rs   = Column(Float,      nullable=False, default=0.0)
    algo_dead      = Column(Boolean,    nullable=False, default=False)
    created_at     = Column(DateTime,   default=datetime.datetime.utcnow)
    updated_at     = Column(DateTime,   default=datetime.datetime.utcnow,
                            onupdate=datetime.datetime.utcnow)


# ── BACKTEST tables (identical columns + from_date / to_date metadata) ────────

class BacktestTrade(Base):
    __tablename__ = 'backtest_trades'

    id           = Column(Integer, primary_key=True, autoincrement=True)
    trade_num    = Column(Integer,    nullable=False)
    session_date = Column(String(10), nullable=False, index=True)
    mode         = Column(String(10), nullable=False)              # REPLAY

    direction    = Column(String(4),  nullable=False)
    bias         = Column(String(10), nullable=False)

    entry_time   = Column(DateTime, nullable=False)
    entry_price  = Column(Float,    nullable=False)
    exit_time    = Column(DateTime, nullable=True)
    exit_price   = Column(Float,    nullable=True)

    sl_trigger   = Column(Float,    nullable=False)
    target       = Column(Float,    nullable=False)
    setup_size   = Column(Float,    nullable=True)

    qty          = Column(Integer,  nullable=False)
    lots         = Column(Integer,  nullable=False)
    pnl_points   = Column(Float,    nullable=True)
    pnl_rs       = Column(Float,    nullable=True)

    reason       = Column(String(50), nullable=True)
    day_pnl_rs   = Column(Float,    nullable=True)

    symbol       = Column(String(30), nullable=True)
    expiry       = Column(String(12), nullable=True)
    orb_high     = Column(Float,    nullable=True)
    orb_low      = Column(Float,    nullable=True)

    # Backtest run metadata — lets you distinguish multiple runs on same dates
    from_date    = Column(String(10), nullable=True)
    to_date      = Column(String(10), nullable=True)
    created_at   = Column(DateTime,   default=datetime.datetime.utcnow)


class BacktestSwingPivot(Base):
    __tablename__ = 'backtest_swing_pivots'

    id           = Column(Integer, primary_key=True, autoincrement=True)
    session_date = Column(String(10), nullable=False, index=True)
    pivot_type   = Column(String(4),  nullable=False)
    candle_time  = Column(DateTime,   nullable=False)
    candle_idx   = Column(Integer,    nullable=False)
    price        = Column(Float,      nullable=False)
    symbol       = Column(String(30), nullable=True)
    from_date    = Column(String(10), nullable=True)
    to_date      = Column(String(10), nullable=True)
    created_at   = Column(DateTime,   default=datetime.datetime.utcnow)


class BacktestOrbSnapshot(Base):
    __tablename__ = 'backtest_orb_snapshots'

    id           = Column(Integer, primary_key=True, autoincrement=True)
    session_date = Column(String(10), nullable=False, index=True)
    symbol       = Column(String(30), nullable=True)
    orb_high     = Column(Float,      nullable=False)
    orb_low      = Column(Float,      nullable=False)
    orb_open     = Column(Float,      nullable=False)
    formed_at    = Column(DateTime,   nullable=False)
    from_date    = Column(String(10), nullable=True)
    to_date      = Column(String(10), nullable=True)
    created_at   = Column(DateTime,   default=datetime.datetime.utcnow)


class BacktestSessionSummary(Base):
    __tablename__ = 'backtest_session_summaries'

    id             = Column(Integer, primary_key=True, autoincrement=True)
    session_date   = Column(String(10), nullable=False, index=True)
    symbol         = Column(String(30), nullable=True)
    mode           = Column(String(10), nullable=False)
    day_bias       = Column(String(10), nullable=True)
    total_trades   = Column(Integer,    nullable=False, default=0)
    winning_trades = Column(Integer,    nullable=False, default=0)
    losing_trades  = Column(Integer,    nullable=False, default=0)
    total_pnl_rs   = Column(Float,      nullable=False, default=0.0)
    algo_dead      = Column(Boolean,    nullable=False, default=False)
    from_date      = Column(String(10), nullable=True)
    to_date        = Column(String(10), nullable=True)
    created_at     = Column(DateTime,   default=datetime.datetime.utcnow)
    updated_at     = Column(DateTime,   default=datetime.datetime.utcnow,
                            onupdate=datetime.datetime.utcnow)


# =============================================================================
#  DATABASE MANAGER
# =============================================================================

class DB:
    """
    Routes all writes to either the live tables or the backtest tables
    depending on is_backtest.

        DB(url, is_backtest=False)  ->  trades, swing_pivots, …
        DB(url, is_backtest=True)   ->  backtest_trades, backtest_swing_pivots, …
    """

    def __init__(self, url: str, is_backtest: bool = False):
        self.is_backtest = is_backtest
        connect_args = {}
        if url.startswith("sqlite"):
            connect_args = {"check_same_thread": False}
        self.engine = create_engine(
            url,
            poolclass    = NullPool,
            connect_args = connect_args,
            echo         = False
        )
        Base.metadata.create_all(self.engine)
        label = "BACKTEST tables" if is_backtest else "LIVE tables"
        log.info(f"[DB] Connected — {label} ready  ({url[:55]}...)")

    # ── Model routing helpers ─────────────────────────────────────────────────

    def _trade_cls(self):   return BacktestTrade          if self.is_backtest else Trade
    def _pivot_cls(self):   return BacktestSwingPivot     if self.is_backtest else SwingPivot
    def _orb_cls(self):     return BacktestOrbSnapshot    if self.is_backtest else OrbSnapshot
    def _summary_cls(self): return BacktestSessionSummary if self.is_backtest else SessionSummary

    def _extra(self) -> dict:
        """Extra columns that only exist on backtest models."""
        return {'from_date': FROM_DATE, 'to_date': TO_DATE} if self.is_backtest else {}

    # ── Trades ────────────────────────────────────────────────────────────────

    def log_trade(self, trade_num: int, t: dict, day_pnl: float,
                  mode: str, symbol: str, expiry, session_date: str,
                  orb_high: float, orb_low: float):

        pnl_pts  = (round(t['entry_price'] - t['exit_price'], 2)
                    if t['direction'] == 'SELL'
                    else round(t['exit_price'] - t['entry_price'], 2))
        exit_dt  = t['exit_dt']
        entry_dt = t['entry_dt']
        if hasattr(exit_dt,  'tzinfo') and exit_dt.tzinfo:
            exit_dt  = exit_dt.replace(tzinfo=None)
        if hasattr(entry_dt, 'tzinfo') and entry_dt.tzinfo:
            entry_dt = entry_dt.replace(tzinfo=None)

        cls    = self._trade_cls()
        kwargs = dict(
            trade_num    = trade_num,
            session_date = session_date,
            mode         = mode,
            direction    = t['direction'],
            bias         = t.get('bias', ''),
            entry_time   = entry_dt,
            entry_price  = round(t['entry_price'], 2),
            exit_time    = exit_dt,
            exit_price   = round(t['exit_price'],  2),
            sl_trigger   = round(t['sl_trigger'],  2),
            target       = round(t['target'],      2),
            setup_size   = round(t.get('setup_size', 0), 2),
            qty          = t.get('qty', 0),
            lots         = t.get('qty', 0) // LOT_SIZE if LOT_SIZE else 0,
            pnl_points   = pnl_pts,
            pnl_rs       = round(t['pnl'],   2),
            reason       = t['reason'],
            day_pnl_rs   = round(day_pnl,    2),
            symbol       = symbol,
            expiry       = str(expiry) if expiry else None,
            orb_high     = round(orb_high, 2) if orb_high else None,
            orb_low      = round(orb_low,  2) if orb_low  else None,
            **self._extra()
        )
        with Session(self.engine) as s:
            s.add(cls(**kwargs))
            s.commit()
        tag = '[BT]' if self.is_backtest else '[DB]'
        log.info(f"{tag} Trade #{trade_num} -> {cls.__tablename__}  "
                 f"PnL Rs.{kwargs['pnl_rs']:+,.0f}")

    # ── Swing Pivots ──────────────────────────────────────────────────────────

    def log_pivot(self, pivot_type: str, candle_dt: datetime.datetime,
                  candle_idx: int, price: float,
                  symbol: str, session_date: str):
        cls = self._pivot_cls()
        dup_filter = dict(session_date=session_date,
                          pivot_type=pivot_type,
                          candle_idx=candle_idx)
        if self.is_backtest:
            dup_filter['from_date'] = FROM_DATE

        with Session(self.engine) as s:
            if not s.query(cls).filter_by(**dup_filter).first():
                ct = candle_dt.replace(tzinfo=None) if getattr(candle_dt, 'tzinfo', None) else candle_dt
                s.add(cls(
                    session_date = session_date,
                    pivot_type   = pivot_type,
                    candle_time  = ct,
                    candle_idx   = candle_idx,
                    price        = round(price, 2),
                    symbol       = symbol,
                    **self._extra()
                ))
                s.commit()

    # ── ORB ───────────────────────────────────────────────────────────────────

    def log_orb(self, session_date: str, symbol: str,
                orb_high: float, orb_low: float, orb_open: float,
                formed_at: datetime.datetime):
        cls        = self._orb_cls()
        dup_filter = dict(session_date=session_date)
        if self.is_backtest:
            dup_filter.update(from_date=FROM_DATE, to_date=TO_DATE)

        with Session(self.engine) as s:
            if not s.query(cls).filter_by(**dup_filter).first():
                fa = formed_at.replace(tzinfo=None) if getattr(formed_at, 'tzinfo', None) else formed_at
                s.add(cls(
                    session_date = session_date,
                    symbol       = symbol,
                    orb_high     = round(orb_high, 2),
                    orb_low      = round(orb_low,  2),
                    orb_open     = round(orb_open, 2),
                    formed_at    = fa,
                    **self._extra()
                ))
                s.commit()
                tag = '[BT]' if self.is_backtest else '[DB]'
                log.info(f"{tag} ORB -> {cls.__tablename__}  "
                         f"H:{orb_high:.2f}  L:{orb_low:.2f}")

    # ── Session Summary ───────────────────────────────────────────────────────

    def upsert_session_summary(self, session_date: str, symbol: str, mode: str,
                                day_bias: str, total_trades: int,
                                total_pnl: float, algo_dead: bool,
                                trade_log: list):
        wins   = sum(1 for t in trade_log if t['pnl'] >= 0)
        losses = sum(1 for t in trade_log if t['pnl'] <  0)
        cls    = self._summary_cls()

        dup_filter = dict(session_date=session_date)
        if self.is_backtest:
            dup_filter.update(from_date=FROM_DATE, to_date=TO_DATE)

        with Session(self.engine) as s:
            row = s.query(cls).filter_by(**dup_filter).first()
            if row:
                row.total_trades   = total_trades
                row.winning_trades = wins
                row.losing_trades  = losses
                row.total_pnl_rs   = round(total_pnl, 2)
                row.algo_dead      = algo_dead
                row.day_bias       = day_bias
                row.updated_at     = datetime.datetime.utcnow()
            else:
                s.add(cls(
                    session_date   = session_date,
                    symbol         = symbol,
                    mode           = mode,
                    day_bias       = day_bias,
                    total_trades   = total_trades,
                    winning_trades = wins,
                    losing_trades  = losses,
                    total_pnl_rs   = round(total_pnl, 2),
                    algo_dead      = algo_dead,
                    **self._extra()
                ))
            s.commit()

        tag = '[BT]' if self.is_backtest else '[DB]'
        log.info(f"{tag} Session summary -> {cls.__tablename__}  "
                 f"trades:{total_trades}  PnL:Rs.{total_pnl:+,.0f}")


# =============================================================================
#  AUTO-DETECT CURRENT MONTH NIFTY FUT
# =============================================================================

_INSTR_CACHE: dict = {"ts": None, "data": None}

def _get_instruments_cached(kite, exchange: str):
    if _INSTR_CACHE["data"] is None:
        log.info(f"[INSTR] Downloading instruments for {exchange}...")
        _INSTR_CACHE["data"] = kite.instruments(exchange)
        _INSTR_CACHE["ts"]   = datetime.datetime.now(IST)
        log.info(f"[INSTR] Loaded {len(_INSTR_CACHE['data'])} rows.")
    return _INSTR_CACHE["data"]


def get_current_month_nifty_future(kite):
    today       = datetime.datetime.now(IST).date()
    instruments = _get_instruments_cached(kite, EXCHANGE_FUT)

    futs = [
        i for i in instruments
        if i.get("segment") == "NFO-FUT"
        and i.get("name")   == BASE_SYMBOL
        and i.get("expiry") is not None
        and i["expiry"]     >= today
    ]
    if not futs:
        raise RuntimeError("No NIFTY FUT contracts found.")

    futs.sort(key=lambda x: x["expiry"])
    c = futs[0]
    log.info("=" * 62)
    log.info(f"[FUT] Selected: {c['tradingsymbol']}  Expiry: {c['expiry']}")
    log.info("=" * 62)
    return int(c["instrument_token"]), str(c["tradingsymbol"]), c["expiry"]


# =============================================================================
#  VWAP
# =============================================================================

def compute_vwap_series(candles: list) -> list:
    if not candles:
        return []
    cum_pv = 0.0
    cum_v  = 0.0
    out    = []
    for c in candles:
        v  = float(c.get('volume', 0) or 0) or 1.0
        tp = (float(c['high']) + float(c['low']) + float(c['close'])) / 3.0
        cum_pv += tp * v
        cum_v  += v
        out.append(cum_pv / cum_v)
    return out


# =============================================================================
#  PIVOT DETECTION
# =============================================================================

def detect_pivots(candles: list, left: int, right: int,
                  min_dist: float = None) -> dict:
    if min_dist is None:
        min_dist = MIN_PIVOT_DIST
    n = len(candles)
    raw_highs, raw_lows = [], []

    for i in range(left, n - right if right > 0 else n):
        hi  = candles[i]['high']
        lo  = candles[i]['low']
        lhi = [candles[j]['high'] for j in range(i - left, i)]
        rhi = [candles[j]['high'] for j in range(i + 1, i + right + 1)]
        llo = [candles[j]['low']  for j in range(i - left, i)]
        rlo = [candles[j]['low']  for j in range(i + 1, i + right + 1)]

        if (hi > max(lhi) if lhi else True) and (hi > max(rhi) if rhi else True):
            raw_highs.append({'dt': candles[i]['dt'], 'price': hi, 'idx': i})
        if (lo < min(llo) if llo else True) and (lo < min(rlo) if rlo else True):
            raw_lows.append({'dt': candles[i]['dt'], 'price': lo, 'idx': i})

    def _filter(pivots, keep='high', opposite=None):
        if not pivots or min_dist <= 0:
            return pivots
        opp_set = {p['idx'] for p in (opposite or [])}
        out = [pivots[0]]
        for p in pivots[1:]:
            last     = out[-1]
            dist     = abs(p['price'] - last['price'])
            has_opp  = any(last['idx'] < oi < p['idx'] for oi in opp_set)
            if dist < min_dist and not has_opp:
                if keep == 'high' and p['price'] > last['price']: out[-1] = p
                elif keep == 'low' and p['price'] < last['price']: out[-1] = p
            else:
                out.append(p)
        return out

    all_highs = _filter(raw_highs, 'high', opposite=raw_lows)
    all_lows  = _filter(raw_lows,  'low',  opposite=raw_highs)
    return {
        'last_high': all_highs[-1] if all_highs else None,
        'last_low':  all_lows[-1]  if all_lows  else None,
        'all_highs': all_highs,
        'all_lows':  all_lows,
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
        if minute == self.current['dt']:
            self.current['high']  = max(self.current['high'],  price)
            self.current['low']   = min(self.current['low'],   price)
            self.current['close'] = price
            return None
        closed = dict(self.current)
        self.completed.append(closed)
        self._open(minute, price)
        return closed

    def _open(self, m, p):
        self.current = {'dt': m, 'open': p, 'high': p, 'low': p, 'close': p, 'volume': 0}

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
        known = {p['idx'] for p in self._pivots}
        for p in all_pivots:
            if p['idx'] not in known:
                self._pivots.append({'price': p['price'], 'idx': p['idx'], 'dt': p['dt']})

    def next_entry_level(self, close: float, origin_idx: int,
                         origin_price: float, last_used_price: float,
                         used_prices: set):
        if self._type == 'high':
            candidates = [
                p for p in self._pivots
                if p['idx'] < origin_idx
                and p['price'] > last_used_price
                and p['price'] >= origin_price + MIN_SETUP_POINTS
                and close >= p['price'] - RETEST_BUFFER
                and p['price'] not in used_prices
            ]
            return min(candidates, key=lambda p: p['price']) if candidates else None
        else:
            candidates = [
                p for p in self._pivots
                if p['idx'] < origin_idx
                and p['price'] < last_used_price
                and p['price'] <= origin_price - MIN_SETUP_POINTS
                and close <= p['price'] + RETEST_BUFFER
                and p['price'] not in used_prices
            ]
            return max(candidates, key=lambda p: p['price']) if candidates else None

    def all_highs(self): return list(self._pivots) if self._type == 'high' else []
    def all_lows(self):  return list(self._pivots) if self._type == 'low'  else []
    def reset(self):     self._pivots = []


# =============================================================================
#  TRADE STATE
# =============================================================================

class TradeState:
    IDLE = 'IDLE'
    OPEN = 'OPEN'

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
        self.trade_log     = []

        self.day_bias  = 'NONE'
        self.algo_dead = False

        self.orb_high  = None
        self.orb_low   = None

        self.armed        = False
        self.origin_price = None
        self.origin_idx   = None
        self.origin_type  = None

        self.sh_registry = PivotRegistry('high')
        self.sl_registry = PivotRegistry('low')

        self._last_skip_price = None

    def arm(self, price: float, idx: int, pivot_type: str):
        self.armed        = True
        self.origin_price = price
        self.origin_idx   = idx
        self.origin_type  = pivot_type
        self._last_skip_price = None
        log.info(f"[ARM] {'swing LOW' if pivot_type=='low' else 'swing HIGH'} "
                 f"armed — origin {price:.2f}  (candle #{idx})")

    def disarm(self, reason: str = ''):
        self.armed = False
        self.origin_price = self.origin_idx = self.origin_type = None
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
#  OPENING RANGE
# =============================================================================

def compute_orb(candles: list):
    if not candles: return None
    day   = candles[-1]['dt'].date()
    end_t = datetime.time(9, 15 + ORB_MINUTES)
    orb_c = [c for c in candles
             if c['dt'].date() == day
             and datetime.time(9, 15) <= c['dt'].time() < end_t]
    if not orb_c: return None
    return {
        'high':   max(c['high'] for c in orb_c),
        'low':    min(orb_c[0]['open'], min(c['low'] for c in orb_c)),
        'open':   orb_c[0]['open'],
        'formed': len(orb_c) >= ORB_MINUTES,
        'day':    day,
    }


# =============================================================================
#  STRATEGY CORE
# =============================================================================

class StrategyCore:
    def __init__(self, state: TradeState, paper: bool = True, kite=None,
                 symbol: str = "NIFTYFUT", expiry=None, db: DB = None,
                 session_date: str = None):
        self.state        = state
        self.paper        = paper
        self.kite         = kite
        self.symbol       = symbol
        self.expiry       = expiry
        self.db           = db
        self.session_date = session_date or datetime.datetime.now(IST).strftime('%Y-%m-%d')
        self._logged_pivot_indices: set = set()

    def _persist_pivots(self, pivots: dict):
        if not self.db:
            return
        for p in (pivots.get('all_highs') or []):
            key = ('HIGH', p['idx'])
            if key not in self._logged_pivot_indices:
                self.db.log_pivot('HIGH', p['dt'], p['idx'], p['price'],
                                  self.symbol, self.session_date)
                self._logged_pivot_indices.add(key)
        for p in (pivots.get('all_lows') or []):
            key = ('LOW', p['idx'])
            if key not in self._logged_pivot_indices:
                self.db.log_pivot('LOW', p['dt'], p['idx'], p['price'],
                                  self.symbol, self.session_date)
                self._logged_pivot_indices.add(key)

    def on_candle_close(self, candles: list, candle_idx: int):
        candle = candles[candle_idx]
        state  = self.state
        t      = candle['dt'].time()
        close  = candle['close']

        if candle_idx < PIVOT_LEFT + PIVOT_RIGHT + 1:
            return 'WARMUP'
        if t < datetime.time(9, 15 + ORB_MINUTES):
            return 'ORB_FORMING'

        orb = compute_orb(candles[:candle_idx + 1])
        if orb is None or not orb['formed']:
            return 'ORB_NOT_READY'

        if state.orb_high is None:
            state.orb_high = orb['high']
            state.orb_low  = orb['low']
            log.info(f"[ORB] Formed  H:{state.orb_high:.2f}  L:{state.orb_low:.2f}")
            if self.db:
                self.db.log_orb(
                    session_date = self.session_date,
                    symbol       = self.symbol,
                    orb_high     = state.orb_high,
                    orb_low      = state.orb_low,
                    orb_open     = orb['open'],
                    formed_at    = candle['dt'],
                )

        if state.algo_dead:
            return 'ALGO_DEAD'

        pivots = detect_pivots(candles[:candle_idx + 1], PIVOT_LEFT, PIVOT_RIGHT)
        state.sh_registry.update(pivots['all_highs'])
        state.sl_registry.update(pivots['all_lows'])
        self._persist_pivots(pivots)

        vwap_series = compute_vwap_series(candles[:candle_idx + 1])
        vwap        = vwap_series[-1] if vwap_series else None

        if state.status == TradeState.OPEN:
            r = self._check_invalidation(candle)
            if r: return r
            r = self._check_vwap_stop(candle, close, vwap)
            if r: return r
            self._maybe_add(candle, candle_idx, close, pivots)
            return self._check_exit(candle)

        if state.trades_today >= MAX_TRADES_DAY:
            return 'MAX_TRADES'

        if state.day_bias == 'NONE':
            if close < orb['low'] - BREAK_BUFFER:
                state.day_bias = 'BEARISH'
                log.info(f"[BIAS] BEARISH — ORB low {orb['low']:.2f} broken  (close {close:.2f})")
            elif close > orb['high'] + BREAK_BUFFER:
                state.day_bias = 'BULLISH'
                log.info(f"[BIAS] BULLISH — ORB high {orb['high']:.2f} broken  (close {close:.2f})")
            else:
                return 'WAITING_ORB_BREAK'

        if state.day_bias == 'BEARISH':
            last_low = pivots['last_low']
            if last_low is None: return 'NO_PIVOT_LOW'
            if state.origin_idx != last_low['idx']:
                state.arm(last_low['price'], last_low['idx'], 'low')
                return 'SWING_LOW_ARMED'
            if state.armed:
                return self._check_sell_entry(candle, candle_idx, close)

        elif state.day_bias == 'BULLISH':
            last_high = pivots['last_high']
            if last_high is None: return 'NO_PIVOT_HIGH'
            if state.origin_idx != last_high['idx']:
                state.arm(last_high['price'], last_high['idx'], 'high')
                return 'SWING_HIGH_ARMED'
            if state.armed:
                return self._check_buy_entry(candle, candle_idx, close)

        return 'WAITING_SETUP'

    # ── VWAP STOP ────────────────────────────────────────────────────────────

    def _check_vwap_stop(self, candle, close: float, vwap: float):
        s = self.state
        if vwap is None: return None

        if s.direction == 'BUY':
            threshold = vwap - VWAP_SL_POINTS
            if close < threshold:
                s.vwap_breach_count += 1
                log.info(f"[VWAP] BUY breach {s.vwap_breach_count}/{VWAP_CONSEC_CLOSES}  "
                         f"close {close:.2f} < {threshold:.2f}")
            else:
                s.vwap_breach_count = 0
            if s.vwap_breach_count >= VWAP_CONSEC_CLOSES:
                log.warning("[VWAP_SL] BUY: EXIT + ALGO DEAD")
                self._exit(candle['dt'], close, 'VWAP_SL')
                s.algo_dead = True
                return 'VWAP_SL_EXIT_DEAD'

        elif s.direction == 'SELL':
            threshold = vwap + VWAP_SL_POINTS
            if close > threshold:
                s.vwap_breach_count += 1
                log.info(f"[VWAP] SELL breach {s.vwap_breach_count}/{VWAP_CONSEC_CLOSES}  "
                         f"close {close:.2f} > {threshold:.2f}")
            else:
                s.vwap_breach_count = 0
            if s.vwap_breach_count >= VWAP_CONSEC_CLOSES:
                log.warning("[VWAP_SL] SELL: EXIT + ALGO DEAD")
                self._exit(candle['dt'], close, 'VWAP_SL')
                s.algo_dead = True
                return 'VWAP_SL_EXIT_DEAD'

        return None

    # ── INVALIDATION ─────────────────────────────────────────────────────────

    def _check_invalidation(self, candle):
        state = self.state
        close = candle['close']
        if state.day_bias == 'BEARISH' and state.orb_high and close > state.orb_high:
            log.warning(f"[INVALIDATION] BEARISH: close {close:.2f} > ORB HIGH {state.orb_high:.2f}")
            self._exit(candle['dt'], close, 'ORB_HIGH_BREAK_ALGO_DEAD')
            state.algo_dead = True
            return 'INVALIDATION_DEAD'
        if state.day_bias == 'BULLISH' and state.orb_low and close < state.orb_low:
            log.warning(f"[INVALIDATION] BULLISH: close {close:.2f} < ORB LOW {state.orb_low:.2f}")
            self._exit(candle['dt'], close, 'ORB_LOW_BREAK_ALGO_DEAD')
            state.algo_dead = True
            return 'INVALIDATION_DEAD'
        return None

    # ── ENTRIES ───────────────────────────────────────────────────────────────

    def _check_sell_entry(self, candle, candle_idx, close) -> str:
        state = self.state
        sh    = state.sh_registry.next_entry_level(
            close=close, origin_idx=state.origin_idx,
            origin_price=state.origin_price, last_used_price=0.0, used_prices=set()
        )
        if sh is None: return 'ARMED_WATCHING'

        setup_size = sh['price'] - state.origin_price
        if setup_size < MIN_SETUP_POINTS:
            if state._last_skip_price != sh['price']:
                log.info(f"[SKIP] Setup too small: {setup_size:.1f} pts")
                state._last_skip_price = sh['price']
            return 'SETUP_TOO_SMALL'
        state._last_skip_price = None

        sl_trigger = sh['price'] + SL_BUFFER
        sl_dist    = sl_trigger - close
        if sl_dist <= 0: return 'SL_INVALID'
        if sl_dist > MAX_SL_POINTS: return 'SL_TOO_WIDE'

        log.info("=" * 62)
        log.info(f"[SELL] BEARISH ENTRY  [{candle['dt'].strftime('%H:%M')}]  {self.symbol}")
        log.info(f"   Close:{close:.2f}  SH:{sh['price']:.2f}  "
                 f"SL:{sl_trigger:.2f}  T:{state.origin_price:.2f}  "
                 f"Setup:{setup_size:.1f}pts")
        log.info("=" * 62)

        state.disarm('sell entry taken')
        self._place_order(candle, 'SELL', close, sl_trigger, state.origin_price,
                          candle_idx, is_add=False,
                          pivot_price_used=sh['price'], setup_size=setup_size)
        return 'SELL_ENTRY'

    def _check_buy_entry(self, candle, candle_idx, close) -> str:
        state = self.state
        sl    = state.sl_registry.next_entry_level(
            close=close, origin_idx=state.origin_idx,
            origin_price=state.origin_price,
            last_used_price=float('inf'), used_prices=set()
        )
        if sl is None: return 'ARMED_WATCHING'

        setup_size = state.origin_price - sl['price']
        if setup_size < MIN_SETUP_POINTS:
            if state._last_skip_price != sl['price']:
                log.info(f"[SKIP] Setup too small: {setup_size:.1f} pts")
                state._last_skip_price = sl['price']
            return 'SETUP_TOO_SMALL'
        state._last_skip_price = None

        sl_trigger = sl['price'] - SL_BUFFER
        sl_dist    = close - sl_trigger
        if sl_dist <= 0: return 'SL_INVALID'
        if sl_dist > MAX_SL_POINTS: return 'SL_TOO_WIDE'

        log.info("=" * 62)
        log.info(f"[BUY]  BULLISH ENTRY  [{candle['dt'].strftime('%H:%M')}]  {self.symbol}")
        log.info(f"   Close:{close:.2f}  SL:{sl['price']:.2f}  "
                 f"SL_trig:{sl_trigger:.2f}  T:{state.origin_price:.2f}  "
                 f"Setup:{setup_size:.1f}pts")
        log.info("=" * 62)

        state.disarm('buy entry taken')
        self._place_order(candle, 'BUY', close, sl_trigger, state.origin_price,
                          candle_idx, is_add=False,
                          pivot_price_used=sl['price'], setup_size=setup_size)
        return 'BUY_ENTRY'

    # ── SCALING ───────────────────────────────────────────────────────────────

    def _maybe_add(self, candle, candle_idx, close, pivots):
        state        = self.state
        current_lots = state.total_qty // LOT_SIZE if LOT_SIZE else 0
        if current_lots >= MAX_TOTAL_LOTS: return

        origin_price = state.target_price
        origin_idx   = state.entry_candle if state.entry_candle is not None else candle_idx

        if state.direction == 'SELL':
            sh = state.sh_registry.next_entry_level(
                close=close, origin_idx=origin_idx, origin_price=origin_price,
                last_used_price=state.last_added_price + 0.0001,
                used_prices=state.used_prices_in_trade
            )
            if sh is None: return
            new_sl = sh['price'] + SL_BUFFER
            log.info(f"[ADD-SELL] SH {sh['price']:.2f}  new SL:{new_sl:.2f}")
            self._place_order(candle, 'SELL', close, new_sl, state.target_price,
                              candle_idx, is_add=True,
                              pivot_price_used=sh['price'], setup_size=0.0)

        elif state.direction == 'BUY':
            sl = state.sl_registry.next_entry_level(
                close=close, origin_idx=origin_idx, origin_price=origin_price,
                last_used_price=state.last_added_price - 0.0001,
                used_prices=state.used_prices_in_trade
            )
            if sl is None: return
            new_sl = sl['price'] - SL_BUFFER
            log.info(f"[ADD-BUY]  SL {sl['price']:.2f}  new SL:{new_sl:.2f}")
            self._place_order(candle, 'BUY', close, new_sl, state.target_price,
                              candle_idx, is_add=True,
                              pivot_price_used=sl['price'], setup_size=0.0)

    # ── EXIT CHECK ────────────────────────────────────────────────────────────

    def _check_exit(self, candle) -> str:
        close = candle['close']
        state = self.state

        if state.direction == 'SELL':
            if close >= state.sl_trigger:
                log.warning(f"[SL] SELL  close {close:.2f} >= SL {state.sl_trigger:.2f}")
                self._exit(candle['dt'], close, 'SL'); return 'SL_EXIT'
            if close <= state.target_price:
                log.info(f"[TARGET] SELL  close {close:.2f} <= T {state.target_price:.2f}")
                self._exit(candle['dt'], close, 'TARGET'); return 'TARGET_EXIT'

        elif state.direction == 'BUY':
            if close <= state.sl_trigger:
                log.warning(f"[SL] BUY  close {close:.2f} <= SL {state.sl_trigger:.2f}")
                self._exit(candle['dt'], close, 'SL'); return 'SL_EXIT'
            if close >= state.target_price:
                log.info(f"[TARGET] BUY  close {close:.2f} >= T {state.target_price:.2f}")
                self._exit(candle['dt'], close, 'TARGET'); return 'TARGET_EXIT'

        return 'HOLDING'

    def check_tick_exit(self, price: float):
        s = self.state
        if s.status != TradeState.OPEN: return
        now = datetime.datetime.now(IST).replace(tzinfo=None)

        if s.day_bias == 'BEARISH' and s.orb_high and price > s.orb_high:
            log.warning(f"[INVALIDATION-TICK] ORB HIGH {price:.2f} > {s.orb_high:.2f}")
            self._exit(now, price, 'ORB_HIGH_BREAK_ALGO_DEAD')
            s.algo_dead = True; return

        if s.day_bias == 'BULLISH' and s.orb_low and price < s.orb_low:
            log.warning(f"[INVALIDATION-TICK] ORB LOW {price:.2f} < {s.orb_low:.2f}")
            self._exit(now, price, 'ORB_LOW_BREAK_ALGO_DEAD')
            s.algo_dead = True; return

        if s.direction == 'SELL' and price <= s.target_price:
            self._exit(now, price, 'TARGET_TICK')
        elif s.direction == 'BUY' and price >= s.target_price:
            self._exit(now, price, 'TARGET_TICK')

    def square_off(self, price: float):
        if self.state.status == TradeState.OPEN:
            log.info(f"[SQOFF] Forced square-off at {price:.2f}")
            self._exit(datetime.datetime.now(IST).replace(tzinfo=None), price, 'SQUARE_OFF')
        self.state.disarm('square-off time')

    # ── ORDER PLACEMENT ───────────────────────────────────────────────────────

    def _place_order(self, candle, direction: str, entry: float,
                     sl_trigger: float, target: float, candle_idx: int,
                     is_add: bool, pivot_price_used: float, setup_size: float):
        state = self.state
        qty   = (ADD_LOTS * LOT_SIZE) if is_add else (LOTS * LOT_SIZE)

        if not self.paper and self.kite:
            try:
                kite_tx = (self.kite.TRANSACTION_TYPE_SELL if direction == 'SELL'
                           else self.kite.TRANSACTION_TYPE_BUY)
                oid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR, exchange=EXCHANGE_FUT,
                    tradingsymbol=self.symbol, transaction_type=kite_tx,
                    quantity=qty, product=PRODUCT,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                log.info(f"[ORDER] {direction}:{oid} qty:{qty}")
            except Exception as e:
                log.error(f"[ERR] Order failed: {e}"); return
        else:
            log.info(f"[PAPER] {'ADD' if is_add else 'ENTRY'} {direction} qty:{qty}")

        if state.status != TradeState.OPEN:
            state.status          = TradeState.OPEN
            state.direction       = direction
            state.total_qty       = qty
            state.avg_entry_price = entry
            state.entry_time      = candle['dt']
            state.entry_candle    = candle_idx
            state.target_price    = target
            state.sl_trigger      = sl_trigger
            state.trades_today   += 1
            state.used_prices_in_trade.add(pivot_price_used)
            state.last_added_price  = pivot_price_used
            state.vwap_breach_count = 0
            state.last_vwap_side    = None
        else:
            if state.avg_entry_price is None or state.total_qty == 0:
                log.warning("[FIX] avg_entry_price was None during ADD — correcting")
                state.avg_entry_price = entry
                state.total_qty = qty
            else:
                new_total = state.total_qty + qty
                state.avg_entry_price = (
                    (state.avg_entry_price * state.total_qty + entry * qty) / new_total
                )
                state.total_qty = new_total
            if direction == 'SELL':
                state.sl_trigger       = max(state.sl_trigger, sl_trigger)
                state.last_added_price = max(state.last_added_price, pivot_price_used)
            else:
                state.sl_trigger       = min(state.sl_trigger, sl_trigger)
                state.last_added_price = min(state.last_added_price, pivot_price_used)
            state.used_prices_in_trade.add(pivot_price_used)

        log.info(f"[POS] {state.direction}  qty:{state.total_qty}  "
                 f"avg:{state.avg_entry_price:.2f}  "
                 f"SL:{state.sl_trigger:.2f}  T:{state.target_price:.2f}")

    # ── EXIT ──────────────────────────────────────────────────────────────────

    def _exit(self, dt, price: float, reason: str):
        state = self.state
        qty   = state.total_qty
        if qty <= 0:
            state.reset_trade(); return

        if not self.paper and self.kite:
            try:
                kite_tx = (self.kite.TRANSACTION_TYPE_BUY if state.direction == 'SELL'
                           else self.kite.TRANSACTION_TYPE_SELL)
                xid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR, exchange=EXCHANGE_FUT,
                    tradingsymbol=self.symbol, transaction_type=kite_tx,
                    quantity=qty, product=PRODUCT,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                log.info(f"[ORDER] exit:{xid} qty:{qty}")
            except Exception as e:
                log.error(f"[ERR] Exit order failed: {e}  — EXIT MANUALLY")

        pnl = ((state.avg_entry_price - price) if state.direction == 'SELL'
               else (price - state.avg_entry_price)) * qty
        state.pnl_today += pnl

        dt = dt.replace(tzinfo=None) if getattr(dt, 'tzinfo', None) else dt

        trade_record = {
            'direction':   state.direction,
            'bias':        state.day_bias,
            'entry_dt':    state.entry_time,
            'exit_dt':     dt,
            'entry_price': state.avg_entry_price,
            'exit_price':  price,
            'sl_trigger':  state.sl_trigger,
            'target':      state.target_price,
            'reason':      reason,
            'pnl':         pnl,
            'qty':         qty,
            'setup_size':  round(abs(state.avg_entry_price - state.target_price), 2),
        }
        state.trade_log.append(trade_record)

        mode_str = ('REPLAY' if (self.db and self.db.is_backtest)
                    else ('PAPER' if self.paper else 'LIVE'))
        if self.db:
            self.db.log_trade(
                trade_num    = len(state.trade_log),
                t            = trade_record,
                day_pnl      = state.pnl_today,
                mode         = mode_str,
                symbol       = self.symbol,
                expiry       = self.expiry,
                session_date = self.session_date,
                orb_high     = state.orb_high,
                orb_low      = state.orb_low,
            )

        log.info("=" * 62)
        log.info(f"[EXIT] {state.direction}  reason={reason}  bias={state.day_bias}")
        log.info(f"   AvgEntry {state.avg_entry_price:.2f} -> Exit {price:.2f}")
        log.info(f"   Qty {qty}  Trade PnL Rs.{pnl:+,.0f}  Day PnL Rs.{state.pnl_today:+,.0f}")
        log.info("=" * 62)

        state.reset_trade()


# =============================================================================
#  KITE DATA FETCH
# =============================================================================

def fetch_candles(kite, instrument_token, from_date: str = None,
                  to_date: str = None, live: bool = False) -> list:
    now = datetime.datetime.now(IST)
    if live:
        from_dt = now.replace(hour=9, minute=15, second=0, microsecond=0, tzinfo=IST)
        to_dt   = now.replace(tzinfo=IST)
    else:
        fd = from_date or FROM_DATE
        td = to_date   or TO_DATE
        from_dt = datetime.datetime.strptime(fd, "%Y-%m-%d").replace(hour=9, minute=15, tzinfo=IST)
        to_dt   = datetime.datetime.strptime(td, "%Y-%m-%d").replace(hour=15, minute=30, tzinfo=IST)
        if to_dt.date() >= now.date():
            to_dt = now.replace(tzinfo=IST)

    log.info(f"Fetching {INTERVAL}  "
             f"{from_dt.strftime('%d %b %Y %H:%M')} -> {to_dt.strftime('%d %b %Y %H:%M')}")

    records = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt, to_date=to_dt,
        interval=INTERVAL, continuous=False, oi=False)

    out = []
    for r in records:
        dt = r['date'] if isinstance(r['date'], datetime.datetime) \
             else datetime.datetime.fromisoformat(str(r['date']))
        dt = dt.replace(tzinfo=None, second=0, microsecond=0)
        if datetime.time(9, 15) <= dt.time() <= datetime.time(15, 30):
            out.append({'dt': dt, 'open': r['open'], 'high': r['high'],
                        'low': r['low'], 'close': r['close'],
                        'volume': r.get('volume', 0)})

    days = sorted(set(c['dt'].date() for c in out))
    log.info(f"Fetched {len(out)} candles across {len(days)} day(s)  "
             f"({days[0]} -> {days[-1]})" if days else "No candles returned")
    return out


# =============================================================================
#  LIVE BOT
# =============================================================================

class LiveBot:
    def __init__(self, kite, db: DB):
        self.kite    = kite
        self.db      = db
        self.builder = CandleBuilder()
        self.state   = TradeState()
        self._lock   = threading.Lock()

        self.instrument_token, self.tradingsymbol, self.expiry = \
            get_current_month_nifty_future(kite)
        self.session_date = datetime.datetime.now(IST).strftime('%Y-%m-%d')

        self.strategy = StrategyCore(
            state        = self.state,
            paper        = PAPER_TRADE,
            kite         = kite,
            symbol       = self.tradingsymbol,
            expiry       = self.expiry,
            db           = db,
            session_date = self.session_date,
        )

        log.info(f"[LIVE] {self.tradingsymbol}  Expiry:{self.expiry}  Paper:{PAPER_TRADE}")

        # ── Preload: replay today's candles before going live ─────────────────
        log.info("[PRELOAD] Fetching today's candles for strategy warm-up...")
        try:
            candles = fetch_candles(kite, self.instrument_token, live=True)
            if not candles:
                log.warning("[PRELOAD] No candles yet — starting fresh")
            else:
                sq_time = datetime.time(*SQUARE_OFF_TIME)
                for c in candles[:-1]:
                    self.builder.completed.append(c)
                completed = self.builder.get_all()
                log.info(f"[PRELOAD] Replaying {len(completed)} candles  "
                         f"({completed[0]['dt'].strftime('%H:%M')} → "
                         f"{completed[-1]['dt'].strftime('%H:%M')})")
                for i in range(len(completed)):
                    if completed[i]['dt'].time() >= sq_time: break
                    if i < PIVOT_LEFT + PIVOT_RIGHT + 1: continue
                    self.strategy.on_candle_close(completed, i)
                log.info(f"[PRELOAD] Done — bias:{self.state.day_bias}  "
                         f"pos:{self.state.status}  trades:{self.state.trades_today}  "
                         f"PnL:Rs.{self.state.pnl_today:+,.0f}")
        except Exception as e:
            import traceback
            log.warning(f"[PRELOAD] Failed: {e}\n{traceback.format_exc()}")

        # ── WebSocket ─────────────────────────────────────────────────────────
        self.ticker = KiteTicker(API_KEY, ACCESS_TOKEN)
        self.ticker.on_connect = self._on_connect
        self.ticker.on_ticks   = self._on_ticks
        self.ticker.on_close   = lambda ws, c, r: log.warning(f"WS closed: {r}")
        self.ticker.on_error   = lambda ws, c, r: log.error(f"WS error: {r}")

    def _on_connect(self, ws, _):
        log.info("WebSocket connected")
        ws.subscribe([self.instrument_token])
        ws.set_mode(ws.MODE_LTP, [self.instrument_token])

    def _on_ticks(self, ws, ticks):
        for t in ticks:
            if t['instrument_token'] != self.instrument_token: continue
            with self._lock:
                self._process(t['last_price'], datetime.datetime.now(IST))

    def _process(self, price, ts):
        if ts.time() >= datetime.time(*SQUARE_OFF_TIME):
            if self.state.status == TradeState.OPEN:
                self.strategy.square_off(price)
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
        log.info("Starting live WebSocket feed...")
        self.ticker.connect(threaded=True)
        try:
            while True:
                time.sleep(30)
                now = datetime.datetime.now(IST)
                if (now.time() > datetime.time(*SQUARE_OFF_TIME)
                        and self.state.status == TradeState.IDLE):
                    log.info(f"Session done — trades:{self.state.trades_today}  "
                             f"PnL:Rs.{self.state.pnl_today:+,.0f}")
                    break
        except KeyboardInterrupt:
            if self.state.status == TradeState.OPEN:
                log.warning("Open trade on exit — EXIT MANUALLY ON KITE")
        finally:
            self.ticker.close()
            self.db.upsert_session_summary(
                session_date  = self.session_date,
                symbol        = self.tradingsymbol,
                mode          = 'PAPER' if PAPER_TRADE else 'LIVE',
                day_bias      = self.state.day_bias,
                total_trades  = self.state.trades_today,
                total_pnl     = self.state.pnl_today,
                algo_dead     = self.state.algo_dead,
                trade_log     = self.state.trade_log,
            )


# =============================================================================
#  BACKTEST REPLAY  (multi-day: fresh TradeState per calendar date)
# =============================================================================

def run_replay(kite, db: DB):
    """
    Fetches FROM_DATE -> TO_DATE candles.
    Processes each calendar date with a fresh TradeState so multi-day backtests
    work correctly and every day has its own rows in the backtest_* tables.
    """
    token, tsym, exp = get_current_month_nifty_future(kite)

    log.info("=" * 62)
    log.info(f"  BACKTEST  {FROM_DATE} -> {TO_DATE}  [{INTERVAL}]  {tsym}  (Exp:{exp})")
    log.info(f"  Writes -> backtest_trades, backtest_swing_pivots,")
    log.info(f"            backtest_orb_snapshots, backtest_session_summaries")
    log.info("=" * 62)

    try:
        all_candles = fetch_candles(kite, token, live=False)
    except Exception as e:
        log.error(f"Fetch failed: {e}"); return
    if not all_candles:
        log.error("No candles returned"); return

    # Group by date
    by_day: dict = _dd(list)
    for c in all_candles:
        by_day[c['dt'].date()].append(c)

    sq_time        = datetime.time(*SQUARE_OFF_TIME)
    overall_pnl    = 0.0
    overall_trades = 0

    for day_date in sorted(by_day.keys()):
        day_candles  = by_day[day_date]
        session_date = day_date.strftime('%Y-%m-%d')

        log.info("=" * 62)
        log.info(f"  DAY: {session_date}  ({len(day_candles)} candles)")
        log.info("=" * 62)

        state    = TradeState()
        strategy = StrategyCore(
            state        = state,
            paper        = True,
            kite         = kite,
            symbol       = tsym,
            expiry       = exp,
            db           = db,
            session_date = session_date,
        )

        for i, candle in enumerate(day_candles):
            if candle['dt'].time() >= sq_time:
                if state.status == TradeState.OPEN:
                    strategy.square_off(candle['close'])
                break
            if i < PIVOT_LEFT + PIVOT_RIGHT + 1:
                continue
            strategy.on_candle_close(day_candles[:i + 1], i)
            if REPLAY_DELAY_S > 0:
                time.sleep(REPLAY_DELAY_S)

        overall_pnl    += state.pnl_today
        overall_trades += state.trades_today

        log.info(f"  Day result — bias:{state.day_bias}  algo_dead:{state.algo_dead}  "
                 f"trades:{len(state.trade_log)}  PnL:Rs.{state.pnl_today:+,.0f}")
        for i, t in enumerate(state.trade_log, 1):
            et = t['exit_dt'].strftime('%H:%M') if hasattr(t['exit_dt'], 'strftime') else ''
            log.info(
                f"    [{i}] {t['direction']}({t.get('bias','?')})  "
                f"in:{t['entry_dt'].strftime('%H:%M')}@{t['entry_price']:.2f}  "
                f"out:{et}@{t['exit_price']:.2f}  "
                f"{t['reason']}  Rs.{t['pnl']:+,.0f}  qty:{t.get('qty',0)}"
            )

        db.upsert_session_summary(
            session_date  = session_date,
            symbol        = tsym,
            mode          = 'REPLAY',
            day_bias      = state.day_bias,
            total_trades  = state.trades_today,
            total_pnl     = state.pnl_today,
            algo_dead     = state.algo_dead,
            trade_log     = state.trade_log,
        )

    log.info("=" * 62)
    log.info(f"  BACKTEST COMPLETE  {FROM_DATE} -> {TO_DATE}")
    log.info(f"  Total trades : {overall_trades}")
    log.info(f"  Total PnL    : Rs.{overall_pnl:+,.0f}")
    log.info("=" * 62)


# =============================================================================
#  CONFIG PRINT
# =============================================================================

def print_config(symbol: str = "NIFTYFUT", mode: str = "LIVE"):
    log.info("=" * 62)
    log.info("  NIFTY FUT  ORB Breakout — Bullish + Bearish Swing Bot")
    log.info(f"  Mode        : {mode}")
    log.info(f"  DB tables   : {'backtest_trades, backtest_swing_pivots, ...' if mode == 'BACKTEST' else 'trades, swing_pivots, ...'}")
    log.info("=" * 62)
    log.info(f"  Symbol      : {symbol} ({EXCHANGE_FUT})")
    log.info(f"  Interval    : {INTERVAL}")
    log.info(f"  ORB window  : 9:15 - 9:{15+ORB_MINUTES:02d}  ({ORB_MINUTES} bars)")
    log.info(f"  Pivot L/R   : {PIVOT_LEFT} / {PIVOT_RIGHT}")
    log.info(f"  ORB buf     : {BREAK_BUFFER} pts  |  Retest: {RETEST_BUFFER} pts")
    log.info(f"  Min setup   : {MIN_SETUP_POINTS} pts  |  SL buf: {SL_BUFFER} pts")
    log.info(f"  VWAP SL     : {VWAP_CONSEC_CLOSES} closes beyond VWAP ± {VWAP_SL_POINTS} pts")
    log.info(f"  Lots        : {LOTS} / add {ADD_LOTS} / max {MAX_TOTAL_LOTS}")
    log.info(f"  Max SL      : {MAX_SL_POINTS} pts  |  Max trades/day: {MAX_TRADES_DAY}")
    log.info(f"  Square-off  : {SQUARE_OFF_TIME[0]:02d}:{SQUARE_OFF_TIME[1]:02d} IST")
    log.info(f"  Paper trade : {PAPER_TRADE}")
    if mode == 'BACKTEST':
        log.info(f"  Date range  : {FROM_DATE} -> {TO_DATE}")
    log.info(f"  Database    : {DATABASE_URL[:55]}...")
    log.info("=" * 62)


# =============================================================================
#  ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NIFTY FUT ORB Breakout Bot')
    parser.add_argument('--replay', action='store_true',
                        help='Force backtest mode (same as FORCE_REPLAY=true)')
    args = parser.parse_args()

    if "YOUR_" in API_KEY or "YOUR_" in ACCESS_TOKEN:
        print("\nSet KITE_API_KEY and KITE_ACCESS_TOKEN as environment variables.\n")
        sys.exit(1)

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    try:
        token, tsym, exp = get_current_month_nifty_future(kite)
    except Exception as e:
        log.error(f"Failed to resolve NIFTY FUT: {e}"); sys.exit(1)

    now         = datetime.datetime.now(IST).time()
    is_live     = datetime.time(9, 15) <= now <= datetime.time(15, 30)
    do_backtest = args.replay or FORCE_REPLAY or not is_live

    if do_backtest:
        reason = ("--replay flag" if args.replay
                  else "FORCE_REPLAY=true" if FORCE_REPLAY
                  else "market closed")
        log.info(f"Mode: BACKTEST ({reason})")
        print_config(tsym, mode='BACKTEST')
        db = DB(DATABASE_URL, is_backtest=True)   # writes to backtest_* tables
        run_replay(kite, db)
    else:
        log.info("Mode: LIVE")
        print_config(tsym, mode='LIVE')
        db = DB(DATABASE_URL, is_backtest=False)  # writes to live tables
        LiveBot(kite, db).run()
