#!/usr/bin/env python3
"""
NIFTY FUT Automated Swing Trade Bot  —  ORB Breakout (Bullish + Bearish)
=========================================================================
PRODUCTION VERSION — Railway PostgreSQL, no UI, no CSV.

SETUP
-----
  pip install kiteconnect pandas psycopg2-binary

  Set environment variables before running:
    DATABASE_URL   = postgresql://user:pass@host:port/dbname   (from Railway)
    REPLAY_DATE    = 2026-03-04          (optional — force replay a single date)
    REPLAY_FROM_DATE / REPLAY_TO_DATE    (optional — replay a date range)

DATABASE TABLES (auto-created on first run)
-------------------------------------------
  trades        — one row per completed trade
  heartbeat     — one row per minute candle: price, VWAP, position snapshot
  pivot_events  — one row each time a new swing high/low is confirmed
"""

import sys, time, datetime, threading, logging, os, io

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
    import psycopg2
    import psycopg2.pool
except: missing.append('psycopg2-binary')

if missing:
    print(f"\n[ERR]  Run:  pip install {' '.join(missing)}\n"); sys.exit(1)

IST = ZoneInfo("Asia/Kolkata")

# =============================================================================
#  CONFIG
# =============================================================================

API_KEY          = "9qfecm39l1j64xyc"
ACCESS_TOKEN     = "LY24Uy4cYIHXrCmcXeRv7P08DVoUYeP7"

BASE_SYMBOL      = "NIFTY"
EXCHANGE_FUT     = "NFO"
PRODUCT          = "MIS"
INTERVAL         = "minute"

ORB_MINUTES      = 15

PIVOT_LEFT       = 2
PIVOT_RIGHT      = 2
MIN_PIVOT_DIST   = 15

BREAK_BUFFER     = 5
RETEST_BUFFER    = 5
MIN_SETUP_POINTS = 15
SL_BUFFER        = 10
VWAP_SL_POINTS   = 0

LOTS             = 1
LOT_SIZE         = 50
ADD_LOTS         = 1
MAX_TOTAL_LOTS   = 6
MAX_SL_POINTS    = 300
MAX_TRADES_DAY   = 10
MIN_ADD_DIST     = 15

SQUARE_OFF_TIME  = (15, 15)
PAPER_TRADE      = True

# Replay defaults (overridden by env vars)
FROM_DATE        = "2026-02-27"
TO_DATE          = "2026-02-27"
REPLAY_DELAY_S   = 0.05

LOG_FILE         = "trader.log"

# =============================================================================
#  LOGGING
# =============================================================================

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level    = logging.INFO,
    format   = '%(asctime)s IST  %(levelname)-8s  %(message)s',
    datefmt  = '%H:%M:%S',
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)

# =============================================================================
#  DATABASE  (Railway PostgreSQL via DATABASE_URL)
# =============================================================================

_pool: psycopg2.pool.ThreadedConnectionPool = None
_db_lock = threading.Lock()


def _get_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        log.error("[DB] DATABASE_URL environment variable is not set.")
        log.error("[DB] Export it before running:")
        log.error("[DB]   export DATABASE_URL=postgresql://user:pass@host:port/dbname")
        sys.exit(1)
    # psycopg2 requires 'postgresql://' not 'postgres://'
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url


def init_db():
    """Create connection pool and tables."""
    global _pool
    url = _get_database_url()
    try:
        _pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=5, dsn=url)
        log.info("[DB] Connected to Railway PostgreSQL")
    except Exception as e:
        log.error(f"[DB] Cannot connect: {e}")
        sys.exit(1)

    ddl = """
    CREATE TABLE IF NOT EXISTS trades (
        id              SERIAL PRIMARY KEY,
        trade_num       INTEGER,
        mode            TEXT,
        symbol          TEXT,
        direction       TEXT,
        bias            TEXT,
        entry_time      TIMESTAMPTZ,
        entry_price     NUMERIC(12,2),
        exit_time       TIMESTAMPTZ,
        exit_price      NUMERIC(12,2),
        sl_trigger      NUMERIC(12,2),
        target          NUMERIC(12,2),
        qty             INTEGER,
        lots            INTEGER,
        pnl_points      NUMERIC(12,2),
        pnl_rs          NUMERIC(14,2),
        setup_size      NUMERIC(12,2),
        day_pnl_rs      NUMERIC(14,2),
        reason          TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS heartbeat (
        id              SERIAL PRIMARY KEY,
        ts              TIMESTAMPTZ,
        symbol          TEXT,
        price           NUMERIC(12,2),
        vwap            NUMERIC(12,2),
        orb_high        NUMERIC(12,2),
        orb_low         NUMERIC(12,2),
        day_bias        TEXT,
        position        TEXT,
        direction       TEXT,
        avg_entry       NUMERIC(12,2),
        sl_trigger      NUMERIC(12,2),
        target          NUMERIC(12,2),
        qty             INTEGER,
        day_pnl_rs      NUMERIC(14,2),
        mode            TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS pivot_events (
        id              SERIAL PRIMARY KEY,
        ts              TIMESTAMPTZ,
        symbol          TEXT,
        pivot_type      TEXT,
        price           NUMERIC(12,2),
        candle_idx      INTEGER,
        day_bias        TEXT,
        mode            TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );
    """
    _exec(ddl)
    log.info("[DB] Tables ready  (trades / heartbeat / pivot_events)")


def _exec(sql: str, params: tuple = None):
    """Execute a write statement using the pool. Thread-safe."""
    conn = None
    try:
        with _db_lock:
            conn = _pool.getconn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
    except Exception as e:
        log.warning(f"[DB] Write failed: {e}")
    finally:
        if conn:
            with _db_lock:
                _pool.putconn(conn)


def db_log_trade(trade_num: int, t: dict, day_pnl: float,
                 mode: str, symbol: str):
    pnl_pts = (round(t['entry_price'] - t['exit_price'], 2)
               if t['direction'] == 'SELL'
               else round(t['exit_price'] - t['entry_price'], 2))
    exit_ts = (t['exit_dt'].strftime('%Y-%m-%d %H:%M:%S')
               if hasattr(t['exit_dt'], 'strftime') else str(t['exit_dt']))
    sql = """
        INSERT INTO trades
            (trade_num, mode, symbol, direction, bias,
             entry_time, entry_price, exit_time, exit_price,
             sl_trigger, target, qty, lots,
             pnl_points, pnl_rs, setup_size, day_pnl_rs, reason)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    params = (
        trade_num, mode, symbol,
        t['direction'], t.get('bias', ''),
        t['entry_dt'].strftime('%Y-%m-%d %H:%M:%S'),
        round(t['entry_price'], 2),
        exit_ts,
        round(t['exit_price'], 2),
        round(t['sl_trigger'], 2),
        round(t['target'], 2),
        t.get('qty', 0),
        t.get('qty', 0) // LOT_SIZE if LOT_SIZE else 0,
        pnl_pts,
        round(t['pnl'], 2),
        round(t.get('setup_size', 0), 2),
        round(day_pnl, 2),
        t['reason'],
    )
    _exec(sql, params)
    log.info(f"[DB] Trade #{trade_num} logged")


def db_log_heartbeat(ts: datetime.datetime, symbol: str, price: float,
                     vwap: float, state, mode: str):
    sql = """
        INSERT INTO heartbeat
            (ts, symbol, price, vwap, orb_high, orb_low,
             day_bias, position, direction, avg_entry,
             sl_trigger, target, qty, day_pnl_rs, mode)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    params = (
        ts.strftime('%Y-%m-%d %H:%M:%S'),
        symbol,
        round(price, 2),
        round(vwap, 2)                  if vwap                  else None,
        round(state.orb_high, 2)        if state.orb_high        else None,
        round(state.orb_low,  2)        if state.orb_low         else None,
        state.day_bias,
        state.status,
        state.direction or '',
        round(state.avg_entry_price, 2) if state.avg_entry_price else None,
        round(state.sl_trigger, 2)      if state.sl_trigger      else None,
        round(state.target_price, 2)    if state.target_price    else None,
        state.total_qty,
        round(state.pnl_today, 2),
        mode,
    )
    _exec(sql, params)


def db_log_pivot(ts: datetime.datetime, symbol: str, pivot_type: str,
                 price: float, candle_idx: int, day_bias: str, mode: str):
    sql = """
        INSERT INTO pivot_events
            (ts, symbol, pivot_type, price, candle_idx, day_bias, mode)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    params = (
        ts.strftime('%Y-%m-%d %H:%M:%S'),
        symbol, pivot_type,
        round(price, 2), candle_idx,
        day_bias, mode,
    )
    _exec(sql, params)
    log.info(f"[DB] Pivot {pivot_type} @ {price:.2f}  [{ts.strftime('%H:%M')}]")

# =============================================================================
#  AUTO-DETECT CURRENT MONTH NIFTY FUT
# =============================================================================

_INSTR_CACHE = {"ts": None, "data": None}


def _get_instruments_cached(kite, exchange: str):
    if _INSTR_CACHE["data"] is None:
        log.info(f"[INSTR] Downloading instruments for {exchange}...")
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
        raise RuntimeError("No NIFTY FUT contracts found.")
    futs.sort(key=lambda x: x["expiry"])
    c = futs[0]
    log.info("=" * 62)
    log.info(f"[FUT] Selected  : {c['tradingsymbol']}")
    log.info(f"[FUT] Expiry    : {c['expiry']}")
    log.info(f"[FUT] Token     : {int(c['instrument_token'])}")
    log.info("=" * 62)
    return int(c["instrument_token"]), str(c["tradingsymbol"]), c["expiry"]

# =============================================================================
#  VWAP
# =============================================================================

def compute_vwap_series(candles: list):
    if not candles:
        return []
    cum_pv = cum_v = 0.0
    out = []
    for c in candles:
        v = float(c.get('volume', 0) or 0)
        if v <= 0:
            v = 1.0
        tp = (float(c['high']) + float(c['low']) + float(c['close'])) / 3.0
        cum_pv += tp * v
        cum_v  += v
        out.append(cum_pv / cum_v if cum_v else float(c['close']))
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
            last = out[-1]
            dist = abs(p['price'] - last['price'])
            has_opp = any(last['idx'] < oi < p['idx'] for oi in opp_set)
            if dist < min_dist and not has_opp:
                if keep == 'high' and p['price'] > last['price']:
                    out[-1] = p
                elif keep == 'low' and p['price'] < last['price']:
                    out[-1] = p
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

    def on_tick(self, price: float, ts: datetime.datetime, volume: int = 0):
        minute = ts.replace(second=0, microsecond=0, tzinfo=None)
        if self.current is None:
            self._open(minute, price, volume)
            return None
        if minute == self.current['dt']:
            self.current['high']  = max(self.current['high'],  price)
            self.current['low']   = min(self.current['low'],   price)
            self.current['close'] = price
            if volume > 0:
                self.current['volume'] = self.current.get('volume', 0) + volume
            return None
        closed = dict(self.current)
        self.completed.append(closed)
        self._open(minute, price, volume)
        return closed

    def _open(self, m, p, volume=0):
        self.current = {'dt': m, 'open': p, 'high': p, 'low': p,
                        'close': p, 'volume': volume}

    def get_all(self):
        return self.completed

# =============================================================================
#  PIVOT REGISTRY
# =============================================================================

class PivotRegistry:
    def __init__(self, pivot_type: str):
        self._type      = pivot_type   # 'high' or 'low'
        self._pivots    = []
        self._known_idx = set()        # prevents duplicate DB inserts

    def update(self, all_pivots: list, symbol: str = '', day_bias: str = '',
               mode: str = '', emit_db: bool = True):
        for p in all_pivots:
            if p['idx'] not in self._known_idx:
                self._pivots.append({'price': p['price'],
                                     'idx':   p['idx'],
                                     'dt':    p['dt']})
                self._known_idx.add(p['idx'])
                if emit_db and symbol:
                    ptype = 'SWING_HIGH' if self._type == 'high' else 'SWING_LOW'
                    db_log_pivot(p['dt'], symbol, ptype,
                                 p['price'], p['idx'], day_bias, mode)

    def next_entry_level(self, close: float, origin_idx: int,
                         origin_price: float, last_used_price: float,
                         used_prices: set, vwap: float = None):
        if self._type == 'high':
            candidates = [
                p for p in self._pivots
                if p['idx'] < origin_idx
                and p['price'] > last_used_price
                and p['price'] >= origin_price + MIN_SETUP_POINTS
                and close >= p['price'] - RETEST_BUFFER
                and p['price'] not in used_prices
                and (vwap is None or close <= vwap)
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
                and (vwap is None or close >= vwap)
            ]
            return max(candidates, key=lambda p: p['price']) if candidates else None

    def all_highs(self):
        return list(self._pivots) if self._type == 'high' else []

    def all_lows(self):
        return list(self._pivots) if self._type == 'low' else []

    def reset(self):
        self._pivots    = []
        self._known_idx = set()

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

        self._last_skip_price    = None
        self.vwap_paused         = False
        self.vwap_pause_ref_high = None
        self.vwap_pause_ref_low  = None

    def arm(self, price: float, idx: int, pivot_type: str):
        self.armed        = True
        self.origin_price = price
        self.origin_idx   = idx
        self.origin_type  = pivot_type
        self._last_skip_price = None
        label = 'swing LOW' if pivot_type == 'low' else 'swing HIGH'
        log.info(f"[ARM] {label} armed — origin {price:.2f}  (candle #{idx})")

    def disarm(self, reason: str = ''):
        self.armed        = False
        self.origin_price = None
        self.origin_idx   = None
        self.origin_type  = None
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

# =============================================================================
#  OPENING RANGE
# =============================================================================

def compute_orb(candles: list):
    if not candles:
        return None
    day   = candles[-1]['dt'].date()
    end_t = datetime.time(9, 15 + ORB_MINUTES)
    orb_c = [c for c in candles
             if c['dt'].date() == day
             and datetime.time(9, 15) <= c['dt'].time() < end_t]
    if not orb_c:
        return None
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
    def __init__(self, state: TradeState, paper: bool = True,
                 kite=None, symbol: str = "NIFTYFUT", mode: str = "PAPER"):
        self.state      = state
        self.paper      = paper
        self.kite       = kite
        self.symbol     = symbol
        self.mode       = mode
        self._last_vwap = None

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

        if state.algo_dead:
            return 'ALGO_DEAD'

        pivots = detect_pivots(candles[:candle_idx + 1], PIVOT_LEFT, PIVOT_RIGHT)

        # Update registries — new pivots written to pivot_events table
        state.sh_registry.update(pivots['all_highs'],
                                  symbol=self.symbol, day_bias=state.day_bias,
                                  mode=self.mode, emit_db=True)
        state.sl_registry.update(pivots['all_lows'],
                                  symbol=self.symbol, day_bias=state.day_bias,
                                  mode=self.mode, emit_db=True)

        vwap_series = compute_vwap_series(candles[:candle_idx + 1])
        vwap = vwap_series[-1] if vwap_series else None
        self._last_vwap = vwap

        # Heartbeat — one DB row per closed candle (every minute)
        db_log_heartbeat(candle['dt'], self.symbol, close, vwap, state, self.mode)

        if state.status == TradeState.OPEN:
            result = self._check_invalidation(candle)
            if result:
                return result
            vwap_res = self._check_vwap_stop(candle, close, vwap, pivots)
            if vwap_res:
                return vwap_res
            self._maybe_add(candle, candle_idx, close, pivots)
            return self._check_exit(candle)

        if state.vwap_paused:
            inv = self._check_invalidation(candle)
            if inv:
                return inv
            reactivation = self._check_vwap_reactivation(
                candle, close, vwap, pivots, candle_idx)
            return reactivation if reactivation else 'VWAP_PAUSED_WATCHING'

        if state.trades_today >= MAX_TRADES_DAY:
            return 'MAX_TRADES'

        if state.day_bias == 'NONE':
            if close < orb['low'] - BREAK_BUFFER:
                state.day_bias = 'BEARISH'
                log.info(f"[BIAS] BEARISH — ORB low {orb['low']:.2f} broken (close {close:.2f})")
            elif close > orb['high'] + BREAK_BUFFER:
                state.day_bias = 'BULLISH'
                log.info(f"[BIAS] BULLISH — ORB high {orb['high']:.2f} broken (close {close:.2f})")
            else:
                return 'WAITING_ORB_BREAK'

        if vwap is not None and not state.vwap_paused:
            if state.day_bias == 'BULLISH' and close < vwap:
                all_highs = pivots.get('all_highs') or []
                ref = (max(p['price'] for p in all_highs) if all_highs
                       else (state.origin_price if state.origin_price else close))
                state.vwap_pause_ref_high = ref
                state.vwap_paused = True
                state.disarm('price closed below VWAP — paused')
                log.info(f"[VWAP_PAUSE] BULLISH: close {close:.2f} < VWAP {vwap:.2f}  "
                         f"→ paused. Need new HH > {ref:.2f}")
                return 'VWAP_PAUSED_ENTRY_BLOCKED'
            elif state.day_bias == 'BEARISH' and close > vwap:
                all_lows = pivots.get('all_lows') or []
                ref = (min(p['price'] for p in all_lows) if all_lows
                       else (state.origin_price if state.origin_price else close))
                state.vwap_pause_ref_low = ref
                state.vwap_paused = True
                state.disarm('price closed above VWAP — paused')
                log.info(f"[VWAP_PAUSE] BEARISH: close {close:.2f} > VWAP {vwap:.2f}  "
                         f"→ paused. Need new LL < {ref:.2f}")
                return 'VWAP_PAUSED_ENTRY_BLOCKED'

        if state.day_bias == 'BEARISH':
            last_low = pivots['last_low']
            if last_low is None:
                return 'NO_PIVOT_LOW'
            if state.origin_idx != last_low['idx']:
                state.arm(last_low['price'], last_low['idx'], 'low')
                return 'SWING_LOW_ARMED'
            if state.armed:
                return self._check_sell_entry(candle, candle_idx, close)

        elif state.day_bias == 'BULLISH':
            last_high = pivots['last_high']
            if last_high is None:
                return 'NO_PIVOT_HIGH'
            if state.origin_idx != last_high['idx']:
                state.arm(last_high['price'], last_high['idx'], 'high')
                return 'SWING_HIGH_ARMED'
            if state.armed:
                return self._check_buy_entry(candle, candle_idx, close)

        return 'WAITING_SETUP'

    # ── VWAP stop ──────────────────────────────────────────────────────────

    def _check_vwap_stop(self, candle, close: float, vwap: float, pivots: dict):
        s = self.state
        if vwap is None:
            return None
        if s.direction == 'BUY' and close < vwap - VWAP_SL_POINTS:
            log.warning(f"[VWAP_SL] BUY close {close:.2f} < VWAP {vwap:.2f} → EXIT+DEAD")
            self._exit(candle['dt'], close, 'VWAP_SL')
            s.algo_dead = True; s.vwap_paused = False
            return 'VWAP_SL_EXIT_DEAD'
        if s.direction == 'SELL' and close > vwap + VWAP_SL_POINTS:
            log.warning(f"[VWAP_SL] SELL close {close:.2f} > VWAP {vwap:.2f} → EXIT+DEAD")
            self._exit(candle['dt'], close, 'VWAP_SL')
            s.algo_dead = True; s.vwap_paused = False
            return 'VWAP_SL_EXIT_DEAD'
        return None

    # ── VWAP reactivation ──────────────────────────────────────────────────

    def _check_vwap_reactivation(self, candle, close: float, vwap: float,
                                  pivots: dict, candle_idx: int) -> str:
        s = self.state
        if not s.vwap_paused or vwap is None:
            return None

        if s.day_bias == 'BULLISH':
            if close < vwap:
                return 'VWAP_PAUSED_WATCHING'
            qualifying = [p for p in (pivots.get('all_highs') or [])
                          if p['price'] > s.vwap_pause_ref_high]
            if not qualifying:
                return 'VWAP_PAUSED_WATCHING'
            best = max(qualifying, key=lambda p: p['price'])
            log.info(f"[VWAP_REACTIVATE] BULLISH  HH {best['price']:.2f} > "
                     f"ref {s.vwap_pause_ref_high:.2f}  close {close:.2f} >= VWAP {vwap:.2f}")
            s.vwap_paused = False; s.vwap_pause_ref_high = None
            s.armed = True; s.origin_price = best['price']
            s.origin_idx = best['idx']; s.origin_type = 'high'
            s._last_skip_price = None
            log.info(f"[ARM] swing HIGH armed (reactivation) — "
                     f"origin {best['price']:.2f}  (candle #{best['idx']})")
            return 'VWAP_REACTIVATED'

        elif s.day_bias == 'BEARISH':
            if close > vwap:
                return 'VWAP_PAUSED_WATCHING'
            qualifying = [p for p in (pivots.get('all_lows') or [])
                          if p['price'] < s.vwap_pause_ref_low]
            if not qualifying:
                return 'VWAP_PAUSED_WATCHING'
            best = min(qualifying, key=lambda p: p['price'])
            log.info(f"[VWAP_REACTIVATE] BEARISH  LL {best['price']:.2f} < "
                     f"ref {s.vwap_pause_ref_low:.2f}  close {close:.2f} <= VWAP {vwap:.2f}")
            s.vwap_paused = False; s.vwap_pause_ref_low = None
            s.armed = True; s.origin_price = best['price']
            s.origin_idx = best['idx']; s.origin_type = 'low'
            s._last_skip_price = None
            log.info(f"[ARM] swing LOW armed (reactivation) — "
                     f"origin {best['price']:.2f}  (candle #{best['idx']})")
            return 'VWAP_REACTIVATED'

        return 'VWAP_PAUSED_WATCHING'

    # ── ORB invalidation ───────────────────────────────────────────────────

    def _check_invalidation(self, candle):
        state = self.state
        close = candle['close']
        if state.day_bias == 'BEARISH':
            if state.orb_high is not None and close > state.orb_high:
                log.warning(f"[INVALIDATION] BEARISH: close {close:.2f} > "
                            f"ORB HIGH {state.orb_high:.2f} — EXIT+DISARM")
                if state.status == TradeState.OPEN:
                    self._exit(candle['dt'], close, 'ORB_INVALIDATION')
                state.disarm('ORB high broken')
                state.vwap_paused = False
                return 'INVALIDATION_DISARM'
        elif state.day_bias == 'BULLISH':
            if state.orb_low is not None and close < state.orb_low:
                log.warning(f"[INVALIDATION] BULLISH: close {close:.2f} < "
                            f"ORB LOW {state.orb_low:.2f} — EXIT+DISARM")
                if state.status == TradeState.OPEN:
                    self._exit(candle['dt'], close, 'ORB_INVALIDATION')
                state.disarm('ORB low broken')
                state.vwap_paused = False
                return 'INVALIDATION_DISARM'
        return None

    # ── Entry checks ───────────────────────────────────────────────────────

    def _check_sell_entry(self, candle, candle_idx, close) -> str:
        state = self.state
        if self._last_vwap is not None and close > self._last_vwap:
            log.info(f"[VWAP GUARD] SELL blocked  close {close:.2f} > VWAP {self._last_vwap:.2f}")
            return 'ABOVE_VWAP_NO_ENTRY'
        sh = state.sh_registry.next_entry_level(
            close=close, origin_idx=state.origin_idx,
            origin_price=state.origin_price, last_used_price=0.0,
            used_prices=set(), vwap=self._last_vwap)
        if sh is None:
            return 'ARMED_WATCHING'
        setup_size = sh['price'] - state.origin_price
        if setup_size < MIN_SETUP_POINTS:
            if state._last_skip_price != sh['price']:
                log.info(f"[SKIP] Setup too small: SH {sh['price']:.2f} - "
                         f"origin {state.origin_price:.2f} = {setup_size:.1f} pts")
                state._last_skip_price = sh['price']
            return 'SETUP_TOO_SMALL'
        state._last_skip_price = None
        sl_trigger = sh['price'] + SL_BUFFER
        sl_dist    = sl_trigger - close
        if sl_dist <= 0: return 'SL_INVALID'
        if sl_dist > MAX_SL_POINTS: return 'SL_TOO_WIDE'
        target = state.origin_price
        log.info("=" * 62)
        log.info(f"[SELL] BEARISH ENTRY [{candle['dt'].strftime('%H:%M')}] {self.symbol}")
        log.info(f"   Close {close:.2f}  SH {sh['price']:.2f}  "
                 f"SL {sl_trigger:.2f}  T {target:.2f}  Setup {setup_size:.1f}pts")
        log.info("=" * 62)
        state.disarm('sell entry taken')
        self._place_order(candle, 'SELL', close, sl_trigger, target,
                          candle_idx, is_add=False,
                          pivot_price_used=sh['price'], setup_size=setup_size)
        return 'SELL_ENTRY'

    def _check_buy_entry(self, candle, candle_idx, close) -> str:
        state = self.state
        if self._last_vwap is not None and close < self._last_vwap:
            log.info(f"[VWAP GUARD] BUY blocked  close {close:.2f} < VWAP {self._last_vwap:.2f}")
            return 'BELOW_VWAP_NO_ENTRY'
        sl = state.sl_registry.next_entry_level(
            close=close, origin_idx=state.origin_idx,
            origin_price=state.origin_price, last_used_price=float('inf'),
            used_prices=set(), vwap=self._last_vwap)
        if sl is None:
            return 'ARMED_WATCHING'
        setup_size = state.origin_price - sl['price']
        if setup_size < MIN_SETUP_POINTS:
            if state._last_skip_price != sl['price']:
                log.info(f"[SKIP] Setup too small: origin {state.origin_price:.2f} - "
                         f"SL {sl['price']:.2f} = {setup_size:.1f} pts")
                state._last_skip_price = sl['price']
            return 'SETUP_TOO_SMALL'
        state._last_skip_price = None
        sl_trigger = sl['price'] - SL_BUFFER
        sl_dist    = close - sl_trigger
        if sl_dist <= 0: return 'SL_INVALID'
        if sl_dist > MAX_SL_POINTS: return 'SL_TOO_WIDE'
        target = state.origin_price
        log.info("=" * 62)
        log.info(f"[BUY]  BULLISH ENTRY [{candle['dt'].strftime('%H:%M')}] {self.symbol}")
        log.info(f"   Close {close:.2f}  SL {sl['price']:.2f}  "
                 f"SL-trig {sl_trigger:.2f}  T {target:.2f}  Setup {setup_size:.1f}pts")
        log.info("=" * 62)
        state.disarm('buy entry taken')
        self._place_order(candle, 'BUY', close, sl_trigger, target,
                          candle_idx, is_add=False,
                          pivot_price_used=sl['price'], setup_size=setup_size)
        return 'BUY_ENTRY'

    # ── Add to position ────────────────────────────────────────────────────

    def _maybe_add(self, candle, candle_idx, close, pivots):
        state = self.state
        current_lots = state.total_qty // LOT_SIZE if LOT_SIZE else 0
        if current_lots >= MAX_TOTAL_LOTS:
            return
        vwap = self._last_vwap
        if vwap is not None:
            if state.direction == 'BUY'  and close < vwap: return
            if state.direction == 'SELL' and close > vwap: return
        origin_price = state.target_price
        origin_idx   = state.entry_candle if state.entry_candle is not None else candle_idx

        if state.direction == 'SELL':
            sh = state.sh_registry.next_entry_level(
                close=close, origin_idx=origin_idx, origin_price=origin_price,
                last_used_price=state.last_added_price + 0.0001,
                used_prices=state.used_prices_in_trade, vwap=self._last_vwap)
            if sh is None:
                return
            if state.last_added_price > 0:
                dist = sh['price'] - state.last_added_price
                if dist < MIN_ADD_DIST:
                    log.info(f"[ADD-SKIP] SELL  SH {sh['price']:.2f} too close to last "
                             f"{state.last_added_price:.2f}  (dist {dist:.1f} < {MIN_ADD_DIST})")
                    return
            new_sl = sh['price'] + SL_BUFFER
            log.info(f"[ADD-SELL] SH {sh['price']:.2f}  add {ADD_LOTS}L @ {close:.2f}  "
                     f"new SL:{new_sl:.2f}")
            self._place_order(candle, 'SELL', close, new_sl, state.target_price,
                              candle_idx, is_add=True,
                              pivot_price_used=sh['price'], setup_size=0.0)

        elif state.direction == 'BUY':
            sl = state.sl_registry.next_entry_level(
                close=close, origin_idx=origin_idx, origin_price=origin_price,
                last_used_price=state.last_added_price - 0.0001,
                used_prices=state.used_prices_in_trade, vwap=self._last_vwap)
            if sl is None:
                return
            if state.last_added_price > 0:
                dist = state.last_added_price - sl['price']
                if dist < MIN_ADD_DIST:
                    log.info(f"[ADD-SKIP] BUY   SL {sl['price']:.2f} too close to last "
                             f"{state.last_added_price:.2f}  (dist {dist:.1f} < {MIN_ADD_DIST})")
                    return
            new_sl = sl['price'] - SL_BUFFER
            log.info(f"[ADD-BUY]  SL {sl['price']:.2f}  add {ADD_LOTS}L @ {close:.2f}  "
                     f"new SL:{new_sl:.2f}")
            self._place_order(candle, 'BUY', close, new_sl, state.target_price,
                              candle_idx, is_add=True,
                              pivot_price_used=sl['price'], setup_size=0.0)

    # ── Exit check ─────────────────────────────────────────────────────────

    def _check_exit(self, candle) -> str:
        close = candle['close']
        state = self.state
        if state.direction == 'SELL':
            if close >= state.sl_trigger:
                log.warning(f"[SL] SELL  close {close:.2f} >= SL {state.sl_trigger:.2f}")
                self._exit(candle['dt'], close, 'SL')
                return 'SL_EXIT'
            if close <= state.target_price:
                log.info(f"[TARGET] SELL  close {close:.2f} <= T {state.target_price:.2f}")
                self._exit(candle['dt'], close, 'TARGET')
                return 'TARGET_EXIT'
        elif state.direction == 'BUY':
            if close <= state.sl_trigger:
                log.warning(f"[SL] BUY  close {close:.2f} <= SL {state.sl_trigger:.2f}")
                self._exit(candle['dt'], close, 'SL')
                return 'SL_EXIT'
            if close >= state.target_price:
                log.info(f"[TARGET] BUY  close {close:.2f} >= T {state.target_price:.2f}")
                self._exit(candle['dt'], close, 'TARGET')
                return 'TARGET_EXIT'
        return 'HOLDING'

    def check_tick_exit(self, price: float):
        s = self.state
        if s.status != TradeState.OPEN:
            return
        if s.day_bias == 'BEARISH' and s.orb_high and price > s.orb_high:
            log.warning(f"[INVALIDATION-TICK] ORB HIGH {price:.2f} > {s.orb_high:.2f}")
            self._exit(datetime.datetime.now(IST).replace(tzinfo=None),
                       price, 'ORB_INVALIDATION')
            s.disarm('ORB high broken tick')
            return
        if s.day_bias == 'BULLISH' and s.orb_low and price < s.orb_low:
            log.warning(f"[INVALIDATION-TICK] ORB LOW {price:.2f} < {s.orb_low:.2f}")
            self._exit(datetime.datetime.now(IST).replace(tzinfo=None),
                       price, 'ORB_INVALIDATION')
            s.disarm('ORB low broken tick')
            return
        if s.direction == 'SELL' and price <= s.target_price:
            self._exit(datetime.datetime.now(IST).replace(tzinfo=None),
                       price, 'TARGET_TICK')
        elif s.direction == 'BUY' and price >= s.target_price:
            self._exit(datetime.datetime.now(IST).replace(tzinfo=None),
                       price, 'TARGET_TICK')

    def square_off(self, price: float):
        if self.state.status == TradeState.OPEN:
            log.info(f"[SQOFF] Forced square-off at {price:.2f}")
            self._exit(datetime.datetime.now(IST).replace(tzinfo=None),
                       price, 'SQUARE_OFF')
        self.state.disarm('square-off time')

    # ── Order placement ────────────────────────────────────────────────────

    def _place_order(self, candle, direction: str, entry: float,
                     sl_trigger: float, target: float,
                     candle_idx: int, is_add: bool,
                     pivot_price_used: float, setup_size: float):
        state = self.state
        qty   = (ADD_LOTS * LOT_SIZE) if is_add else (LOTS * LOT_SIZE)
        if not self.paper and self.kite:
            try:
                kite_tx = (self.kite.TRANSACTION_TYPE_SELL
                           if direction == 'SELL'
                           else self.kite.TRANSACTION_TYPE_BUY)
                oid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=EXCHANGE_FUT,
                    tradingsymbol=self.symbol,
                    transaction_type=kite_tx,
                    quantity=qty, product=PRODUCT,
                    order_type=self.kite.ORDER_TYPE_MARKET)
                log.info(f"[ORDER] {direction}:{oid}  qty:{qty}")
            except Exception as e:
                log.error(f"[ERR] Order failed: {e}")
                return
        else:
            log.info(f"[PAPER] {'ADD' if is_add else 'ENTRY'}  {direction}  qty:{qty}")

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
            state.last_added_price = pivot_price_used
        else:
            if state.avg_entry_price is None or state.total_qty == 0:
                state.avg_entry_price = entry
                state.total_qty = qty
            else:
                new_total = state.total_qty + qty
                state.avg_entry_price = (
                    (state.avg_entry_price * state.total_qty + entry * qty) / new_total)
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

    def _exit(self, dt, price: float, reason: str):
        state = self.state
        qty   = state.total_qty
        if qty <= 0:
            state.reset_trade()
            return
        if not self.paper and self.kite:
            try:
                kite_tx = (self.kite.TRANSACTION_TYPE_BUY
                           if state.direction == 'SELL'
                           else self.kite.TRANSACTION_TYPE_SELL)
                xid = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=EXCHANGE_FUT,
                    tradingsymbol=self.symbol,
                    transaction_type=kite_tx,
                    quantity=qty, product=PRODUCT,
                    order_type=self.kite.ORDER_TYPE_MARKET)
                log.info(f"[ORDER] exit:{xid}  qty:{qty}")
            except Exception as e:
                log.error(f"[ERR] Exit order failed: {e}  — EXIT MANUALLY")
        if state.direction == 'SELL':
            pnl = (state.avg_entry_price - price) * qty
        else:
            pnl = (price - state.avg_entry_price) * qty
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
        db_log_trade(len(state.trade_log), trade_record, state.pnl_today,
                     self.mode, self.symbol)
        log.info("=" * 62)
        log.info(f"[EXIT] {state.direction}  reason={reason}  bias={state.day_bias}")
        log.info(f"   AvgEntry {state.avg_entry_price:.2f} -> Exit {price:.2f}")
        log.info(f"   Qty {qty}   PnL Rs.{pnl:+,.0f}   Day PnL Rs.{state.pnl_today:+,.0f}")
        log.info("=" * 62)
        state.reset_trade()

# =============================================================================
#  KITE DATA FETCH
# =============================================================================

def fetch_candles(kite, instrument_token, from_date=None, to_date=None, live=False):
    now = datetime.datetime.now(IST)
    if live:
        from_dt = now.replace(hour=9, minute=15, second=0, microsecond=0, tzinfo=IST)
        to_dt   = now.replace(tzinfo=IST)
    else:
        fd = from_date or FROM_DATE
        td = to_date   or TO_DATE
        from_dt = datetime.datetime.strptime(fd, "%Y-%m-%d").replace(
                      hour=9, minute=15, tzinfo=IST)
        to_dt   = datetime.datetime.strptime(td, "%Y-%m-%d").replace(
                      hour=15, minute=30, tzinfo=IST)
        if to_dt.date() >= now.date():
            to_dt = now.replace(tzinfo=IST)

    log.info(f"Fetching {INTERVAL}  "
             f"{from_dt.strftime('%d %b %Y %H:%M')} -> {to_dt.strftime('%H:%M')}")
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
    log.info(f"Fetched {len(out)} candles  ({days[0]} -> {days[-1]})" if days
             else "No candles returned")
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

        mode_str = 'PAPER' if PAPER_TRADE else 'LIVE'
        self.strategy = StrategyCore(
            self.state, paper=PAPER_TRADE, kite=kite,
            symbol=self.tradingsymbol, mode=mode_str)

        log.info(f"[MODE] {mode_str}  {self.tradingsymbol}  Expiry:{self.expiry}")

        # ── Preload today's candles ────────────────────────────────────────
        log.info("[PRELOAD] Fetching today's candles...")
        try:
            candles = fetch_candles(kite, self.instrument_token, live=True)
            if not candles:
                log.warning("[PRELOAD] No candles — starting fresh")
            else:
                sq_time = datetime.time(*SQUARE_OFF_TIME)
                for c in candles[:-1]:
                    self.builder.completed.append(c)
                last = candles[-1]
                self.builder.current = {
                    'dt': last['dt'], 'open': last['open'],
                    'high': last['high'], 'low': last['low'],
                    'close': last['close'], 'volume': last.get('volume', 0)
                }
                log.info(f"[PRELOAD] Seeded forming candle "
                         f"{last['dt'].strftime('%H:%M')}  C:{last['close']}")
                completed = self.builder.get_all()
                log.info(f"[PRELOAD] Replaying {len(completed)} candles...")
                for i in range(len(completed)):
                    if completed[i]['dt'].time() >= sq_time:
                        break
                    if i < PIVOT_LEFT + PIVOT_RIGHT + 1:
                        continue
                    self.strategy.on_candle_close(completed, i)
                log.info(f"[PRELOAD] Done  bias={self.state.day_bias}  "
                         f"pos={self.state.status}  "
                         f"trades={self.state.trades_today}  "
                         f"PnL=Rs.{self.state.pnl_today:+,.0f}")
        except Exception as e:
            log.warning(f"[PRELOAD] Failed: {e}")
            import traceback; log.warning(traceback.format_exc())

        # ── WebSocket ─────────────────────────────────────────────────────
        self.ticker = KiteTicker(API_KEY, ACCESS_TOKEN)
        self.ticker.on_connect = self._on_connect
        self.ticker.on_ticks   = self._on_ticks
        self.ticker.on_close   = self._on_close
        self.ticker.on_error   = lambda ws, c, r: log.error(f"WS error: {r}")

    def _on_close(self, ws, code, reason):
        log.warning(f"[WS] Closed (code={code}): {reason}")
        if datetime.datetime.now(IST).time() < datetime.time(*SQUARE_OFF_TIME):
            log.info("[WS] Reconnecting in 5 s...")
            time.sleep(5)
            try:
                self.ticker.connect(threaded=True)
            except Exception as e:
                log.error(f"[WS] Reconnect failed: {e}")

    def _on_connect(self, ws, _):
        log.info(f"[WS] Connected — subscribing token {self.instrument_token}")
        ws.subscribe([self.instrument_token])
        ws.set_mode(ws.MODE_QUOTE, [self.instrument_token])

    def _on_ticks(self, ws, ticks):
        for t in ticks:
            if t['instrument_token'] != self.instrument_token:
                continue
            with self._lock:
                price = (t.get('last_price')
                         or t.get('last_traded_price')
                         or t.get('close', 0))
                if not price:
                    continue
                volume = t.get('volume_traded', 0) or t.get('volume', 0) or 0
                self._process(price, datetime.datetime.now(IST), volume)

    def _process(self, price, ts, volume: int = 0):
        if ts.time() >= datetime.time(*SQUARE_OFF_TIME):
            if self.state.status == TradeState.OPEN:
                self.strategy.square_off(price)
            return
        if ts.time() < datetime.time(9, 15):
            return
        closed = self.builder.on_tick(price, ts, volume)
        if closed:
            candles = self.builder.get_all()
            self.strategy.on_candle_close(candles, len(candles) - 1)
        if self.state.status == TradeState.OPEN:
            self.strategy.check_tick_exit(price)

    def run(self):
        log.info("[BOT] Starting live feed...")
        self.ticker.connect(threaded=True)
        try:
            while True:
                time.sleep(30)
                now = datetime.datetime.now(IST)
                if (now.time() > datetime.time(*SQUARE_OFF_TIME)
                        and self.state.status == TradeState.IDLE):
                    log.info(f"[BOT] Session done  "
                             f"trades:{self.state.trades_today}  "
                             f"PnL:Rs.{self.state.pnl_today:+,.0f}")
                    break
        except KeyboardInterrupt:
            if self.state.status == TradeState.OPEN:
                log.warning("[BOT] Open position — EXIT MANUALLY ON KITE")
        finally:
            self.ticker.close()

# =============================================================================
#  REPLAY
# =============================================================================

def run_replay(kite, from_date: str = None, to_date: str = None):
    token, tsym, exp = get_current_month_nifty_future(kite)
    fd = from_date or FROM_DATE
    td = to_date   or TO_DATE
    log.info("=" * 62)
    log.info(f"  REPLAY  {fd} -> {td}  [{INTERVAL}]   {tsym}  (Exp:{exp})")
    log.info("=" * 62)
    try:
        all_candles = fetch_candles(kite, token, from_date=fd, to_date=td, live=False)
    except Exception as e:
        log.error(f"Fetch failed: {e}"); return
    if not all_candles:
        log.error("No candles returned"); return

    state    = TradeState()
    strategy = StrategyCore(state, paper=True, kite=kite, symbol=tsym, mode='REPLAY')
    sq_time  = datetime.time(*SQUARE_OFF_TIME)

    for i, candle in enumerate(all_candles):
        if candle['dt'].time() >= sq_time:
            if state.status == TradeState.OPEN:
                strategy.square_off(candle['close'])
            break
        if i < PIVOT_LEFT + PIVOT_RIGHT + 1:
            continue
        strategy.on_candle_close(all_candles[:i + 1], i)
        if REPLAY_DELAY_S > 0:
            time.sleep(REPLAY_DELAY_S)

    log.info("=" * 62)
    log.info("  REPLAY COMPLETE")
    log.info(f"  Day Bias : {state.day_bias}   Algo Dead : {state.algo_dead}")
    log.info(f"  Trades   : {len(state.trade_log)}   PnL : Rs.{state.pnl_today:+,.0f}")
    log.info("=" * 62)
    for i, t in enumerate(state.trade_log, 1):
        et = t['exit_dt'].strftime('%H:%M') if hasattr(t['exit_dt'], 'strftime') else ''
        log.info(
            f"  [{i}] {t['direction']}({t.get('bias','?')})  "
            f"in:{t['entry_dt'].strftime('%H:%M')}@{t['entry_price']:.2f}  "
            f"out:{et}@{t['exit_price']:.2f}  "
            f"{t['reason']}  Rs.{t['pnl']:+,.0f}  qty:{t.get('qty',0)}"
        )
    log.info("=" * 62)

# =============================================================================
#  CONFIG PRINT
# =============================================================================

def print_config(symbol="NIFTYFUT"):
    log.info("=" * 62)
    log.info("  NIFTY FUT  ORB Breakout — Bullish + Bearish  [PROD]")
    log.info("=" * 62)
    log.info(f"  Symbol          : {symbol} ({EXCHANGE_FUT})")
    log.info(f"  Interval        : {INTERVAL}")
    log.info(f"  ORB window      : 9:15 - 9:{15+ORB_MINUTES:02d}  ({ORB_MINUTES} bars)")
    log.info(f"  Pivot L/R       : {PIVOT_LEFT} / {PIVOT_RIGHT}")
    log.info(f"  Min pivot gap   : {MIN_PIVOT_DIST} pts")
    log.info(f"  ORB break buf   : {BREAK_BUFFER} pts")
    log.info(f"  Retest buffer   : {RETEST_BUFFER} pts")
    log.info(f"  Min setup size  : {MIN_SETUP_POINTS} pts")
    log.info(f"  SL buffer       : {SL_BUFFER} pts")
    log.info(f"  Max SL dist     : {MAX_SL_POINTS} pts")
    log.info(f"  Lots/Add/Max    : {LOTS} / {ADD_LOTS} / {MAX_TOTAL_LOTS}")
    log.info(f"  Min add dist    : {MIN_ADD_DIST} pts")
    log.info(f"  Max trades/day  : {MAX_TRADES_DAY}")
    log.info(f"  Square-off      : {SQUARE_OFF_TIME[0]:02d}:{SQUARE_OFF_TIME[1]:02d} IST")
    log.info(f"  Paper trade     : {PAPER_TRADE}")
    log.info(f"  DB              : Railway PostgreSQL (DATABASE_URL)")
    log.info(f"  Log file        : {LOG_FILE}")
    log.info("=" * 62)

# =============================================================================
#  ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    env_date      = os.environ.get('REPLAY_DATE', '').strip()
    env_from_date = os.environ.get('REPLAY_FROM_DATE', '').strip()
    env_to_date   = os.environ.get('REPLAY_TO_DATE', '').strip()

    def _validate_date(val, name):
        try:
            datetime.datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            log.error(f"Env var {name}={val!r} must be YYYY-MM-DD")
            sys.exit(1)

    if env_date:      _validate_date(env_date,      'REPLAY_DATE')
    if env_from_date: _validate_date(env_from_date, 'REPLAY_FROM_DATE')
    if env_to_date:   _validate_date(env_to_date,   'REPLAY_TO_DATE')

    if (not API_KEY or not ACCESS_TOKEN
            or "PUT_YOUR" in API_KEY or "PUT_YOUR" in ACCESS_TOKEN):
        print("\nSet API_KEY and ACCESS_TOKEN at the top of this file.\n")
        sys.exit(1)

    # Connect to Railway PostgreSQL and create tables if they don't exist
    init_db()

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    try:
        token, tsym, exp = get_current_month_nifty_future(kite)
    except Exception as e:
        log.error(f"Failed to resolve NIFTY FUT: {e}")
        sys.exit(1)

    print_config(tsym)

    now     = datetime.datetime.now(IST).time()
    is_live = datetime.time(9, 15) <= now <= datetime.time(15, 30)

    if env_date:
        log.info(f"Mode: REPLAY  date={env_date}")
        run_replay(kite, from_date=env_date, to_date=env_date)

    elif env_from_date or env_to_date:
        fd = env_from_date or FROM_DATE
        td = env_to_date   or TO_DATE
        log.info(f"Mode: REPLAY  {fd} -> {td}")
        run_replay(kite, from_date=fd, to_date=td)

    elif is_live:
        log.info("Mode: LIVE")
        LiveBot(kite).run()

    else:
        log.info(f"Mode: REPLAY (market closed  {now.strftime('%H:%M')} IST)")
        run_replay(kite)
