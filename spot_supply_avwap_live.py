from __future__ import annotations

import gzip
import json
import logging
import os
import queue
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import requests
import upstox_client

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
CONNECT_TIME = dt_time(9, 10)
BREAKOUT_END = dt_time(10, 15)
ENTRY_CUTOFF = dt_time(12, 15)

ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
D_SLOPE = float(os.getenv("D_SLOPE", "0.69"))
D_INTERCEPT = float(os.getenv("D_INTERCEPT", "0.0"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "0.50"))
PHI = 1.61803398875
SQRT_252 = 252 ** 0.5

DATABASE_URL = (os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
ACCESS_TOKEN = (os.getenv("UPSTOX_ACCESS_TOKEN") or os.getenv("UPSTOX_TOKEN") or "").strip()
INSTRUMENTS_URL = os.getenv(
    "UPSTOX_INSTRUMENTS_URL",
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz",
)
HISTORY_BASE = "https://api.upstox.com/v3/historical-candle"
QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"
OPTION_CONTRACT_URL = "https://api.upstox.com/v2/option/contract"

NSE_HOLIDAYS_2026 = {
    date(2026, 1, 26), date(2026, 3, 3), date(2026, 3, 26), date(2026, 3, 31),
    date(2026, 4, 3), date(2026, 4, 14), date(2026, 5, 1), date(2026, 5, 28),
    date(2026, 6, 26), date(2026, 8, 26), date(2026, 9, 14), date(2026, 10, 2),
    date(2026, 10, 20), date(2026, 11, 10), date(2026, 11, 24), date(2026, 12, 25),
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("spot-supply-avwap-live")


def extra_holidays() -> set[date]:
    out = set()
    for item in os.getenv("NSE_EXTRA_HOLIDAYS", "").split(","):
        item = item.strip()
        if item:
            try:
                out.add(date.fromisoformat(item))
            except ValueError:
                LOG.warning("Ignoring invalid holiday: %s", item)
    return out


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in NSE_HOLIDAYS_2026 and day not in extra_holidays()


def db_connect():
    return psycopg.connect(DATABASE_URL, autocommit=False)


DDL = """
CREATE TABLE IF NOT EXISTS public.spot_supply_avwap_live (
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_instrument_key TEXT NOT NULL,

    day_open NUMERIC,
    atr14 NUMERIC,
    strong_supply_low NUMERIC,
    strong_supply_high NUMERIC,

    breakout_1015 BOOLEAN NOT NULL DEFAULT FALSE,
    breakout_close NUMERIC,
    breakout_pct NUMERIC,
    breakout_confirmed_at TIMESTAMPTZ,

    anchor_0915_low NUMERIC,
    avwap NUMERIC,
    last_3m_close NUMERIC,
    last_3m_low NUMERIC,
    last_3m_end TIMESTAMPTZ,

    retrace_found BOOLEAN NOT NULL DEFAULT FALSE,
    retrace_at TIMESTAMPTZ,
    retrace_low NUMERIC,
    avwap_at_retrace NUMERIC,

    entry_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    entry_time TIMESTAMPTZ,
    entry_price NUMERIC,
    avwap_at_entry NUMERIC,
    target_price NUMERIC,
    target_hit BOOLEAN NOT NULL DEFAULT FALSE,
    target_hit_at TIMESTAMPTZ,

    setup_state TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (trading_date, spot_instrument_key)
);

CREATE INDEX IF NOT EXISTS idx_spot_supply_avwap_live_state
ON public.spot_supply_avwap_live (trading_date, setup_state, entry_time);

CREATE TABLE IF NOT EXISTS public.spot_supply_avwap_3m (
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_instrument_key TEXT NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    candle_end TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    avwap NUMERIC,
    strong_supply_high NUMERIC,
    breakout_1015 BOOLEAN NOT NULL DEFAULT FALSE,
    retrace_now BOOLEAN NOT NULL DEFAULT FALSE,
    entry_now BOOLEAN NOT NULL DEFAULT FALSE,
    target_hit_now BOOLEAN NOT NULL DEFAULT FALSE,
    setup_state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trading_date, spot_instrument_key, candle_start)
);


CREATE TABLE IF NOT EXISTS public.spot_supply_avwap_option_live (
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    option_type TEXT NOT NULL,
    strike NUMERIC NOT NULL,
    expiry DATE NOT NULL,
    option_instrument_key TEXT NOT NULL,
    trading_symbol TEXT,

    spot_entry_time TIMESTAMPTZ NOT NULL,
    spot_entry_price NUMERIC NOT NULL,

    entry_option_price NUMERIC,
    entry_option_oi BIGINT,

    latest_ts TIMESTAMPTZ,
    latest_price NUMERIC,
    latest_oi BIGINT,
    cumulative_signed_volume_proxy BIGINT NOT NULL DEFAULT 0,

    premium_change_pct NUMERIC,
    oi_change_pct NUMERIC,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (trading_date, symbol, option_type)
);

CREATE TABLE IF NOT EXISTS public.spot_supply_avwap_option_1m (
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    option_type TEXT NOT NULL,
    strike NUMERIC NOT NULL,
    expiry DATE NOT NULL,
    option_instrument_key TEXT NOT NULL,

    spot_entry_time TIMESTAMPTZ NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    candle_end TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,
    oi BIGINT,

    signed_volume_proxy BIGINT NOT NULL DEFAULT 0,
    cumulative_signed_volume_proxy BIGINT NOT NULL DEFAULT 0,

    premium_change_from_entry_pct NUMERIC,
    oi_change_from_entry_pct NUMERIC,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (trading_date, symbol, option_type, candle_start)
);

CREATE TABLE IF NOT EXISTS public.spot_supply_avwap_option_score (
    trading_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    spot_entry_time TIMESTAMPTZ NOT NULL,

    ce_strike NUMERIC,
    pe_strike NUMERIC,
    expiry DATE,

    score_5m INTEGER,
    ce_premium_up_5m BOOLEAN,
    ce_oi_down_5m BOOLEAN,
    pe_premium_down_5m BOOLEAN,
    pe_oi_up_5m BOOLEAN,
    bullish_proxy_5m BOOLEAN,

    option_entry_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    option_entry_time TIMESTAMPTZ,
    option_entry_score INTEGER,
    option_entry_ce_price NUMERIC,
    option_entry_pe_price NUMERIC,
    option_entry_spot_price NUMERIC,
    option_entry_spot_target NUMERIC,
    option_entry_spot_stop NUMERIC,
    score_15m INTEGER,
    ce_premium_up_15m BOOLEAN,
    ce_oi_down_15m BOOLEAN,
    pe_premium_down_15m BOOLEAN,
    pe_oi_up_15m BOOLEAN,
    bullish_proxy_15m BOOLEAN,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (trading_date, symbol)
);



ALTER TABLE public.spot_supply_avwap_option_score ADD COLUMN IF NOT EXISTS option_entry_confirmed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE public.spot_supply_avwap_option_score ADD COLUMN IF NOT EXISTS option_entry_time TIMESTAMPTZ;
ALTER TABLE public.spot_supply_avwap_option_score ADD COLUMN IF NOT EXISTS option_entry_score INTEGER;
ALTER TABLE public.spot_supply_avwap_option_score ADD COLUMN IF NOT EXISTS option_entry_ce_price NUMERIC;
ALTER TABLE public.spot_supply_avwap_option_score ADD COLUMN IF NOT EXISTS option_entry_pe_price NUMERIC;
ALTER TABLE public.spot_supply_avwap_option_score ADD COLUMN IF NOT EXISTS option_entry_spot_price NUMERIC;
ALTER TABLE public.spot_supply_avwap_option_score ADD COLUMN IF NOT EXISTS option_entry_spot_target NUMERIC;
ALTER TABLE public.spot_supply_avwap_option_score ADD COLUMN IF NOT EXISTS option_entry_spot_stop NUMERIC;

CREATE TABLE IF NOT EXISTS public.spot_supply_avwap_heartbeat (
    service_name TEXT PRIMARY KEY,
    trading_date DATE,
    last_tick_at TIMESTAMPTZ,
    instruments INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_LIVE = """
INSERT INTO public.spot_supply_avwap_live (
 trading_date,symbol,spot_instrument_key,day_open,atr14,strong_supply_low,strong_supply_high,
 breakout_1015,breakout_close,breakout_pct,breakout_confirmed_at,
 anchor_0915_low,avwap,last_3m_close,last_3m_low,last_3m_end,
 retrace_found,retrace_at,retrace_low,avwap_at_retrace,
 entry_confirmed,entry_time,entry_price,avwap_at_entry,target_price,target_hit,target_hit_at,
 setup_state
) VALUES (
 %(trading_date)s,%(symbol)s,%(spot_instrument_key)s,%(day_open)s,%(atr14)s,%(strong_supply_low)s,%(strong_supply_high)s,
 %(breakout_1015)s,%(breakout_close)s,%(breakout_pct)s,%(breakout_confirmed_at)s,
 %(anchor_0915_low)s,%(avwap)s,%(last_3m_close)s,%(last_3m_low)s,%(last_3m_end)s,
 %(retrace_found)s,%(retrace_at)s,%(retrace_low)s,%(avwap_at_retrace)s,
 %(entry_confirmed)s,%(entry_time)s,%(entry_price)s,%(avwap_at_entry)s,%(target_price)s,%(target_hit)s,%(target_hit_at)s,
 %(setup_state)s
)
ON CONFLICT (trading_date,spot_instrument_key) DO UPDATE SET
 day_open=EXCLUDED.day_open,atr14=EXCLUDED.atr14,strong_supply_low=EXCLUDED.strong_supply_low,
 strong_supply_high=EXCLUDED.strong_supply_high,breakout_1015=EXCLUDED.breakout_1015,
 breakout_close=EXCLUDED.breakout_close,breakout_pct=EXCLUDED.breakout_pct,
 breakout_confirmed_at=EXCLUDED.breakout_confirmed_at,anchor_0915_low=EXCLUDED.anchor_0915_low,
 avwap=EXCLUDED.avwap,last_3m_close=EXCLUDED.last_3m_close,last_3m_low=EXCLUDED.last_3m_low,last_3m_end=EXCLUDED.last_3m_end,
 retrace_found=EXCLUDED.retrace_found,retrace_at=EXCLUDED.retrace_at,retrace_low=EXCLUDED.retrace_low,
 avwap_at_retrace=EXCLUDED.avwap_at_retrace,entry_confirmed=EXCLUDED.entry_confirmed,
 entry_time=EXCLUDED.entry_time,entry_price=EXCLUDED.entry_price,avwap_at_entry=EXCLUDED.avwap_at_entry,
 target_price=EXCLUDED.target_price,target_hit=EXCLUDED.target_hit,target_hit_at=EXCLUDED.target_hit_at,
 setup_state=EXCLUDED.setup_state,updated_at=NOW();
"""

UPSERT_3M = """
INSERT INTO public.spot_supply_avwap_3m (
 trading_date,symbol,spot_instrument_key,candle_start,candle_end,open,high,low,close,volume,avwap,
 strong_supply_high,breakout_1015,retrace_now,entry_now,target_hit_now,setup_state
) VALUES (
 %(trading_date)s,%(symbol)s,%(spot_instrument_key)s,%(candle_start)s,%(candle_end)s,
 %(open)s,%(high)s,%(low)s,%(close)s,%(volume)s,%(avwap)s,%(strong_supply_high)s,
 %(breakout_1015)s,%(retrace_now)s,%(entry_now)s,%(target_hit_now)s,%(setup_state)s
)
ON CONFLICT (trading_date,spot_instrument_key,candle_start) DO UPDATE SET
 high=GREATEST(public.spot_supply_avwap_3m.high,EXCLUDED.high),
 low=LEAST(public.spot_supply_avwap_3m.low,EXCLUDED.low),
 close=EXCLUDED.close,volume=EXCLUDED.volume,avwap=EXCLUDED.avwap,
 retrace_now=EXCLUDED.retrace_now,entry_now=EXCLUDED.entry_now,target_hit_now=EXCLUDED.target_hit_now,
 setup_state=EXCLUDED.setup_state;
"""


UPSERT_OPTION_LIVE = """
INSERT INTO public.spot_supply_avwap_option_live (
 trading_date,symbol,option_type,strike,expiry,option_instrument_key,trading_symbol,
 spot_entry_time,spot_entry_price,entry_option_price,entry_option_oi,
 latest_ts,latest_price,latest_oi,cumulative_signed_volume_proxy,
 premium_change_pct,oi_change_pct
) VALUES (
 %(trading_date)s,%(symbol)s,%(option_type)s,%(strike)s,%(expiry)s,%(option_instrument_key)s,%(trading_symbol)s,
 %(spot_entry_time)s,%(spot_entry_price)s,%(entry_option_price)s,%(entry_option_oi)s,
 %(latest_ts)s,%(latest_price)s,%(latest_oi)s,%(cumulative_signed_volume_proxy)s,
 %(premium_change_pct)s,%(oi_change_pct)s
)
ON CONFLICT(trading_date,symbol,option_type) DO UPDATE SET
 strike=EXCLUDED.strike,expiry=EXCLUDED.expiry,option_instrument_key=EXCLUDED.option_instrument_key,
 trading_symbol=EXCLUDED.trading_symbol,spot_entry_time=EXCLUDED.spot_entry_time,
 spot_entry_price=EXCLUDED.spot_entry_price,entry_option_price=EXCLUDED.entry_option_price,
 entry_option_oi=EXCLUDED.entry_option_oi,latest_ts=EXCLUDED.latest_ts,
 latest_price=EXCLUDED.latest_price,latest_oi=EXCLUDED.latest_oi,
 cumulative_signed_volume_proxy=EXCLUDED.cumulative_signed_volume_proxy,
 premium_change_pct=EXCLUDED.premium_change_pct,oi_change_pct=EXCLUDED.oi_change_pct,
 updated_at=NOW();
"""

UPSERT_OPTION_1M = """
INSERT INTO public.spot_supply_avwap_option_1m (
 trading_date,symbol,option_type,strike,expiry,option_instrument_key,spot_entry_time,
 candle_start,candle_end,open,high,low,close,volume,oi,
 signed_volume_proxy,cumulative_signed_volume_proxy,
 premium_change_from_entry_pct,oi_change_from_entry_pct
) VALUES (
 %(trading_date)s,%(symbol)s,%(option_type)s,%(strike)s,%(expiry)s,%(option_instrument_key)s,%(spot_entry_time)s,
 %(candle_start)s,%(candle_end)s,%(open)s,%(high)s,%(low)s,%(close)s,%(volume)s,%(oi)s,
 %(signed_volume_proxy)s,%(cumulative_signed_volume_proxy)s,
 %(premium_change_from_entry_pct)s,%(oi_change_from_entry_pct)s
)
ON CONFLICT(trading_date,symbol,option_type,candle_start) DO UPDATE SET
 high=GREATEST(public.spot_supply_avwap_option_1m.high,EXCLUDED.high),
 low=LEAST(public.spot_supply_avwap_option_1m.low,EXCLUDED.low),
 close=EXCLUDED.close,volume=EXCLUDED.volume,oi=EXCLUDED.oi,
 signed_volume_proxy=EXCLUDED.signed_volume_proxy,
 cumulative_signed_volume_proxy=EXCLUDED.cumulative_signed_volume_proxy,
 premium_change_from_entry_pct=EXCLUDED.premium_change_from_entry_pct,
 oi_change_from_entry_pct=EXCLUDED.oi_change_from_entry_pct;
"""

UPSERT_OPTION_SCORE = """
INSERT INTO public.spot_supply_avwap_option_score (
 trading_date,symbol,spot_entry_time,ce_strike,pe_strike,expiry,
 option_entry_confirmed,option_entry_time,option_entry_score,option_entry_ce_price,option_entry_pe_price,
 option_entry_spot_price,option_entry_spot_target,option_entry_spot_stop,
 score_5m,ce_premium_up_5m,ce_oi_down_5m,pe_premium_down_5m,pe_oi_up_5m,bullish_proxy_5m,
 score_15m,ce_premium_up_15m,ce_oi_down_15m,pe_premium_down_15m,pe_oi_up_15m,bullish_proxy_15m
) VALUES (
 %(trading_date)s,%(symbol)s,%(spot_entry_time)s,%(ce_strike)s,%(pe_strike)s,%(expiry)s,
 %(option_entry_confirmed)s,%(option_entry_time)s,%(option_entry_score)s,%(option_entry_ce_price)s,%(option_entry_pe_price)s,
 %(option_entry_spot_price)s,%(option_entry_spot_target)s,%(option_entry_spot_stop)s,
 %(score_5m)s,%(ce_premium_up_5m)s,%(ce_oi_down_5m)s,%(pe_premium_down_5m)s,%(pe_oi_up_5m)s,%(bullish_proxy_5m)s,
 %(score_15m)s,%(ce_premium_up_15m)s,%(ce_oi_down_15m)s,%(pe_premium_down_15m)s,%(pe_oi_up_15m)s,%(bullish_proxy_15m)s
)
ON CONFLICT(trading_date,symbol) DO UPDATE SET
 option_entry_confirmed=public.spot_supply_avwap_option_score.option_entry_confirmed OR EXCLUDED.option_entry_confirmed,
 option_entry_time=COALESCE(public.spot_supply_avwap_option_score.option_entry_time,EXCLUDED.option_entry_time),
 option_entry_score=COALESCE(public.spot_supply_avwap_option_score.option_entry_score,EXCLUDED.option_entry_score),
 option_entry_ce_price=COALESCE(public.spot_supply_avwap_option_score.option_entry_ce_price,EXCLUDED.option_entry_ce_price),
 option_entry_pe_price=COALESCE(public.spot_supply_avwap_option_score.option_entry_pe_price,EXCLUDED.option_entry_pe_price),
 option_entry_spot_price=COALESCE(public.spot_supply_avwap_option_score.option_entry_spot_price,EXCLUDED.option_entry_spot_price),
 option_entry_spot_target=COALESCE(public.spot_supply_avwap_option_score.option_entry_spot_target,EXCLUDED.option_entry_spot_target),
 option_entry_spot_stop=COALESCE(public.spot_supply_avwap_option_score.option_entry_spot_stop,EXCLUDED.option_entry_spot_stop),
 score_5m=COALESCE(EXCLUDED.score_5m,public.spot_supply_avwap_option_score.score_5m),
 ce_premium_up_5m=COALESCE(EXCLUDED.ce_premium_up_5m,public.spot_supply_avwap_option_score.ce_premium_up_5m),
 ce_oi_down_5m=COALESCE(EXCLUDED.ce_oi_down_5m,public.spot_supply_avwap_option_score.ce_oi_down_5m),
 pe_premium_down_5m=COALESCE(EXCLUDED.pe_premium_down_5m,public.spot_supply_avwap_option_score.pe_premium_down_5m),
 pe_oi_up_5m=COALESCE(EXCLUDED.pe_oi_up_5m,public.spot_supply_avwap_option_score.pe_oi_up_5m),
 bullish_proxy_5m=COALESCE(EXCLUDED.bullish_proxy_5m,public.spot_supply_avwap_option_score.bullish_proxy_5m),
 score_15m=COALESCE(EXCLUDED.score_15m,public.spot_supply_avwap_option_score.score_15m),
 ce_premium_up_15m=COALESCE(EXCLUDED.ce_premium_up_15m,public.spot_supply_avwap_option_score.ce_premium_up_15m),
 ce_oi_down_15m=COALESCE(EXCLUDED.ce_oi_down_15m,public.spot_supply_avwap_option_score.ce_oi_down_15m),
 pe_premium_down_15m=COALESCE(EXCLUDED.pe_premium_down_15m,public.spot_supply_avwap_option_score.pe_premium_down_15m),
 pe_oi_up_15m=COALESCE(EXCLUDED.pe_oi_up_15m,public.spot_supply_avwap_option_score.pe_oi_up_15m),
 bullish_proxy_15m=COALESCE(EXCLUDED.bullish_proxy_15m,public.spot_supply_avwap_option_score.bullish_proxy_15m),
 updated_at=NOW();
"""



@dataclass(frozen=True)
class Instrument:
    symbol: str
    spot_key: str


@dataclass
class Zone:
    day_open: float
    atr: float
    strong_supply_low: float
    strong_supply_high: float


@dataclass
class Bar:
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


@dataclass
class State:
    instrument: Instrument
    zone: Zone
    bar3: Bar | None = None
    bar60: Bar | None = None
    last_cum_volume: int | None = None

    cum_pv: float = 0.0
    cum_v: float = 0.0
    anchor_low: float | None = None
    avwap: float | None = None

    breakout_1015: bool = False
    breakout_close: float | None = None
    breakout_pct: float | None = None
    breakout_at: datetime | None = None

    retrace_found: bool = False
    retrace_at: datetime | None = None
    retrace_low: float | None = None
    avwap_at_retrace: float | None = None

    entry_confirmed: bool = False
    entry_time: datetime | None = None
    entry_price: float | None = None
    avwap_at_entry: float | None = None
    target_price: float | None = None
    target_hit: bool = False
    target_hit_at: datetime | None = None
    last_spot_price: float | None = None

    lock: threading.Lock = field(default_factory=threading.Lock)



@dataclass
class OptionState:
    symbol: str
    option_type: str
    strike: float
    expiry: date
    option_key: str
    trading_symbol: str
    spot_entry_time: datetime
    spot_entry_price: float
    bar1: Bar | None = None
    last_cum_volume: int | None = None
    latest_oi: int | None = None
    entry_option_price: float | None = None
    entry_option_oi: int | None = None
    cumulative_signed_volume_proxy: int = 0
    score_5m_done: bool = False
    score_15m_done: bool = False
    option_entry_frozen: bool = False


def ensure_schema():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()


def clean_symbol(row):
    for k in ("underlying_symbol","asset_symbol","short_name","name","trading_symbol"):
        if row.get(k):
            return str(row[k]).strip().upper()
    return "UNKNOWN"


def load_master():
    r = requests.get(INSTRUMENTS_URL, timeout=45)
    r.raise_for_status()
    raw = r.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def discover_spots(master):
    out = []
    for row in master:
        segment = str(row.get("segment","")).upper()
        it = str(row.get("instrument_type","")).upper()
        if segment == "NSE_EQ" and it in {"EQ","BE"}:
            key = row.get("instrument_key")
            if key:
                out.append(Instrument(clean_symbol(row), str(key)))
    # Keep only symbols with stock futures available today.
    fut_symbols = set()
    today = datetime.now(IST).date()
    for row in master:
        segment = str(row.get("segment","")).upper()
        it = str(row.get("instrument_type","")).upper()
        if segment in {"NSE_FO","NSE_F&O","NFO"} and it in {"FUT","FUTSTK"}:
            exp = row.get("expiry")
            try:
                if isinstance(exp,(int,float)):
                    x=float(exp); x=x/1000 if x>10_000_000_000 else x
                    ed=datetime.fromtimestamp(x,tz=UTC).date()
                else:
                    ed=date.fromisoformat(str(exp)[:10])
                if ed >= today:
                    fut_symbols.add(clean_symbol(row))
            except Exception:
                pass
    d = {x.symbol:x for x in out if x.symbol in fut_symbols}
    return sorted(d.values(), key=lambda x:x.symbol)


def auth_headers():
    return {"Accept":"application/json","Authorization":f"Bearer {ACCESS_TOKEN}"}


def chunks(items, size=400):
    for i in range(0,len(items),size):
        yield items[i:i+size]


def get_quotes(keys):
    out={}
    for batch in chunks(keys):
        r=requests.get(QUOTE_URL,params={"instrument_key":",".join(batch)},headers=auth_headers(),timeout=30)
        r.raise_for_status()
        for q in r.json().get("data",{}).values():
            token=q.get("instrument_token")
            if token: out[str(token)]=q
    return out


def historical_daily(key, days=40):
    to_date=datetime.now(IST).date()-timedelta(days=1)
    from_date=to_date-timedelta(days=days*2)
    enc=requests.utils.quote(key,safe="")
    url=f"{HISTORY_BASE}/{enc}/days/1/{to_date.isoformat()}/{from_date.isoformat()}"
    r=requests.get(url,headers=auth_headers(),timeout=20); r.raise_for_status()
    a=[]
    for c in r.json().get("data",{}).get("candles",[]):
        if len(c)>=5:a.append({"ts":c[0],"high":float(c[2]),"low":float(c[3]),"close":float(c[4])})
    a.sort(key=lambda x:x["ts"])
    return a[-days:]


def wilder_atr(candles, period=ATR_PERIOD):
    trs=[]
    for i in range(1,len(candles)):
        h,l,pc=candles[i]["high"],candles[i]["low"],candles[i-1]["close"]
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    if len(trs)<period: raise ValueError("insufficient ATR history")
    atr=sum(trs[:period])/period
    for tr in trs[period:]:
        atr=((atr*(period-1))+tr)/period
    return atr


def calc_zone(day_open, atr, prev_close):
    ann=atr/prev_close*SQRT_252*100
    ev=D_SLOPE*ann + D_INTERCEPT
    p=round(day_open)
    sigma=p*ev/(100*SQRT_252)
    ws=round(sigma/4)
    return Zone(day_open,atr,round(p+sigma-ws/2),round(p+sigma+ws/2))


def build_zones(insts):
    quotes=get_quotes([i.spot_key for i in insts])
    zones={}
    for idx,i in enumerate(insts,1):
        try:
            q=quotes.get(i.spot_key,{})
            op=float((q.get("ohlc") or {}).get("open") or 0)
            if op<=0: raise ValueError("day open unavailable")
            daily=historical_daily(i.spot_key,40)
            atr=wilder_atr(daily)
            zones[i.spot_key]=calc_zone(op,atr,daily[-1]["close"])
        except Exception as exc:
            LOG.warning("%s zone skipped: %s",i.symbol,exc)
        if idx%20==0: time.sleep(.2)
    return zones


def deep_find(obj, keys):
    if isinstance(obj,dict):
        for k,v in obj.items():
            if str(k).lower() in keys and v is not None:return v
        for v in obj.values():
            f=deep_find(v,keys)
            if f is not None:return f
    elif isinstance(obj,list):
        for v in obj:
            f=deep_find(v,keys)
            if f is not None:return f
    return None


def extract_ticks(message):
    if isinstance(message,str): payload=json.loads(message)
    elif isinstance(message,dict): payload=message
    elif hasattr(message,"to_dict"): payload=message.to_dict()
    else: payload=getattr(message,"__dict__",{}) or {}
    feeds=payload.get("feeds") or payload.get("data",{}).get("feeds") or {}
    for key,feed in feeds.items():
        price=deep_find(feed,{"ltp","last_price","lastprice"})
        if price is None: continue
        epoch=deep_find(feed,{"ltt","last_trade_time","timestamp"})
        volume=deep_find(feed,{"vtt","volume_traded_today","volume"})
        oi=deep_find(feed,{"oi","open_interest"})
        try:
            raw=float(epoch); raw=raw/1000 if raw>10_000_000_000 else raw
            ts=datetime.fromtimestamp(raw,tz=UTC).astimezone(IST)
        except Exception: ts=datetime.now(IST)
        try: volume=int(float(volume)) if volume is not None else None
        except Exception: volume=None
        try: oi=int(float(oi)) if oi is not None else None
        except Exception: oi=None
        yield str(key),ts,float(price),volume,oi


def aligned_start(ts, minutes):
    day_open=datetime.combine(ts.date(),MARKET_OPEN,tzinfo=IST)
    m=int((ts-day_open).total_seconds()//60)
    return day_open+timedelta(minutes=(m//minutes)*minutes)


def setup_state(st, now):
    if not st.breakout_1015:
        return "NO_1015_BREAKOUT" if now.time().replace(tzinfo=None) >= BREAKOUT_END else "WAIT_1015_BREAKOUT"
    if st.entry_confirmed:
        return "TARGET_HIT" if st.target_hit else "ENTRY_CONFIRMED"
    if now.time().replace(tzinfo=None) > ENTRY_CUTOFF:
        return "ENTRY_WINDOW_CLOSED"
    if st.retrace_found:
        return "RETRACED_WAIT_CLOSE_ABOVE_AVWAP"
    return "WAIT_RETRACE_TO_AVWAP"


def live_row(st, bar3, now):
    return {
        "trading_date": now.date(),
        "symbol": st.instrument.symbol,
        "spot_instrument_key": st.instrument.spot_key,
        "day_open": st.zone.day_open,
        "atr14": st.zone.atr,
        "strong_supply_low": st.zone.strong_supply_low,
        "strong_supply_high": st.zone.strong_supply_high,
        "breakout_1015": st.breakout_1015,
        "breakout_close": st.breakout_close,
        "breakout_pct": st.breakout_pct,
        "breakout_confirmed_at": st.breakout_at,
        "anchor_0915_low": st.anchor_low,
        "avwap": st.avwap,
        "last_3m_close": bar3.close if bar3 else None,
        "last_3m_low": bar3.low if bar3 else None,
        "last_3m_end": bar3.start+timedelta(minutes=3) if bar3 else None,
        "retrace_found": st.retrace_found,
        "retrace_at": st.retrace_at,
        "retrace_low": st.retrace_low,
        "avwap_at_retrace": st.avwap_at_retrace,
        "entry_confirmed": st.entry_confirmed,
        "entry_time": st.entry_time,
        "entry_price": st.entry_price,
        "avwap_at_entry": st.avwap_at_entry,
        "target_price": st.target_price,
        "target_hit": st.target_hit,
        "target_hit_at": st.target_hit_at,
        "setup_state": setup_state(st, now),
    }



def option_contracts(spot_key: str) -> list[dict[str, Any]]:
    r = requests.get(
        OPTION_CONTRACT_URL,
        params={"instrument_key": spot_key},
        headers=auth_headers(),
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("data", []) or []


def choose_atm_pair(st: State) -> dict[str, OptionState]:
    if not st.entry_confirmed or st.entry_price is None or st.entry_time is None:
        return {}

    rows = option_contracts(st.instrument.spot_key)
    today = st.entry_time.date()
    valid = []
    for r in rows:
        try:
            exp = date.fromisoformat(str(r.get("expiry"))[:10])
            if exp < today:
                continue
            side = str(r.get("instrument_type") or r.get("option_type") or "").upper()
            if side not in {"CE", "PE"}:
                continue
            strike_raw = r.get("strike_price") if r.get("strike_price") is not None else r.get("strike")
            strike = float(strike_raw)
            key = r.get("instrument_key")
            if not key:
                continue
            valid.append((exp, side, strike, r))
        except Exception:
            continue

    if not valid:
        return {}

    expiry = min(x[0] for x in valid)
    out = {}
    for side in ("CE", "PE"):
        cands = [x for x in valid if x[0] == expiry and x[1] == side]
        if not cands:
            continue
        cands.sort(key=lambda x: (abs(x[2] - st.entry_price), x[2]))
        _, _, strike, r = cands[0]
        out[side] = OptionState(
            symbol=st.instrument.symbol,
            option_type=side,
            strike=strike,
            expiry=expiry,
            option_key=str(r["instrument_key"]),
            trading_symbol=str(r.get("trading_symbol") or ""),
            spot_entry_time=st.entry_time,
            spot_entry_price=float(st.entry_price),
        )
    return out


class Collector:
    def __init__(self, insts, zones):
        self.states={i.spot_key:State(i,zones[i.spot_key]) for i in insts if i.spot_key in zones}
        self.stop_event=threading.Event()
        self.streamer=None
        self.last_tick_at=None
        self.option_states: dict[str, OptionState] = {}
        self.option_by_symbol: dict[str, dict[str, OptionState]] = {}

    def heartbeat(self,status,msg=""):
        q="""INSERT INTO public.spot_supply_avwap_heartbeat(service_name,trading_date,last_tick_at,instruments,status,message)
        VALUES('spot_supply_avwap_live',%s,%s,%s,%s,%s)
        ON CONFLICT(service_name) DO UPDATE SET trading_date=EXCLUDED.trading_date,last_tick_at=EXCLUDED.last_tick_at,
        instruments=EXCLUDED.instruments,status=EXCLUDED.status,message=EXCLUDED.message,updated_at=NOW()"""
        try:
            with db_connect() as c:
                with c.cursor() as x:x.execute(q,(datetime.now(IST).date(),self.last_tick_at,len(self.states),status,msg[:500]))
                c.commit()
        except Exception: LOG.exception("heartbeat failed")

    def write(self, st, b3, retrace_now=False, entry_now=False, target_now=False):
        now=(b3.start+timedelta(minutes=3)) if b3 else datetime.now(IST)
        lr=live_row(st,b3,now)
        br=None
        if b3:
            br={
                "trading_date":now.date(),"symbol":st.instrument.symbol,"spot_instrument_key":st.instrument.spot_key,
                "candle_start":b3.start,"candle_end":now,"open":b3.open,"high":b3.high,"low":b3.low,"close":b3.close,
                "volume":b3.volume,"avwap":st.avwap,"strong_supply_high":st.zone.strong_supply_high,
                "breakout_1015":st.breakout_1015,"retrace_now":retrace_now,"entry_now":entry_now,
                "target_hit_now":target_now,"setup_state":lr["setup_state"],
            }
        with db_connect() as c:
            with c.cursor() as x:
                x.execute(UPSERT_LIVE,lr)
                if br:x.execute(UPSERT_3M,br)
            c.commit()

    def subscribe_atm_options(self, st: State):
        if st.instrument.symbol in self.option_by_symbol:
            return
        pair = choose_atm_pair(st)
        if set(pair) != {"CE", "PE"}:
            LOG.warning("%s ATM option pair unavailable", st.instrument.symbol)
            return
        self.option_by_symbol[st.instrument.symbol] = pair
        new_keys = []
        for os_ in pair.values():
            self.option_states[os_.option_key] = os_
            new_keys.append(os_.option_key)
        try:
            self.streamer.subscribe(new_keys, "full")
            LOG.info(
                "%s ATM options subscribed | CE %.2f | PE %.2f | exp %s",
                st.instrument.symbol, pair["CE"].strike, pair["PE"].strike, pair["CE"].expiry
            )
        except Exception:
            LOG.exception("%s ATM option subscription failed", st.instrument.symbol)

    def finalise_option_1m(self, os_: OptionState):
        b = os_.bar1
        if not b:
            return
        os_.bar1 = None
        end = b.start + timedelta(minutes=1)

        if os_.entry_option_price is None:
            os_.entry_option_price = b.close
            os_.entry_option_oi = os_.latest_oi

        signed = b.volume if b.close > b.open else (-b.volume if b.close < b.open else 0)
        os_.cumulative_signed_volume_proxy += signed

        premium_pct = (
            (b.close / os_.entry_option_price - 1) * 100
            if os_.entry_option_price else None
        )
        oi_pct = (
            (os_.latest_oi / os_.entry_option_oi - 1) * 100
            if os_.latest_oi is not None and os_.entry_option_oi else None
        )

        row1 = {
            "trading_date": end.date(),
            "symbol": os_.symbol,
            "option_type": os_.option_type,
            "strike": os_.strike,
            "expiry": os_.expiry,
            "option_instrument_key": os_.option_key,
            "spot_entry_time": os_.spot_entry_time,
            "candle_start": b.start,
            "candle_end": end,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
            "oi": os_.latest_oi,
            "signed_volume_proxy": signed,
            "cumulative_signed_volume_proxy": os_.cumulative_signed_volume_proxy,
            "premium_change_from_entry_pct": premium_pct,
            "oi_change_from_entry_pct": oi_pct,
        }

        live = {
            "trading_date": end.date(),
            "symbol": os_.symbol,
            "option_type": os_.option_type,
            "strike": os_.strike,
            "expiry": os_.expiry,
            "option_instrument_key": os_.option_key,
            "trading_symbol": os_.trading_symbol,
            "spot_entry_time": os_.spot_entry_time,
            "spot_entry_price": os_.spot_entry_price,
            "entry_option_price": os_.entry_option_price,
            "entry_option_oi": os_.entry_option_oi,
            "latest_ts": end,
            "latest_price": b.close,
            "latest_oi": os_.latest_oi,
            "cumulative_signed_volume_proxy": os_.cumulative_signed_volume_proxy,
            "premium_change_pct": premium_pct,
            "oi_change_pct": oi_pct,
        }

        with db_connect() as c:
            with c.cursor() as x:
                x.execute(UPSERT_OPTION_1M, row1)
                x.execute(UPSERT_OPTION_LIVE, live)
            c.commit()

        self.maybe_score_symbol(os_.symbol, end)

    def process_option_tick(
        self,
        os_: OptionState,
        tick_time: datetime,
        price: float,
        cumulative_volume: int | None,
        oi: int | None,
    ):
        start = aligned_start(tick_time, 1)

        if os_.bar1 and start > os_.bar1.start:
            self.finalise_option_1m(os_)

        volume_delta = 0
        if cumulative_volume is not None:
            if os_.last_cum_volume is not None:
                volume_delta = max(0, cumulative_volume - os_.last_cum_volume)
            os_.last_cum_volume = cumulative_volume

        if oi is not None:
            os_.latest_oi = oi

        if os_.bar1 is None:
            os_.bar1 = Bar(start, price, price, price, price, volume_delta)
        else:
            b = os_.bar1
            b.high = max(b.high, price)
            b.low = min(b.low, price)
            b.close = price
            b.volume += volume_delta

    def maybe_score_symbol(self, symbol: str, now: datetime):
        pair = self.option_by_symbol.get(symbol)
        if not pair or set(pair) != {"CE", "PE"}:
            return

        ce = pair["CE"]
        pe = pair["PE"]
        if (
            ce.entry_option_price is None or pe.entry_option_price is None or
            ce.entry_option_oi is None or pe.entry_option_oi is None
        ):
            return

        def latest(o: OptionState):
            with db_connect() as c:
                with c.cursor(row_factory=dict_row) as x:
                    x.execute("""
                        SELECT close,oi,cumulative_signed_volume_proxy,candle_end
                        FROM public.spot_supply_avwap_option_1m
                        WHERE trading_date=%s AND symbol=%s AND option_type=%s
                        ORDER BY candle_end DESC
                        LIMIT 1
                    """, (o.spot_entry_time.date(), symbol, o.option_type))
                    return x.fetchone()

        ce_m = latest(ce)
        pe_m = latest(pe)
        if not ce_m or not pe_m:
            return

        common_end = min(ce_m["candle_end"], pe_m["candle_end"])
        age_sec = (common_end - ce.spot_entry_time).total_seconds()

        ce_prem_up = float(ce_m["close"]) > ce.entry_option_price
        ce_oi_down = ce_m["oi"] is not None and int(ce_m["oi"]) < ce.entry_option_oi
        pe_prem_down = float(pe_m["close"]) < pe.entry_option_price
        pe_oi_up = pe_m["oi"] is not None and int(pe_m["oi"]) > pe.entry_option_oi

        score = sum([ce_prem_up, ce_oi_down, pe_prem_down, pe_oi_up])

        option_entry_confirmed = False
        option_entry_time = None
        option_entry_score = None
        option_entry_ce_price = None
        option_entry_pe_price = None
        option_entry_spot_price = None
        option_entry_spot_target = None
        option_entry_spot_stop = None

        if score >= 3 and not ce.option_entry_frozen:
            spot_state = next(
                (s for s in self.states.values() if s.instrument.symbol == symbol),
                None
            )
            option_entry_confirmed = True
            option_entry_time = common_end
            option_entry_score = score
            option_entry_ce_price = float(ce_m["close"])
            option_entry_pe_price = float(pe_m["close"])
            option_entry_spot_price = (
                float(spot_state.last_spot_price)
                if spot_state is not None and spot_state.last_spot_price is not None
                else float(ce.spot_entry_price)
            )
            option_entry_spot_target = float(ce.spot_entry_price) * 1.005
            option_entry_spot_stop = float(ce.spot_entry_price) * 0.995
            ce.option_entry_frozen = True
            pe.option_entry_frozen = True
            LOG.warning(
                "%s OPTION ENTRY CONFIRMED | %s | score=%d | CE %.2f @ %.2f | target %.2f | stop %.2f",
                symbol, option_entry_time.strftime("%H:%M"), option_entry_score,
                ce.strike, option_entry_ce_price, option_entry_spot_target, option_entry_spot_stop
            )
        bullish_proxy = (
            int(ce_m["cumulative_signed_volume_proxy"] or 0) > 0
            and int(pe_m["cumulative_signed_volume_proxy"] or 0) < 0
        )

        score5 = flags5 = proxy5 = None
        score15 = flags15 = proxy15 = None

        if age_sec >= 5 * 60 and not ce.score_5m_done:
            score5 = score
            flags5 = (ce_prem_up, ce_oi_down, pe_prem_down, pe_oi_up)
            proxy5 = bullish_proxy
            ce.score_5m_done = pe.score_5m_done = True

        if age_sec >= 15 * 60 and not ce.score_15m_done:
            score15 = score
            flags15 = (ce_prem_up, ce_oi_down, pe_prem_down, pe_oi_up)
            proxy15 = bullish_proxy
            ce.score_15m_done = pe.score_15m_done = True

        if score5 is None and score15 is None and not option_entry_confirmed:
            return

        row = {
            "trading_date": ce.spot_entry_time.date(),
            "symbol": symbol,
            "spot_entry_time": ce.spot_entry_time,
            "ce_strike": ce.strike,
            "pe_strike": pe.strike,
            "expiry": ce.expiry,
            "option_entry_confirmed": option_entry_confirmed,
            "option_entry_time": option_entry_time,
            "option_entry_score": option_entry_score,
            "option_entry_ce_price": option_entry_ce_price,
            "option_entry_pe_price": option_entry_pe_price,
            "option_entry_spot_price": option_entry_spot_price,
            "option_entry_spot_target": option_entry_spot_target,
            "option_entry_spot_stop": option_entry_spot_stop,
            "score_5m": score5,
            "ce_premium_up_5m": flags5[0] if flags5 else None,
            "ce_oi_down_5m": flags5[1] if flags5 else None,
            "pe_premium_down_5m": flags5[2] if flags5 else None,
            "pe_oi_up_5m": flags5[3] if flags5 else None,
            "bullish_proxy_5m": proxy5,
            "score_15m": score15,
            "ce_premium_up_15m": flags15[0] if flags15 else None,
            "ce_oi_down_15m": flags15[1] if flags15 else None,
            "pe_premium_down_15m": flags15[2] if flags15 else None,
            "pe_oi_up_15m": flags15[3] if flags15 else None,
            "bullish_proxy_15m": proxy15,
        }

        with db_connect() as c:
            with c.cursor() as x:
                x.execute(UPSERT_OPTION_SCORE, row)
            c.commit()

    def finalise_3m(self, st):
        b=st.bar3
        if not b:return
        st.bar3=None
        end=b.start+timedelta(minutes=3)

        tp=(b.high+b.low+b.close)/3
        st.cum_pv += tp*b.volume
        st.cum_v += b.volume
        st.avwap = st.cum_pv/st.cum_v if st.cum_v>0 else tp

        if b.start.time().replace(tzinfo=None)==MARKET_OPEN:
            st.anchor_low=b.low

        retrace_now=False; entry_now=False; target_now=False

        # Only after the 10:15 breakout is confirmed.
        if st.breakout_1015 and not st.entry_confirmed and end.time().replace(tzinfo=None) <= ENTRY_CUTOFF:
            if not st.retrace_found and b.low <= st.avwap:
                st.retrace_found=True
                st.retrace_at=end
                st.retrace_low=b.low
                st.avwap_at_retrace=st.avwap
                retrace_now=True
                LOG.info("%s RETRACE AVWAP | low %.2f avwap %.2f",st.instrument.symbol,b.low,st.avwap)

            # Entry must be on a later completed 3m candle, not the same retrace candle.
            elif st.retrace_found and st.retrace_at and end > st.retrace_at and b.close > st.avwap:
                st.entry_confirmed=True
                st.entry_time=end
                st.entry_price=b.close
                st.avwap_at_entry=st.avwap
                st.target_price=b.close*(1+TARGET_PCT/100)
                entry_now=True
                LOG.info("%s ENTRY CONFIRMED | %.2f > AVWAP %.2f | target %.2f",
                         st.instrument.symbol,b.close,st.avwap,st.target_price)
                self.subscribe_atm_options(st)

        if st.entry_confirmed and not st.target_hit and st.target_price and b.high >= st.target_price:
            st.target_hit=True
            st.target_hit_at=end
            target_now=True
            LOG.info("%s TARGET +%.2f%% HIT | target %.2f",st.instrument.symbol,TARGET_PCT,st.target_price)

        self.write(st,b,retrace_now,entry_now,target_now)

    def finalise_60(self, st):
        b=st.bar60
        if not b:return
        st.bar60=None
        end=b.start+timedelta(minutes=60)
        # We only care about the first completed 09:15-10:15 candle.
        if end.time().replace(tzinfo=None) != BREAKOUT_END:
            return
        if b.close > st.zone.strong_supply_high:
            st.breakout_1015=True
            st.breakout_close=b.close
            st.breakout_pct=(b.close/st.zone.strong_supply_high-1)*100
            st.breakout_at=end
            LOG.info("%s 10:15 STRONG SUPPLY BREAKOUT | close %.2f > %.2f | %+0.2f%%",
                     st.instrument.symbol,b.close,st.zone.strong_supply_high,st.breakout_pct)
        else:
            st.breakout_1015=False
        self.write(st,None)

    def process(self,st,ts,price,cumvol):
        st.last_spot_price=price
        s3=aligned_start(ts,3)
        s60=aligned_start(ts,60)
        with st.lock:
            if st.bar3 and s3>st.bar3.start:self.finalise_3m(st)
            if st.bar60 and s60>st.bar60.start:self.finalise_60(st)

            vd=0
            if cumvol is not None:
                if st.last_cum_volume is not None:vd=max(0,cumvol-st.last_cum_volume)
                st.last_cum_volume=cumvol

            if st.bar3 is None:st.bar3=Bar(s3,price,price,price,price,vd)
            else:
                b=st.bar3;b.high=max(b.high,price);b.low=min(b.low,price);b.close=price;b.volume+=vd

            if st.bar60 is None:st.bar60=Bar(s60,price,price,price,price,vd)
            else:
                b=st.bar60;b.high=max(b.high,price);b.low=min(b.low,price);b.close=price;b.volume+=vd

    def on_message(self,message):
        for key,ts,price,vol,oi in extract_ticks(message):
            tt=ts.time().replace(tzinfo=None)
            if not(MARKET_OPEN<=tt<=MARKET_CLOSE):
                continue
            self.last_tick_at=ts

            st=self.states.get(key)
            if st:
                self.process(st,ts,price,vol)
                continue

            os_=self.option_states.get(key)
            if os_:
                self.process_option_tick(os_,ts,price,vol,oi)

    def flush_clock(self):
        now=datetime.now(IST)
        for st in self.states.values():
            with st.lock:
                if st.bar3 and now>=st.bar3.start+timedelta(minutes=3,seconds=3):self.finalise_3m(st)
                if st.bar60 and now>=st.bar60.start+timedelta(minutes=60,seconds=3):self.finalise_60(st)
        for os_ in list(self.option_states.values()):
            if os_.bar1 and now>=os_.bar1.start+timedelta(minutes=1,seconds=3):
                self.finalise_option_1m(os_)

    def stop(self,*_):self.stop_event.set()
    def on_open(self,*_):self.heartbeat("CONNECTED")
    def on_error(self,*args):self.heartbeat("ERROR",str(args[-1] if args else "unknown"))
    def on_close(self,*_):self.heartbeat("DISCONNECTED")

    def run(self):
        signal.signal(signal.SIGTERM,self.stop);signal.signal(signal.SIGINT,self.stop)
        cfg=upstox_client.Configuration();cfg.access_token=ACCESS_TOKEN
        api=upstox_client.ApiClient(cfg)
        keys=list(self.states)
        self.streamer=upstox_client.MarketDataStreamerV3(api,keys,"full")
        self.streamer.on("open",self.on_open);self.streamer.on("message",self.on_message)
        self.streamer.on("error",self.on_error);self.streamer.on("close",self.on_close)
        try:self.streamer.auto_reconnect(True,5,50)
        except Exception:pass
        self.streamer.connect()
        hb=0
        while not self.stop_event.wait(1):
            self.flush_clock()
            if time.monotonic()-hb>=30:self.heartbeat("RUNNING");hb=time.monotonic()
            if datetime.now(IST).time().replace(tzinfo=None)>MARKET_CLOSE:
                self.flush_clock();self.heartbeat("MARKET_CLOSED");break
        try:self.streamer.disconnect()
        except Exception:pass


def wait_for_session():
    while True:
        now=datetime.now(IST);day=now.date()
        if not is_trading_day(day):
            time.sleep(300);continue
        if now.time().replace(tzinfo=None)<CONNECT_TIME:
            time.sleep(min(300,max(1,int((datetime.combine(day,CONNECT_TIME,tzinfo=IST)-now).total_seconds()))));continue
        if now.time().replace(tzinfo=None)>MARKET_CLOSE:
            time.sleep(300);continue
        return


def main():
    if not DATABASE_URL:raise RuntimeError("NEON_DATABASE_URL required")
    if not ACCESS_TOKEN:raise RuntimeError("UPSTOX_TOKEN required")
    ensure_schema()
    while True:
        wait_for_session()
        master=load_master()
        insts=discover_spots(master)
        LOG.info("Discovered %d NSE F&O spot symbols",len(insts))
        zones=build_zones(insts)
        LOG.info("Built zones for %d symbols",len(zones))
        c=Collector(insts,zones);c.run()
        if c.stop_event.is_set():break
        time.sleep(30)


if __name__=="__main__":
    main()