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

    lock: threading.Lock = field(default_factory=threading.Lock)


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
        try:
            raw=float(epoch); raw=raw/1000 if raw>10_000_000_000 else raw
            ts=datetime.fromtimestamp(raw,tz=UTC).astimezone(IST)
        except Exception: ts=datetime.now(IST)
        try: volume=int(float(volume)) if volume is not None else None
        except Exception: volume=None
        yield str(key),ts,float(price),volume


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


class Collector:
    def __init__(self, insts, zones):
        self.states={i.spot_key:State(i,zones[i.spot_key]) for i in insts if i.spot_key in zones}
        self.stop_event=threading.Event()
        self.streamer=None
        self.last_tick_at=None

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
        for key,ts,price,vol in extract_ticks(message):
            st=self.states.get(key)
            if not st:continue
            tt=ts.time().replace(tzinfo=None)
            if not(MARKET_OPEN<=tt<=MARKET_CLOSE):continue
            self.last_tick_at=ts
            self.process(st,ts,price,vol)

    def flush_clock(self):
        now=datetime.now(IST)
        for st in self.states.values():
            with st.lock:
                if st.bar3 and now>=st.bar3.start+timedelta(minutes=3,seconds=3):self.finalise_3m(st)
                if st.bar60 and now>=st.bar60.start+timedelta(minutes=60,seconds=3):self.finalise_60(st)

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
