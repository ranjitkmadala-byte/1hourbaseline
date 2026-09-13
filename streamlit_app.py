import os
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import streamlit as st

IST = ZoneInfo("Asia/Kolkata")
DATABASE_URL = (os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()

st.set_page_config(page_title="Spot Supply + AVWAP Entries", layout="wide")
st.title("Spot Strong Supply → AVWAP Retrace Entries")
st.caption(
    "SPOT only | 10:15 Strong Supply breakout → retrace to 09:15-anchored AVWAP → "
    "first later 3-minute close back above AVWAP → ENTRY by 12:15 | Target +0.50%"
)

if not DATABASE_URL:
    st.error("NEON_DATABASE_URL (or DATABASE_URL) is not configured.")
    st.stop()

@st.cache_data(ttl=15)
def load_dates():
    with psycopg.connect(DATABASE_URL) as conn:
        return pd.read_sql(
            "SELECT DISTINCT trading_date FROM public.spot_supply_avwap_live ORDER BY trading_date DESC",
            conn,
        )

@st.cache_data(ttl=10)
def load_live(selected_date: date):
    sql = """
    SELECT
      trading_date, symbol, strong_supply_high,
      breakout_1015, breakout_close, breakout_pct,
      anchor_0915_low, avwap, last_3m_close, last_3m_low,
      last_3m_end AT TIME ZONE 'Asia/Kolkata' AS last_3m_end_ist,
      retrace_found, retrace_at AT TIME ZONE 'Asia/Kolkata' AS retrace_at_ist,
      entry_confirmed, entry_time AT TIME ZONE 'Asia/Kolkata' AS entry_time_ist,
      entry_price, avwap_at_entry, target_price, target_hit,
      target_hit_at AT TIME ZONE 'Asia/Kolkata' AS target_hit_at_ist,
      setup_state, updated_at AT TIME ZONE 'Asia/Kolkata' AS updated_at_ist
    FROM public.spot_supply_avwap_live
    WHERE trading_date=%s
    ORDER BY
      CASE setup_state
        WHEN 'ENTRY_CONFIRMED' THEN 1
        WHEN 'TARGET_HIT' THEN 2
        WHEN 'RETRACED_WAIT_CLOSE_ABOVE_AVWAP' THEN 3
        WHEN 'WAIT_RETRACE_TO_AVWAP' THEN 4
        ELSE 5
      END,
      entry_time NULLS LAST, symbol
    """
    with psycopg.connect(DATABASE_URL) as conn:
        return pd.read_sql(sql, conn, params=(selected_date,))

try:
    dates=load_dates()
except Exception as exc:
    st.error(f"Could not read spot_supply_avwap_live: {exc}")
    st.stop()

if dates.empty:
    st.info("No live spot rows yet.")
    st.stop()

available=[pd.to_datetime(x).date() for x in dates["trading_date"].tolist()]
today=datetime.now(IST).date()
default=today if today in available else available[0]
selected=st.selectbox("Trading date",available,index=available.index(default),
                      format_func=lambda d:d.strftime("%d %b %Y"))

try:
    df=load_live(selected)
except Exception as exc:
    st.error(f"Could not load {selected}: {exc}")
    st.stop()

if df.empty:
    st.info("No rows for this date.")
    st.stop()

breakouts=df[df["breakout_1015"]==True].copy()
retested=breakouts[breakouts["retrace_found"]==True].copy()
entries=breakouts[breakouts["entry_confirmed"]==True].copy()
targets=entries[entries["target_hit"]==True].copy()

c1,c2,c3,c4,c5=st.columns(5)
c1.metric("10:15 Breakouts",len(breakouts))
c2.metric("AVWAP Retraces",len(retested))
c3.metric("ENTRY Confirmed",len(entries))
c4.metric("+0.5% Target Hit",len(targets))
c5.metric("Entry Conversion",f"{100*len(entries)/max(1,len(breakouts)):.1f}%")

st.divider()

st.subheader("🚨 ENTRY CONFIRMED")
if entries.empty:
    st.info("No AVWAP retrace + 3-minute recovery entry yet.")
else:
    entry_view=entries.copy()
    entry_view["ENTRY TIME"]=pd.to_datetime(entry_view["entry_time_ist"]).dt.strftime("%H:%M")
    entry_view["ENTRY"]=pd.to_numeric(entry_view["entry_price"],errors="coerce").round(2)
    entry_view["AVWAP"]=pd.to_numeric(entry_view["avwap_at_entry"],errors="coerce").round(2)
    entry_view["TARGET +0.5%"]=pd.to_numeric(entry_view["target_price"],errors="coerce").round(2)
    entry_view["TARGET"]=entry_view["target_hit"].map({True:"✅ HIT",False:"ACTIVE"})
    entry_view["Supply High"]=pd.to_numeric(entry_view["strong_supply_high"],errors="coerce").round(2)

    show=entry_view[["symbol","ENTRY TIME","ENTRY","AVWAP","Supply High","TARGET +0.5%","TARGET"]].rename(
        columns={"symbol":"SYMBOL"}
    )
    st.dataframe(
        show,
        use_container_width=True,
        hide_index=True,
        column_config={
            "SYMBOL": st.column_config.TextColumn("🔥 SYMBOL"),
            "ENTRY": st.column_config.NumberColumn("🟢 ENTRY", format="%.2f"),
            "TARGET +0.5%": st.column_config.NumberColumn("🎯 TARGET +0.5%", format="%.2f"),
        },
    )

st.subheader("Setup Pipeline")
pipeline=breakouts.copy()
if pipeline.empty:
    st.warning("No 10:15 Strong Supply breakout today.")
else:
    labels={
        "WAIT_RETRACE_TO_AVWAP":"🟡 WAITING FOR AVWAP RETRACE",
        "RETRACED_WAIT_CLOSE_ABOVE_AVWAP":"🟠 RETRACED — WAIT 3M CLOSE ABOVE",
        "ENTRY_CONFIRMED":"🟢 ENTRY CONFIRMED",
        "TARGET_HIT":"🎯 TARGET HIT",
        "ENTRY_WINDOW_CLOSED":"⚫ NO ENTRY — 12:15 CUTOFF",
    }
    pipeline["STATUS"]=pipeline["setup_state"].map(labels).fillna(pipeline["setup_state"])
    pipeline["Last 3m"]=pd.to_datetime(pipeline["last_3m_end_ist"]).dt.strftime("%H:%M")
    pipeline["3m Close"]=pd.to_numeric(pipeline["last_3m_close"],errors="coerce").round(2)
    pipeline["AVWAP"]=pd.to_numeric(pipeline["avwap"],errors="coerce").round(2)
    pipeline["Supply High"]=pd.to_numeric(pipeline["strong_supply_high"],errors="coerce").round(2)
    pipeline["Breakout Close"]=pd.to_numeric(pipeline["breakout_close"],errors="coerce").round(2)
    pipeline["Entry"]=pd.to_numeric(pipeline["entry_price"],errors="coerce").round(2)
    pipeline["Target"]=pd.to_numeric(pipeline["target_price"],errors="coerce").round(2)
    st.dataframe(
        pipeline[["symbol","STATUS","Last 3m","3m Close","AVWAP","Supply High","Breakout Close","Entry","Target"]],
        use_container_width=True,hide_index=True
    )

st.divider()
with st.expander("Exact live rule"):
    st.markdown("""
1. **SPOT only.**
2. At **10:15**, the completed 09:15–10:15 spot candle must close above **Strong Supply High**.
3. After 10:15, wait for price to **retrace to/touch AVWAP** anchored from the **09:15 3-minute bar**.
4. The retrace itself is **not an entry**.
5. Entry occurs only on the **first later completed 3-minute candle that closes above AVWAP**.
6. Entry must be confirmed by **12:15 PM**. Later confirmations are ignored.
7. Current target is **+0.50% from entry**.
""")
