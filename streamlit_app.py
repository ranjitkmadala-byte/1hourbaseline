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
    "SPOT only | 10:15 Strong Supply breakout → AVWAP retrace → "
    "later 3-minute close above AVWAP → entry by 12:15 | Target +0.50% | "
    "ATM CE/PE option score at +5m and +15m"
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
      l.trading_date, l.symbol, l.strong_supply_high,
      l.breakout_1015, l.breakout_close, l.breakout_pct,
      l.anchor_0915_low, l.avwap, l.last_3m_close, l.last_3m_low,
      l.last_3m_end AT TIME ZONE 'Asia/Kolkata' AS last_3m_end_ist,
      l.retrace_found, l.retrace_at AT TIME ZONE 'Asia/Kolkata' AS retrace_at_ist,
      l.entry_confirmed, l.entry_time AT TIME ZONE 'Asia/Kolkata' AS entry_time_ist,
      l.entry_price, l.avwap_at_entry, l.target_price, l.target_hit,
      l.target_hit_at AT TIME ZONE 'Asia/Kolkata' AS target_hit_at_ist,
      l.setup_state, l.updated_at AT TIME ZONE 'Asia/Kolkata' AS updated_at_ist,

      s.option_entry_confirmed,
      s.option_entry_time AT TIME ZONE 'Asia/Kolkata' AS option_entry_time_ist,
      s.option_entry_score,
      s.option_entry_ce_price,
      s.option_entry_pe_price,
      s.option_entry_spot_price,
      s.option_entry_spot_target,
      s.option_entry_spot_stop,
      s.score_5m, s.bullish_proxy_5m,
      s.score_15m, s.bullish_proxy_15m,

      ce.strike AS ce_strike,
      ce.expiry AS ce_expiry,
      ce.entry_option_price AS ce_entry_price,
      ce.latest_price AS ce_current_price,
      ce.premium_change_pct AS ce_change_pct,
      ce.oi_change_pct AS ce_oi_change_pct,

      pe.strike AS pe_strike,
      pe.expiry AS pe_expiry,
      pe.entry_option_price AS pe_entry_price,
      pe.latest_price AS pe_current_price,
      pe.premium_change_pct AS pe_change_pct,
      pe.oi_change_pct AS pe_oi_change_pct

    FROM public.spot_supply_avwap_live l

    LEFT JOIN public.spot_supply_avwap_option_score s
      ON s.trading_date=l.trading_date
     AND s.symbol=l.symbol

    LEFT JOIN public.spot_supply_avwap_option_live ce
      ON ce.trading_date=l.trading_date
     AND ce.symbol=l.symbol
     AND ce.option_type='CE'

    LEFT JOIN public.spot_supply_avwap_option_live pe
      ON pe.trading_date=l.trading_date
     AND pe.symbol=l.symbol
     AND pe.option_type='PE'

    WHERE l.trading_date=%s

    ORDER BY
      CASE l.setup_state
        WHEN 'ENTRY_CONFIRMED' THEN 1
        WHEN 'TARGET_HIT' THEN 2
        WHEN 'RETRACED_WAIT_CLOSE_ABOVE_AVWAP' THEN 3
        WHEN 'WAIT_RETRACE_TO_AVWAP' THEN 4
        ELSE 5
      END,
      l.entry_time NULLS LAST,
      l.symbol
    """
    with psycopg.connect(DATABASE_URL) as conn:
        return pd.read_sql(sql, conn, params=(selected_date,))

dates=load_dates()
if dates.empty:
    st.info("No live spot rows yet.")
    st.stop()

available=[pd.to_datetime(x).date() for x in dates["trading_date"].tolist()]
today=datetime.now(IST).date()
default=today if today in available else available[0]
selected=st.selectbox("Trading date",available,index=available.index(default),
                      format_func=lambda d:d.strftime("%d %b %Y"))

df=load_live(selected)
if df.empty:
    st.info("No rows for this date.")
    st.stop()

breakouts=df[df["breakout_1015"]==True].copy()
retested=breakouts[breakouts["retrace_found"]==True].copy()
entries=breakouts[breakouts["entry_confirmed"]==True].copy()
targets=entries[entries["target_hit"]==True].copy()
strong5=entries[pd.to_numeric(entries["score_5m"], errors="coerce") >= 3].copy()

c1,c2,c3,c4,c5,c6=st.columns(6)
c1.metric("10:15 Breakouts",len(breakouts))
c2.metric("AVWAP Retraces",len(retested))
c3.metric("ENTRY Confirmed",len(entries))
c4.metric("5m Score ≥3",len(strong5))
c5.metric("+0.5% Target Hit",len(targets))
c6.metric("Entry Conversion",f"{100*len(entries)/max(1,len(breakouts)):.1f}%")

st.divider()
st.subheader("🔥 EXACT ATM CE OPTION ENTRIES")
exact_entries = entries[entries["option_entry_confirmed"] == True].copy()
if exact_entries.empty:
    st.info("Waiting for live option score to reach ≥3.")
else:
    exact_entries["OPTION ENTRY TIME"]=pd.to_datetime(exact_entries["option_entry_time_ist"]).dt.strftime("%H:%M")
    exact_entries["SCORE"]=pd.to_numeric(exact_entries["option_entry_score"],errors="coerce").astype("Int64")
    exact_entries["ATM CE"]=pd.to_numeric(exact_entries["ce_strike"],errors="coerce").round(2)
    exact_entries["EXACT CE ENTRY"]=pd.to_numeric(exact_entries["option_entry_ce_price"],errors="coerce").round(2)
    exact_entries["SPOT @ SIGNAL"]=pd.to_numeric(exact_entries["option_entry_spot_price"],errors="coerce").round(2)
    exact_entries["+0.5% TARGET"]=pd.to_numeric(exact_entries["option_entry_spot_target"],errors="coerce").round(2)
    exact_entries["-0.5% STOP"]=pd.to_numeric(exact_entries["option_entry_spot_stop"],errors="coerce").round(2)
    exact_entries["CE LIVE"]=pd.to_numeric(exact_entries["ce_current_price"],errors="coerce").round(2)
    exact_entries["TARGET STATUS"]=exact_entries["target_hit"].map({True:"🎯 HIT",False:"ACTIVE"})
    st.dataframe(
        exact_entries[["symbol","OPTION ENTRY TIME","SCORE","ATM CE","EXACT CE ENTRY",
                       "SPOT @ SIGNAL","+0.5% TARGET","-0.5% STOP","CE LIVE","TARGET STATUS"]]
        .rename(columns={"symbol":"SYMBOL"}),
        use_container_width=True,
        hide_index=True,
        column_config={
            "EXACT CE ENTRY": st.column_config.NumberColumn("🔥 EXACT CE ENTRY",format="%.2f"),
            "+0.5% TARGET": st.column_config.NumberColumn("🎯 SPOT TARGET",format="%.2f"),
            "-0.5% STOP": st.column_config.NumberColumn("🛑 SPOT STOP",format="%.2f"),
        },
    )

st.subheader("🚨 ENTRY CONFIRMED")

if entries.empty:
    st.info("No AVWAP retrace + 3-minute recovery entry yet.")
else:
    v=entries.copy()
    v["ENTRY TIME"]=pd.to_datetime(v["entry_time_ist"]).dt.strftime("%H:%M")
    v["ENTRY"]=pd.to_numeric(v["entry_price"],errors="coerce").round(2)
    v["AVWAP"]=pd.to_numeric(v["avwap_at_entry"],errors="coerce").round(2)
    v["SUPPLY"]=pd.to_numeric(v["strong_supply_high"],errors="coerce").round(2)

    # Prominent spot +0.5% target.
    v["SPOT +0.5% TARGET"]=pd.to_numeric(v["target_price"],errors="coerce").round(2)
    v["TARGET STATUS"]=v["target_hit"].map({True:"🎯 HIT",False:"ACTIVE"})

    # ATM CE live details.
    v["ATM CE"]=pd.to_numeric(v["ce_strike"],errors="coerce").round(2)
    v["CE ENTRY"]=pd.to_numeric(v["ce_entry_price"],errors="coerce").round(2)
    v["CE NOW"]=pd.to_numeric(v["ce_current_price"],errors="coerce").round(2)
    v["CE %"]=pd.to_numeric(v["ce_change_pct"],errors="coerce").round(2)
    v["CE OI %"]=pd.to_numeric(v["ce_oi_change_pct"],errors="coerce").round(2)

    # ATM PE details retained for confirmation context.
    v["ATM PE"]=pd.to_numeric(v["pe_strike"],errors="coerce").round(2)
    v["PE ENTRY"]=pd.to_numeric(v["pe_entry_price"],errors="coerce").round(2)
    v["PE NOW"]=pd.to_numeric(v["pe_current_price"],errors="coerce").round(2)
    v["PE %"]=pd.to_numeric(v["pe_change_pct"],errors="coerce").round(2)
    v["PE OI %"]=pd.to_numeric(v["pe_oi_change_pct"],errors="coerce").round(2)

    v["5M SCORE"]=pd.to_numeric(v["score_5m"],errors="coerce").astype("Int64")
    v["15M SCORE"]=pd.to_numeric(v["score_15m"],errors="coerce").astype("Int64")
    v["5M PROXY"]=v["bullish_proxy_5m"].map({True:"✅ BULL",False:"—"}).fillna("WAIT")
    v["15M PROXY"]=v["bullish_proxy_15m"].map({True:"✅ BULL",False:"—"}).fillna("WAIT")

    # 15-minute score is the main research filter from the historical study.
    v["OPTION STATUS"]=v["15M SCORE"].apply(
        lambda x: "🔥 STRONG" if pd.notna(x) and x >= 3
        else ("WAIT" if pd.isna(x) else "NORMAL")
    )

    st.dataframe(
        v[[
            "symbol","ENTRY TIME","ENTRY","SPOT +0.5% TARGET","TARGET STATUS",
            "ATM CE","CE ENTRY","CE NOW","CE %","CE OI %",
            "5M SCORE","15M SCORE","OPTION STATUS",
            "ATM PE","PE ENTRY","PE NOW","PE %","PE OI %",
            "AVWAP","SUPPLY"
        ]].rename(columns={"symbol":"SYMBOL"}),
        use_container_width=True,
        hide_index=True,
        column_config={
            "SYMBOL": st.column_config.TextColumn("🔥 SYMBOL"),
            "ENTRY": st.column_config.NumberColumn("🟢 SPOT ENTRY", format="%.2f"),
            "SPOT +0.5% TARGET": st.column_config.NumberColumn("🎯 SPOT +0.5% TARGET", format="%.2f"),
            "ATM CE": st.column_config.NumberColumn("ATM CE STRIKE", format="%.2f"),
            "CE ENTRY": st.column_config.NumberColumn("CE ENTRY PREMIUM", format="%.2f"),
            "CE NOW": st.column_config.NumberColumn("CE LIVE PREMIUM", format="%.2f"),
            "CE %": st.column_config.NumberColumn("CE % CHANGE", format="%.2f%%"),
            "CE OI %": st.column_config.NumberColumn("CE OI %", format="%.2f%%"),
            "ATM PE": st.column_config.NumberColumn("ATM PE STRIKE", format="%.2f"),
            "PE ENTRY": st.column_config.NumberColumn("PE ENTRY PREMIUM", format="%.2f"),
            "PE NOW": st.column_config.NumberColumn("PE LIVE PREMIUM", format="%.2f"),
            "PE %": st.column_config.NumberColumn("PE % CHANGE", format="%.2f%%"),
            "PE OI %": st.column_config.NumberColumn("PE OI %", format="%.2f%%"),
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
    pipeline["Target +0.5%"]=pd.to_numeric(pipeline["target_price"],errors="coerce").round(2)
    pipeline["ATM CE"]=pd.to_numeric(pipeline["ce_strike"],errors="coerce").round(2)
    pipeline["CE Now"]=pd.to_numeric(pipeline["ce_current_price"],errors="coerce").round(2)
    pipeline["5m Score"]=pd.to_numeric(pipeline["score_5m"],errors="coerce").astype("Int64")
    pipeline["15m Score"]=pd.to_numeric(pipeline["score_15m"],errors="coerce").astype("Int64")

    st.dataframe(
        pipeline[["symbol","STATUS","Last 3m","3m Close","AVWAP",
                  "Supply High","Breakout Close","Entry","Target +0.5%",
                  "ATM CE","CE Now","5m Score","15m Score"]],
        use_container_width=True,
        hide_index=True
    )

st.divider()
with st.expander("Exact live rule"):
    st.markdown("""
1. **SPOT only.**
2. At **10:15**, the completed 09:15–10:15 spot candle must close above **Strong Supply High**.
3. After 10:15, wait for price to **retrace to/touch AVWAP** anchored from the **09:15 3-minute bar**.
4. The retrace itself is **not an entry**.
5. Entry occurs only on the **first later completed 3-minute candle that closes above AVWAP**.
6. Entry must be confirmed by **12:15 PM**.
7. Target is **+0.50% from entry**.
8. Immediately after entry, subscribe to nearest-expiry **ATM CE + ATM PE**.
9. Option score at +5m and +15m:
   - CE premium ↑ = 1
   - CE OI ↓ = 1
   - PE premium ↓ = 1
   - PE OI ↑ = 1
10. Score **3–4** is highlighted as strong option confirmation.
11. The confirmed-entry table prominently shows **Spot +0.5% Target**, **ATM CE strike**, CE entry premium, CE live premium, CE % change and CE OI % change.
12. Signed-volume proxy is also shown. It is a research proxy, **not true CVD**.
""")
