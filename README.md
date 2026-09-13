# Spot Supply + AVWAP Live

Two Railway services recommended:

## Collector service
Start command:
`python spot_supply_avwap_live.py`

## Dashboard service
Start command:
`streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port $PORT`

Required variables:
- UPSTOX_TOKEN
- NEON_DATABASE_URL

Logic:
- Spot only.
- 09:15-10:15 spot close > Strong Supply High.
- After breakout, wait for touch/retrace to AVWAP anchored at 09:15 3-minute bar.
- No entry on retrace candle.
- Entry on first later completed 3-minute close > AVWAP.
- Entry cutoff 12:15 IST.
- Target +0.50%.
- No trading-holiday activity.
