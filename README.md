# Spot Supply + AVWAP + ATM Options Live

Collector:
python spot_supply_avwap_live.py

Dashboard:
streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port $PORT

Required:
- NEON_DATABASE_URL
- UPSTOX_TOKEN

After every valid AVWAP entry, the collector:
- finds nearest-expiry ATM CE + PE
- dynamically subscribes to both option contracts
- stores 1-minute option OHLC, volume, and OI
- calculates the 5-minute and 15-minute 0–4 option confirmation score
- stores a signed-volume proxy (not true CVD)

Neon tables:
- public.spot_supply_avwap_live
- public.spot_supply_avwap_3m
- public.spot_supply_avwap_option_live
- public.spot_supply_avwap_option_1m
- public.spot_supply_avwap_option_score
- public.spot_supply_avwap_heartbeat
