import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# Load data
spot_df = pd.read_csv("ABB.csv", index_col="ti")
fut_df  = pd.read_csv("ABB_FUT - Copy (2)_output.csv", index_col="ti")

# Filter epoch range
start, end = 1705737960, 1713426240
spot_df = spot_df.loc[(spot_df.index > start) & (spot_df.index < end)]
fut_df  = fut_df.loc[(fut_df.index > start) & (fut_df.index < end)]

# Align futures to spot timestamps (IMPORTANT)
fut_df = fut_df.reindex(spot_df.index).dropna()

# Convert epoch → datetime (seconds)
import datetime
spot_df.index = pd.to_datetime(spot_df.index, unit="s") - datetime.timedelta(minutes=330)
fut_df.index  = pd.to_datetime(fut_df.index, unit="s") - datetime.timedelta(minutes=330)

# Create chart
fig = go.Figure()

# Spot candles
fig.add_trace(
    go.Candlestick(
        x=spot_df.index,
        open=spot_df["o"],
        high=spot_df["h"],
        low=spot_df["l"],
        close=spot_df["c"],
        name="SPOT"
    )
)

# Futures candles
fig.add_trace(
    go.Candlestick(
        x=fut_df.index,
        open=fut_df["o"],
        high=fut_df["h"],
        low=fut_df["l"],
        close=fut_df["c"],
        name="FUT",
        opacity=0.6
    )
)

fig.update_layout(
    title="ABB Spot vs Futures",
    xaxis_title="Time",
    yaxis_title="Price",
    xaxis_rangeslider_visible=False,
    height=700
)

st.plotly_chart(fig, use_container_width=True)
