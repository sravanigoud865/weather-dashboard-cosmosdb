###############################################
# Streamlit – Weather Dashboard (FINAL FIXED VERSION)
###############################################

import streamlit as st
st.set_page_config(page_title="Weather Dashboard", layout="wide")

import pandas as pd
import numpy as np
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime

################################################
# ✅ AZURE COSMOS DB (Mongo API)
################################################

MONGO_URI = "mongodb+srv://mongodb:Shanu1234@sravanitest.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"

DB_NAME = "weatherdb"
COLLECTION_NAME = "Weather Data"

################################################
# ✅ LOAD WEATHER DATA
################################################

@st.cache_data(show_spinner=True)
def load_weather():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    docs = list(coll.find({}))
    if not docs:
        return pd.DataFrame()

    df = pd.json_normalize(docs)

    # ✅ Correct timestamp extraction (your real field)
    if "ts" in df.columns:
        df["timestamp"] = pd.to_datetime(df["ts"], errors="coerce")
    elif "ts.$date.$numberLong" in df.columns:
        df["timestamp"] = pd.to_datetime(df["ts.$date.$numberLong"], unit="ms", errors="coerce")
    else:
        df["timestamp"] = pd.NaT

    # ✅ Extract proper coordinates
    if "position.coordinates" in df.columns:
        df["lat"] = df["position.coordinates"].apply(lambda x: x[1] if isinstance(x, list) else np.nan)
        df["lon"] = df["position.coordinates"].apply(lambda x: x[0] if isinstance(x, list) else np.nan)
        df["location"] = df["lat"].round(3).astype(str) + "," + df["lon"].round(3).astype(str)
    else:
        df["location"] = "Unknown"

    # ✅ Extract weather values
    df["temperature"] = pd.to_numeric(df.get("airTemperature.value"), errors="coerce")
    df["dewpoint"] = pd.to_numeric(df.get("dewPoint.value"), errors="coerce")
    df["wind_speed"] = pd.to_numeric(df.get("wind.speed.rate"), errors="coerce")
    df["pressure"] = pd.to_numeric(df.get("pressure.value"), errors="coerce")

    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour

    return df


################################################
# ✅ LOAD DATA
################################################

df = load_weather()

if df.empty:
    st.error("❌ No data found in Cosmos DB.")
    st.stop()

################################################
# ✅ SIDEBAR FILTERS
################################################

st.sidebar.header("Filters")

# ✅ Stations
stations = sorted(df["location"].dropna().unique().tolist())
stations_selected = st.sidebar.multiselect(
    "Station(s)", stations, default=stations
)

# ✅ Safe date range
valid_ts = df["timestamp"].dropna()

date_min = valid_ts.min().to_pydatetime()
date_max = valid_ts.max().to_pydatetime()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(date_min.date(), date_max.date()),
    min_value=date_min.date(),
    max_value=date_max.date()
)

# ✅ Convert dateInput → datetime
start_dt = pd.to_datetime(date_range[0])
end_dt = pd.to_datetime(date_range[1])

################################################
# ✅ APPLY FILTERS
################################################

mask_stat = df["location"].isin(stations_selected)
mask_date = (df["timestamp"] >= start_dt) & (df["timestamp"] <= end_dt)

filtered = df[mask_stat & mask_date]

if filtered.empty:
    st.warning("⚠️ No data after applying filters.")
    st.stop()

################################################
# ✅ HEADER + KPIs
################################################

st.title("🌦️ Weather Dashboard")
st.caption("Powered by Azure Cosmos DB (MongoDB vCore)")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Avg Temp (°C)", f"{filtered['temperature'].mean():.2f}")
k2.metric("Avg Dewpoint (°C)", f"{filtered['dewpoint'].mean():.2f}")
k3.metric("Avg Wind (m/s)", f"{filtered['wind_speed'].mean():.2f}")
k4.metric("Avg Pressure", f"{filtered['pressure'].mean():.2f}")

st.divider()

################################################
# ✅ TEMPERATURE TREND
################################################

fig1 = px.line(
    filtered,
    x="timestamp",
    y="temperature",
    color="location",
    title="Temperature Over Time",
    markers=True
)
st.plotly_chart(fig1, use_container_width=True)

################################################
# ✅ HOURLY TEMP
################################################

hourly = filtered.groupby(["hour", "location"])["temperature"].mean().reset_index()
fig2 = px.line(hourly, x="hour", y="temperature", color="location", title="Hourly Profile", markers=True)
st.plotly_chart(fig2, use_container_width=True)


################################################
# ✅ PRESSURE TREND
################################################

fig3 = px.line(
    filtered,
    x="timestamp",
    y="pressure",
    color="location",
    title="Pressure Trend"
)
st.plotly_chart(fig3, use_container_width=True)

################################################
# ✅ PREVIEW
################################################

with st.expander("Preview data"):

    st.dataframe(filtered.head(200), use_container_width=True)
