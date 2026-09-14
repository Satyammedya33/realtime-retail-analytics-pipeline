"""Streamlit live dashboard for the real-time retail analytics pipeline.

Run with:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.db import get_engine  # noqa: E402

st.set_page_config(page_title="Retail Streaming Analytics", layout="wide", page_icon="⚡")
st.title("⚡ Real-Time Retail Analytics")
st.caption("Live KPIs from the Kafka → stream-processor → PostgreSQL pipeline")

engine = get_engine()

col1, col2, col3 = st.columns(3)

with engine.connect() as conn:
    try:
        revenue_df = pd.read_sql(
            text(
                "SELECT window_start, category, revenue, order_count, average_order_value "
                "FROM agg_revenue_by_minute ORDER BY window_start DESC LIMIT 500"
            ),
            conn,
        )
        alerts_df = pd.read_sql(
            text("SELECT * FROM fraud_alerts ORDER BY detected_at DESC LIMIT 50"), conn
        )
    except Exception as exc:  # table may not exist yet
        st.warning(f"No data yet — start the pipeline first. ({exc})")
        revenue_df = pd.DataFrame()
        alerts_df = pd.DataFrame()

if not revenue_df.empty:
    col1.metric("Total Revenue (window)", f"₹{revenue_df['revenue'].sum():,.0f}")
    col2.metric("Total Orders (window)", int(revenue_df["order_count"].sum()))
    col3.metric("Active Fraud Alerts", len(alerts_df))

    st.subheader("Revenue Over Time by Category")
    pivot = revenue_df.pivot_table(index="window_start", columns="category", values="revenue", aggfunc="sum").fillna(0)
    st.line_chart(pivot)

    st.subheader("Top Categories by Revenue")
    top_cat = revenue_df.groupby("category")["revenue"].sum().sort_values(ascending=False)
    st.bar_chart(top_cat)

    st.subheader("🚨 Recent Fraud / Anomaly Alerts")
    st.dataframe(alerts_df, use_container_width=True)
else:
    st.info("Run `docker compose up` to start producing and processing events, then refresh this page.")
