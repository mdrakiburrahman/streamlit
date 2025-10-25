import streamlit as st
import pandas as pd
import numpy as np
import time

st.set_page_config(page_title="Real-Time Random Data", layout="wide")
st.title("📈 Real-Time Random Data Dashboard")

def fake_api():
    """Simulate a REST API returning JSON data."""
    return {
        "timestamp": time.time(),
        "value": np.random.randn() * 10 + 50
    }

chart_placeholder = st.empty()
table_placeholder = st.empty()

data = pd.DataFrame(columns=["timestamp", "value"])

refresh_interval = 1.0
st.write(f"Polling every {refresh_interval} second(s)...")

while True:
    json_data = fake_api()
    new_row = {"timestamp": json_data["timestamp"], "value": json_data["value"]}
    data.loc[len(data)] = new_row

    data = data.tail(50)

    chart_placeholder.line_chart(
        data.set_index("timestamp")["value"], height=400
    )
    table_placeholder.json(json_data)
    time.sleep(refresh_interval)
