import streamlit as st
import pandas as pd
import time
from datetime import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
import plotly.express as px

@st.cache_resource
def get_client():
    return KustoClient(KustoConnectionStringBuilder.with_interactive_login("https://eventhousekusto.westcentralus.kusto.windows.net"))

def query():
    response = get_client().execute("kusto", ".show external tables operations query_acceleration statistics")
    columns = [col.column_name for col in response.primary_results[0].columns]
    data = [[row[i] for i in range(len(columns))] for row in response.primary_results[0]]
    df = pd.DataFrame(data, columns=columns)
    df['QueryTimestamp'] = datetime.now()
    return df

if 'data' not in st.session_state:
    st.session_state.data = pd.DataFrame()

chart = st.empty()

while True:
    current = query()
    st.session_state.data = pd.concat([st.session_state.data, current]).tail(1600)
    
    if not st.session_state.data.empty:
        plot_data = st.session_state.data.copy()
        plot_data['AccelerationPendingDataFilesCount'] = plot_data['AccelerationPendingDataFilesCount'].astype(int)
        fig = px.line(plot_data, x='QueryTimestamp', y='AccelerationPendingDataFilesCount', color='ExternalTableName')
        chart.plotly_chart(fig, use_container_width=True)
    
    time.sleep(30)
