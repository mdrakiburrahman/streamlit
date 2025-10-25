import streamlit as st, pandas as pd, time, sys, plotly.express as px
from datetime import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
clusters = [c.rsplit(':', 1) for c in args['--clusters'].split(',')]
poll = int(args.get('--poll', 30))

st.title("🚀 Multi-Cluster Kusto Monitor")

@st.cache_resource
def client(url): return KustoClient(KustoConnectionStringBuilder.with_az_cli_authentication(url))

def query(url, db):
    r = client(url).execute(db, ".show external tables operations query_acceleration statistics")
    df = pd.DataFrame([[row[i] for i in range(len(r.primary_results[0].columns))] for row in r.primary_results[0]], 
                      columns=[c.column_name for c in r.primary_results[0].columns])
    df['QueryTimestamp'] = datetime.now()
    return df

for url, db in clusters:
    k = url.split('//')[1].split('.')[0]
    if k not in st.session_state: st.session_state[k] = pd.DataFrame()
    st.subheader(f"{k.upper()} - {db}")
    globals()[k] = st.empty()

while True:
    for url, db in clusters:
        k = url.split('//')[1].split('.')[0]
        try:
            st.session_state[k] = pd.concat([st.session_state[k], query(url, db)]).tail(1600)
            if not st.session_state[k].empty:
                d = st.session_state[k].copy()
                d['AccelerationPendingDataFilesCount'] = d['AccelerationPendingDataFilesCount'].astype(int)
                globals()[k].plotly_chart(px.line(d, x='QueryTimestamp', y='AccelerationPendingDataFilesCount', color='ExternalTableName'))
        except Exception as e: globals()[k].error(f"❌ {k}: {e}")
    time.sleep(poll)
