import streamlit as st, pandas as pd, time, sys, plotly.express as px
from datetime import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
clusters = [c.rsplit(':', 2) if c.count(':') >= 3 else c.rsplit(':', 1) + [c.split('//')[1].split('.')[0]] for c in args['--clusters'].split(',')]
poll = int(args.get('--poll', 30))

st.title("Query Acceleration Policy Lag Monitor")

@st.cache_resource
def client(url): return KustoClient(KustoConnectionStringBuilder.with_az_cli_authentication(url))

def query(url, db):
    r = client(url).execute(db, ".show external tables operations query_acceleration statistics")
    df = pd.DataFrame([[row[i] for i in range(len(r.primary_results[0].columns))] for row in r.primary_results[0]], 
                      columns=[c.column_name for c in r.primary_results[0].columns])
    df['QueryTimestamp'] = datetime.now()
    return df

for url, db, name in clusters:
    if name not in st.session_state: st.session_state[name] = pd.DataFrame()
    st.subheader(f"{name.upper()} - {db}")
    globals()[name] = st.empty()

while True:
    for url, db, name in clusters:
        try:
            st.session_state[name] = pd.concat([st.session_state[name], query(url, db)]).tail(1600)
            if not st.session_state[name].empty:
                d = st.session_state[name].copy()
                d['AccelerationPendingDataFilesCount'] = d['AccelerationPendingDataFilesCount'].astype(int)
                globals()[name].plotly_chart(px.line(d, x='QueryTimestamp', y='AccelerationPendingDataFilesCount', color='ExternalTableName'))
        except Exception as e: globals()[name].error(f"❌ {name}: {e}")
    time.sleep(poll)
