import streamlit as st, pandas as pd, time, sys, plotly.express as px, duckdb, os
from datetime import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
clusters = [c.rsplit(':', 2) if c.count(':') >= 3 else c.rsplit(':', 1) + [c.split('//')[1].split('.')[0]] for c in args['--clusters'].split(',')]
poll = int(args.get('--poll', 30))

st.title("Query Acceleration Policy Lag Monitor")

@st.cache_resource
def get_db_connection():
    db_path = os.path.join(os.getcwd(), 'kusto_monitor.db')
    conn = duckdb.connect(db_path)
    return conn

@st.cache_resource
def client(url): return KustoClient(KustoConnectionStringBuilder.with_az_cli_authentication(url))

def query_and_store(url, db, cluster_name):
    r = client(url).execute(db, ".show external tables operations query_acceleration statistics")
    df = pd.DataFrame([[row[i] for i in range(len(r.primary_results[0].columns))] for row in r.primary_results[0]], columns=[c.column_name for c in r.primary_results[0].columns])
    df['QueryTimestamp'] = datetime.now()
    df['cluster_name'] = cluster_name
    df['database_name'] = db
    
    conn = get_db_connection()
    if 'AccelerationPendingDataFilesCount' in df.columns:
        df['AccelerationPendingDataFilesCount'] = pd.to_numeric(df['AccelerationPendingDataFilesCount'], errors='coerce').fillna(0).astype(int)
    
    table_exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='kusto_data'").fetchone()
    if not table_exists:
        conn.register('temp_df', df)
        conn.execute("CREATE TABLE kusto_data AS SELECT * FROM temp_df WHERE 1=0")
        conn.unregister('temp_df')
    
    conn.register('temp_df', df)
    conn.execute("INSERT INTO kusto_data SELECT * FROM temp_df")
    conn.unregister('temp_df')
    
    conn.execute("""
        DELETE FROM kusto_data 
        WHERE cluster_name = ? 
        AND QueryTimestamp < now() - INTERVAL '7 days'
    """, [cluster_name])

def get_cluster_data(cluster_name):
    conn = get_db_connection()
    df = conn.execute("""
        SELECT * FROM kusto_data 
        WHERE cluster_name = ? 
        ORDER BY QueryTimestamp ASC
    """, [cluster_name]).fetchdf()
    return df

for url, db, name in clusters:
    st.subheader(f"{name.upper()} - {db}")
    globals()[name] = st.empty()

while True:
    for url, db, name in clusters:
        try:
            query_and_store(url, db, name)
            d = get_cluster_data(name)
            
            if not d.empty and 'AccelerationPendingDataFilesCount' in d.columns:
                d['AccelerationPendingDataFilesCount'] = pd.to_numeric(d['AccelerationPendingDataFilesCount'], errors='coerce').fillna(0).astype(int)
                globals()[name].plotly_chart(px.line(d, x='QueryTimestamp', y='AccelerationPendingDataFilesCount', color='ExternalTableName'))
        except Exception as e: 
            globals()[name].error(f"❌ {name}: {e}")
    time.sleep(poll)
