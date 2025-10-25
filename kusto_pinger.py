import streamlit as st
import pandas as pd
import time
from datetime import datetime
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Kusto External Tables Acceleration Monitor", layout="wide")
st.title("� Kusto External Tables Query Acceleration Monitor")

# Configuration
KUSTO_CLUSTER = "https://eventhousekusto.westcentralus.kusto.windows.net"
KUSTO_QUERY = ".show external tables operations query_acceleration statistics"
POLLING_INTERVAL = 30  # seconds

@st.cache_resource
def get_kusto_client():
    """Initialize Kusto client with device authentication."""
    try:
        # Use device authentication for interactive login
        kcsb = KustoConnectionStringBuilder.with_interactive_login(KUSTO_CLUSTER)
        return KustoClient(kcsb)
    except Exception as e:
        st.error(f"Failed to connect to Kusto cluster: {str(e)}")
        return None

def execute_kusto_query(client):
    """Execute the Kusto query and return results as DataFrame."""
    if client is None:
        return None
    
    try:
        response = client.execute("kusto", KUSTO_QUERY)
        
        # Convert to DataFrame
        columns = [col.column_name for col in response.primary_results[0].columns]
        data = []
        for row in response.primary_results[0]:
            data.append([row[i] for i in range(len(columns))])
        
        df = pd.DataFrame(data, columns=columns)
        
        # Add timestamp for tracking
        df['QueryTimestamp'] = datetime.now()
        
        return df
    
    except KustoServiceError as e:
        st.error(f"Kusto query failed: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Unexpected error: {str(e)}")
        return None

# Initialize Streamlit components
st.sidebar.header("Configuration")
st.sidebar.write(f"**Cluster:** {KUSTO_CLUSTER}")
st.sidebar.write(f"**Polling Interval:** {POLLING_INTERVAL} seconds")
st.sidebar.write(f"**Query:** `{KUSTO_QUERY}`")

# Authentication status
auth_status = st.empty()
auth_status.info("🔐 Initializing Kusto connection... Please complete authentication if prompted.")

# Get Kusto client
client = get_kusto_client()

if client:
    auth_status.success("✅ Connected to Kusto cluster successfully!")
    
    # Placeholders for dynamic content
    metrics_placeholder = st.empty()
    chart_placeholder = st.empty()
    table_placeholder = st.empty()
    
    # Initialize data storage
    if 'historical_data' not in st.session_state:
        st.session_state.historical_data = pd.DataFrame()
    
    # Main polling loop
    while True:
        with st.spinner("Querying Kusto cluster..."):
            current_data = execute_kusto_query(client)
        
        if current_data is not None:
            # Append to historical data
            if not st.session_state.historical_data.empty:
                st.session_state.historical_data = pd.concat([
                    st.session_state.historical_data, 
                    current_data
                ], ignore_index=True)
            else:
                st.session_state.historical_data = current_data.copy()
            
            # Keep only last 100 data points per table to prevent memory issues
            if len(st.session_state.historical_data) > 1600:  # 16 tables * 100 points
                st.session_state.historical_data = st.session_state.historical_data.tail(1600)
            
            # Display current metrics
            with metrics_placeholder.container():
                st.subheader("📊 Current Statistics")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    total_pending = current_data['AccelerationPendingDataFilesCount'].astype(int).sum()
                    st.metric("Total Pending Files", f"{total_pending:,}")
                
                with col2:
                    avg_completion = current_data['AccelerationCompletePercentage'].astype(float).mean()
                    st.metric("Avg Completion %", f"{avg_completion:.2f}%")
                
                with col3:
                    last_update = datetime.now().strftime("%H:%M:%S")
                    st.metric("Last Update", last_update)
            
            # Create time series chart
            with chart_placeholder.container():
                st.subheader("📈 Acceleration Pending Data Files Count Over Time")
                
                if len(st.session_state.historical_data) > 0:
                    # Prepare data for plotting
                    plot_data = st.session_state.historical_data.copy()
                    plot_data['AccelerationPendingDataFilesCount'] = plot_data['AccelerationPendingDataFilesCount'].astype(int)
                    
                    # Create interactive line chart
                    fig = px.line(
                        plot_data,
                        x='QueryTimestamp',
                        y='AccelerationPendingDataFilesCount',
                        color='ExternalTableName',
                        title='Pending Data Files Count by External Table',
                        labels={
                            'QueryTimestamp': 'Time',
                            'AccelerationPendingDataFilesCount': 'Pending Files Count',
                            'ExternalTableName': 'Table Name'
                        }
                    )
                    
                    fig.update_layout(
                        height=500,
                        hovermode='x unified',
                        legend=dict(
                            orientation="v",
                            yanchor="top",
                            y=1,
                            xanchor="left",
                            x=1.02
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Collecting data... Chart will appear after first successful query.")
            
            # Display current data table
            with table_placeholder.container():
                st.subheader("📋 Current Query Results")
                
                # Format the data for display
                display_data = current_data.drop('QueryTimestamp', axis=1).copy()
                display_data['AccelerationPendingDataFilesCount'] = display_data['AccelerationPendingDataFilesCount'].astype(int)
                display_data['AccelerationCompletePercentage'] = display_data['AccelerationCompletePercentage'].astype(float).round(2)
                
                st.dataframe(
                    display_data,
                    use_container_width=True,
                    hide_index=True
                )
        
        else:
            st.error("❌ Failed to retrieve data from Kusto cluster.")
        
        # Wait before next poll
        time.sleep(POLLING_INTERVAL)

else:
    auth_status.error("❌ Failed to connect to Kusto cluster. Please check your authentication and network connection.")
