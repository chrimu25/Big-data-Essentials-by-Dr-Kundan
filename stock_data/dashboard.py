import streamlit as st
import pandas as pd
import psycopg2

# 1. PAGE SETUP
st.set_page_config(page_title="Live Stock Predictions", layout="wide")
st.title("📈 Real-Time AI Stock Predictions")
st.markdown("Live data streaming from **Kafka** ➔ **PySpark MLlib** ➔ **AWS RDS**")

# 2. AWS DATABASE CREDENTIALS (Matches your consumer.py exactly)
DB_HOST = "dbessentialexam.c9kaassq2icm.eu-north-1.rds.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "mugabo"
DB_PASSWORD = "dbessentialexam"

# 3. FETCH LIVE DATA FROM AWS
# We cache the data for 3 seconds so we don't overwhelm the database with requests
@st.cache_data(ttl=3)
def load_data():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        # Fetch the most recent 2000 records
        query = 'SELECT * FROM live_stock_predictions ORDER BY "Date" DESC LIMIT 2000'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"❌ Could not connect to AWS: {e}")
        return pd.DataFrame()

# Load the data
df = load_data()

# 4. BUILD THE INTERACTIVE DASHBOARD
if df.empty:
    st.warning("⏳ Waiting for data to stream into the AWS database...")
else:
    # --- Sidebar Filters ---
    st.sidebar.header("Filter Options")
    symbols = df['Symbol'].unique()
    selected_symbol = st.sidebar.selectbox("Select a Stock Symbol:", symbols)

    # Filter the dataframe for the chosen stock
    stock_df = df[df['Symbol'] == selected_symbol].copy()
    
    # Sort chronologically so the line chart moves left to right correctly
    stock_df = stock_df.sort_values(by="Date")

    # --- Main Chart ---
    st.subheader(f"Actual Close vs. Predicted Close: {selected_symbol}")
    
    # Format the data for Streamlit's native line chart
    chart_data = stock_df.set_index("Date")[["Close", "prediction"]]
    
    # Draw the chart!
    st.line_chart(chart_data)

    # --- Metrics & Raw Data ---
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(label="Latest Actual Close Price", value=f"${stock_df['Close'].iloc[-1]:.2f}")
    with col2:
        st.metric(label="Latest AI Predicted Price", value=f"${stock_df['prediction'].iloc[-1]:.2f}")

    st.subheader("Raw Streaming Data (Latest Records)")
    # Show the newest records at the top of the table
    st.dataframe(stock_df.sort_values(by="Date", ascending=False).head(15))

    # Add a simple refresh button
    if st.button("🔄 Refresh Data"):
        st.rerun()