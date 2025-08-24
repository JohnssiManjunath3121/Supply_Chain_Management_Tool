import streamlit as st
import mysql.connector
import pandas as pd

# Database credentials
DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = "123456789"
DB_NAME = "supply_chain_db"

# Function to establish database connection
def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

# Function to fetch data from a table
def fetch_data(table_name):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)  # Fetch rows as dictionaries
    cursor.execute(f"SELECT * FROM {table_name}")
    data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(data) if data else pd.DataFrame()

# Streamlit UI
st.title("📊 Supply Chain Data Viewer")
st.write("Dynamically fetch and display SQL data.")

# Get table names from the database
try:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    conn.close()
except Exception as e:
    st.error(f"Error connecting to the database: {e}")
    tables = []

# Select table
selected_table = st.selectbox("Choose a table:", tables)

if selected_table:
    df = fetch_data(selected_table)
    if df.empty:
        st.warning("No data found in this table.")
    else:
        st.write(f"### Data from `{selected_table}`")
        st.dataframe(df)  # Display as table
        st.download_button("Download CSV", df.to_csv(index=False), "data.csv", "text/csv")

# Sidebar
st.sidebar.header("Database Info")
st.sidebar.write(f"📌 Connected to `{DB_NAME}` at `{DB_HOST}`")
st.sidebar.write(f"🗄️ Available Tables: {len(tables)}")
