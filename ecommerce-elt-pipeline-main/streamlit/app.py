import streamlit as st
import pandas as pd
import plotly.express as px
from pyhive import hive

# Set the page configuration
st.set_page_config(
    page_title="E-Commerce Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Configure the Hive connection
hive_host = "hive-server"
hive_port = 10000
hive_database = "ecommerce_db"

# Establish Hive connection
@st.cache_resource
def get_hive_connection():
    return hive.Connection(host=hive_host, port=hive_port, database=hive_database)

# Query Hive to get data
@st.cache_data
def get_hive_data(query):
    conn = get_hive_connection()
    return pd.read_sql(query, conn)

# Load the data from Hive table
@st.cache_data
def load_table_data():
    query = "SELECT * FROM ecommerce_transformed"
    data = get_hive_data(query)
    data.columns = [col.split('.')[-1] for col in data.columns]  # Simplify column names
    data['invoicedate'] = pd.to_datetime(data['invoicedate'], errors='coerce')  # Convert dates
    return data


# Load data
data = load_table_data()

# Custom CSS for modern design
st.markdown("""
    <style>
        /* Set custom background color */
        .main {
            background-color: #f5f7fa;
        }

        /* Unique sidebar styling */
        section[data-testid="stSidebar"] {
            color: black;
        }
        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3,
        section[data-testid="stSidebar"] h4,
        section[data-testid="stSidebar"] h5,
        section[data-testid="stSidebar"] h6 {
            color: #000000;
        }

        /* Adjust text and card styles */
        .stMetricLabel {
            color: #2c3e50;
        }
        .stMarkdown {
            margin-bottom: 15px;
        }

    </style>
""", unsafe_allow_html=True)

# Title
st.title("📊 E-Commerce Sales Dashboard")

# Top KPIs
st.markdown("### Key Metrics")
kpi1, kpi2, kpi3 = st.columns(3)

with kpi1:
    st.metric(label="🛒 Total Transactions", value=data.shape[0], delta=None)

with kpi2:
    total_quantity = data["quantity"].sum()
    st.metric(label="📦 Total Quantity Sold", value=f"{total_quantity:,}")

with kpi3:
    total_revenue = (data["quantity"] * data["unitprice"]).sum()
    st.metric(label="💰 Total Revenue (€)", value=f"€{total_revenue:,.2f}")

# Sidebar for Filters
st.sidebar.header("Filter Data")
st.sidebar.markdown("Apply filters to customize the data view.")

country_filter = st.sidebar.multiselect(
    "🌍 Select Countries:",
    options=data['country'].dropna().unique(),
    default=None
)

min_date, max_date = data['invoicedate'].min(), data['invoicedate'].max()
date_filter = st.sidebar.date_input(
    "📅 Date Range:",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date,
)

# Filter Data
filtered_data = data[
    (data['country'].isin(country_filter)) &
    (data['invoicedate'] >= pd.Timestamp(date_filter[0])) &
    (data['invoicedate'] <= pd.Timestamp(date_filter[1]))
]

# Filtered Data Preview
st.markdown("### Filtered Data Preview")
st.dataframe(filtered_data.head(), use_container_width=True)

# Download Filtered Data
st.download_button(
    label="⬇️ Download Filtered Data",
    data=filtered_data.to_csv(index=False).encode('utf-8'),
    file_name='filtered_data.csv',
    mime='text/csv',
)

# Visualizations
st.markdown("### Visual Analytics")

# Sales by Country
st.subheader("🌍 Sales by Country")
sales_by_country = (
    filtered_data.groupby("country")["totalamount"].sum().reset_index().sort_values("totalamount", ascending=False)
)
fig = px.bar(
    sales_by_country,
    x="country",
    y="totalamount",
    title="Total Revenue by Country",
    color="totalamount",
    color_continuous_scale=px.colors.sequential.Blues,
)
st.plotly_chart(fig, use_container_width=True)

# Top Selling Products
st.subheader("📦 Top Selling Products")
top_products = (
    filtered_data.groupby("description")["quantity"].sum().reset_index().sort_values("quantity", ascending=False).head(10)
)
fig = px.bar(
    top_products,
    x="quantity",
    y="description",
    orientation='h',
    title="Top Selling Products",
    color="quantity",
    color_continuous_scale=px.colors.sequential.Purples,
)
st.plotly_chart(fig, use_container_width=True)

# Sales Trend Over Time
st.subheader("📈 Sales Trend Over Time")
sales_over_time = (
    filtered_data.groupby(filtered_data['invoicedate'].dt.date)["totalamount"].sum().reset_index()
)
fig = px.line(
    sales_over_time,
    x="invoicedate",
    y="totalamount",
    title="Sales Trend by Revenue",
    markers=True,
    color_discrete_sequence=["#2A9D8F"],
)
st.plotly_chart(fig, use_container_width=True)

# Sales by Year
st.subheader("📅 Sales by Year")
sales_by_year = (
    filtered_data.groupby("year")["totalamount"].sum().reset_index().sort_values("year")
)
fig = px.bar(
    sales_by_year,
    x="year",
    y="totalamount",
    title="Total Revenue by Year",
    color="totalamount",
    color_continuous_scale=px.colors.sequential.Viridis,
)
st.plotly_chart(fig, use_container_width=True)