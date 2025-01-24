import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, date
import psycopg2
from psycopg2 import sql,pool

#initialize the connection pool
try:
    connection_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=5,
        maxconn=20,
        dbname="postgres",
        user="postgres",
        password="Ollatunji24$$",
        host="localhost",
        port="5432")
    
except psycopg2.Error as e:
    st.error(f"Error creating connection pool: {e}")

#function to get a connection from the pool
def get_connection():
    try:
        connection = connection_pool.getconn()
        return connection
    except psycopg2.Error as e:
        st.error(f"Error getting connection from pool: {e}")
        return None
    
#function to return connection to the pool
def return_connection(connection):
    try: 
        connection_pool.putconn(connection)
    except psycopg2.Error as e:
        st.error(f"Error returning connection to pool: {e}")


#function to execute a query

def get_date_range():
    conn = get_connection()
    if conn is None:
        return None, None
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT 
                    MIN(order_date::date)::text, 
                    MAX(order_date::date)::text 
                FROM public.sales_data
            """)
            cur.execute(query)
            min_date, max_date = cur.fetchone()
            return min_date, max_date
    finally:
        return_connection(conn)

def get_unique_category():
    conn = get_connection()
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT DISTINCT category FROM public.sales_data ORDER BY category")
            cur.execute(query)
            return [row[0].capitalize() for row in cur.fetchall()]
    finally:
        return_connection(conn)


def get_dashboard_stats(start_date, end_date, category):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT 
                    category,
                    COUNT(DISTINCT order_id) as total_orders,
                    SUM(price * quantity) as category_revenue
                FROM public.sales_data
                WHERE order_date::date BETWEEN %s::date AND %s::date
                AND (%s = 'All category' OR category = %s)
                GROUP BY category
                ORDER BY category_revenue DESC
            """)
            
            # Convert dates to string format if they're not already
            start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, (datetime, date)) else start_date
            end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, (datetime, date)) else end_date
            
            cur.execute(query, [start_date_str, end_date_str, category, category])
            result = cur.fetchall()
            if not result:
                return pd.DataFrame(columns=['category', 'total_orders', 'category_revenue'])
            return pd.DataFrame(result, columns=['category', 'total_orders', 'category_revenue'])
    finally:
        return_connection(conn)

def get_plot_data(start_date, end_date, category):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT DATE(order_date) as date,
                       SUM(price * quantity) as revenue
                FROM public.sales_data
                WHERE order_date::date BETWEEN %s::date AND %s::date
                  AND (%s = 'All category' OR category = %s)
                GROUP BY DATE(order_date)
                ORDER BY date
            """)
            start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, (datetime, date)) else start_date
            end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, (datetime, date)) else end_date
            
            cur.execute(query, [start_date_str, end_date_str, category, category])
            result = cur.fetchall()
            if not result:
                return pd.DataFrame(columns=['date', 'revenue'])
            return pd.DataFrame(result, columns=['date', 'revenue'])
    finally:
        return_connection(conn)


def get_revenue_by_category(start_date, end_date, category):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT category,
                       SUM(price * quantity) as revenue
                FROM public.sales_data
                WHERE order_date::date BETWEEN %s::date AND %s::date
                  AND (%s = 'All category' OR category = %s)
                GROUP BY category
                ORDER BY revenue DESC
            """)
            start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, (datetime, date)) else start_date
            end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, (datetime, date)) else end_date
            
            cur.execute(query, [start_date_str, end_date_str, category, category])
            result = cur.fetchall()
            if not result:
                return pd.DataFrame(columns=['category', 'revenue'])
            return pd.DataFrame(result, columns=['category', 'revenue'])
    finally:
        return_connection(conn)

def get_raw_data(start_date, end_date, category):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT 
                    order_id, order_date, customer_id, customer_name, 
                    product_id, product_name, category, quantity, price, 
                    (price * quantity) as revenue
                FROM public.sales_data
                WHERE order_date::date BETWEEN %s::date AND %s::date
                  AND (%s = 'All category' OR category = %s)
                ORDER BY order_date, order_id
            """)
            start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, (datetime, date)) else start_date
            end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, (datetime, date)) else end_date
            
            cur.execute(query, [start_date_str, end_date_str, category, category])
            return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    finally:
        return_connection(conn)



def get_top_products(start_date, end_date, category):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT product_name,
                       SUM(quantity) as total_quantity,
                       SUM(price * quantity) as total_revenue
                FROM public.sales_data
                WHERE order_date::date BETWEEN %s::date AND %s::date
                  AND (%s = 'All category' OR category = %s)
                GROUP BY product_name
                ORDER BY total_revenue DESC
            """)
            
            # Convert dates to string format
            start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, (datetime, date)) else start_date
            end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, (datetime, date)) else end_date
            
            cur.execute(query, [start_date_str, end_date_str, category, category])
            result = cur.fetchall()
            if not result:
                return pd.DataFrame(columns=['product_name', 'total_quantity', 'total_revenue'])
            return pd.DataFrame(result, columns=['product_name', 'total_quantity', 'total_revenue'])
    except Exception as e:
        print(f"Error in get_top_products: {str(e)}")
        return pd.DataFrame(columns=['product_name', 'total_quantity', 'total_revenue'])
    finally:
        return_connection(conn)


def execute_query_with_dates(query, start_date, end_date, category):
    try:
        # Convert dates to string format
        start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, (datetime, date)) else start_date
        end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, (datetime, date)) else end_date
        
        return [start_date_str, end_date_str, category, category]
    except Exception as e:
        print(f"Error formatting dates: {str(e)}")
        return None



def plot_data(data, x_col, y_col, title, xlabel, ylabel, orientation='v'):
    fig, ax = plt.subplots(figsize=(10, 6))
    if not data.empty:
        if orientation == 'v':
            ax.bar(data[x_col], data[y_col])
        else:
            ax.barh(data[x_col], data[y_col])
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        plt.xticks(rotation=45)
    else:
        ax.text(0.5, 0.5, "No data available", ha='center', va='center')
    return fig


# Streamlit App
st.title("Sales Performance Dashboard")

# Filters
with st.container():
    col1, col2, col3 = st.columns([1, 1, 2])
    
    # Get date range
    min_date, max_date = get_date_range()
    
    # Convert string dates to datetime.date objects
    default_start = datetime.strptime(min_date, '%Y-%m-%d').date() if min_date else date.today()
    default_end = datetime.strptime(max_date, '%Y-%m-%d').date() if max_date else date.today()
    
    # Create date inputs
    start_date = col1.date_input("Start Date", default_start)
    end_date = col2.date_input("End Date", default_end)
    
    # Get categories
    categories = get_unique_category()
    category = col3.selectbox("Category", ["All category"] + categories)


# Custom CSS for metrics
st.markdown("""
    <style>
    .metric-row {
        display: flex;
        justify-content: space-between;
        margin-bottom: 20px;
    }
    .metric-container {
        flex: 1;
        padding: 10px;
        text-align: center;
        background-color: #f0f2f6;
        border-radius: 5px;
        margin: 0 5px;
    }
    .metric-label {
        font-size: 14px;
        color: #555;
        margin-bottom: 5px;
    }
    .metric-value {
        font-size: 18px;
        font-weight: bold;
        color: #0e1117;
    }
    </style>
""", unsafe_allow_html=True)

# Metrics
st.header("Key Metrics")
stats = get_dashboard_stats(start_date, end_date, category)
if not stats.empty:  # Check if DataFrame is not empty
    # Assuming your DataFrame has these columns
    total_revenue = stats['category_revenue'].sum()
    total_orders = len(stats)
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    top_category = stats.iloc[0]['category'] if len(stats) > 0 else "N/A"
else:
    total_revenue, total_orders, avg_order_value, top_category = 0, 0, 0, "N/A"

# Custom metrics display
metrics_html = f"""
<div class="metric-row">
    <div class="metric-container">
        <div class="metric-label">Total Revenue</div>
        <div class="metric-value">${total_revenue:,.2f}</div>
    </div>
    <div class="metric-container">
        <div class="metric-label">Total Orders</div>
        <div class="metric-value">{total_orders:,}</div>
    </div>
    <div class="metric-container">
        <div class="metric-label">Average Order Value</div>
        <div class="metric-value">${avg_order_value:,.2f}</div>
    </div>
    <div class="metric-container">
        <div class="metric-label">Top Category</div>
        <div class="metric-value">{top_category}</div>
    </div>
</div>
"""
st.markdown(metrics_html, unsafe_allow_html=True)


# Visualization Tabs
st.header("Visualizations")
tabs = st.tabs(["Revenue Over Time", "Revenue by Category", "Top Products"])

# Revenue Over Time Tab
with tabs[0]:
    st.subheader("Revenue Over Time")
    revenue_data = get_plot_data(start_date, end_date, category)
    st.pyplot(plot_data(revenue_data, 'date', 'revenue', "Revenue Over Time", "Date", "Revenue"))

# Revenue by Category Tab
with tabs[1]:
    st.subheader("Revenue by Category")
    category_data = get_revenue_by_category(start_date, end_date, category)
    st.pyplot(plot_data(category_data, 'category', 'revenue', "Revenue by Category", "Category", "Revenue"))

# Top Products Tab
with tabs[2]:
    st.subheader("Top Products")
    try:
        top_products_data = get_top_products(start_date, end_date, category)
        if not top_products_data.empty:
            st.pyplot(plot_data(
                top_products_data, 
                'product_name', 
                'total_revenue',
                "Top Products by Revenue",
                "Product Name",
                "Revenue",
                orientation='h'
            ))
        else:
            st.warning("No data available for the selected criteria")
    except Exception as e:
        st.error(f"Error displaying top products: {str(e)}")

####################################################################


st.header("Raw Data")

# Debug information
st.write(f"Start Date: {start_date}")
st.write(f"End Date: {end_date}")
st.write(f"Selected Category: {category}")


raw_data = get_raw_data(
    start_date=start_date,
    end_date=end_date,
    category=category
)

# Check if data is empty
if raw_data.empty:
    st.warning("No data found for the selected criteria")
else:
    # Remove the index by resetting it and dropping the old index
    raw_data = raw_data.reset_index(drop=True)
    st.write(f"Number of rows: {len(raw_data)}")
    st.dataframe(raw_data, hide_index=True)

# Add spacing
st.write("")