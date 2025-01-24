import polars as pl
import numpy as np
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine
import pandas as pd
import psycopg2

# Create the data directory if it doesn't exist
os.makedirs("data", exist_ok=True)

def generate(nrows: int, filename: str):
    names = np.asarray(["Laptop", "Smartphone", "Desk", "Chair", "Monitor", "Printer", "Paper", "Pen", "Notebook", "Coffee Maker", "Cabinet", "Plastic Cups"])
    
    categories = np.asarray(["Electronics", "Electronics", "Office", "Office", "Electronics", "Electronics", "Stationery", "Stationery", "Stationery", "Electronics", "Office", "Sundry"])
    
    product_id = np.random.randint(len(names), size=nrows)
    
    quantity = np.random.randint(1, 11, size=nrows)
    
    price = np.random.randint(199, 10000, size=nrows)/100
    
    # Generate random dates between 2010-01-01 and 2023-12-31
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2023, 12, 31)
    date_range = (end_date - start_date).days  # Get the number of days
    
    # Create random dates
    random_days = np.random.randint(0, date_range, size=nrows)
    order_dates = [(start_date + timedelta(days=int(days))).strftime('%Y-%m-%d') for days in random_days]
    
    # Define columns
    columns = {
        "order_id": np.arange(nrows),
        "order_date": order_dates,
        "customer_id": np.random.randint(100, 1000, size=nrows),
        "customer_name": [f"Customer_{i}" for i in np.random.randint(2**15, size=nrows)],
        "product_id": product_id + 200,
        "product_name": names[product_id],
        "category": categories[product_id],
        "quantity": quantity,
        "price": price,
        "total": quantity * price
    }
    
    # Create a polars dataframe and write to csv with explicit delimiter
    df = pl.DataFrame(columns)
    
    # Convert Windows path separator if needed
    filename = os.path.normpath(filename)
    
    df.write_csv(filename, separator=",", include_header=True)
    print(f"CSV file successfully generated at: {filename}")


def load_to_postgres():
    try:
        # Read the CSV file
        csv_file_path = os.path.join("data", "sales.csv")
        df = pd.read_csv(csv_file_path)
        
        # Create PostgreSQL connection
        db_params = {
            'host': 'localhost',
            'database': 'postgres',
            'user': 'postgres',
            'password': 'Ollatunji24$$',
            'port': '5432'
        }
        
        # Create SQLAlchemy engine
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
        
        # Create table and load data
        df.to_sql('sales_data', 
                  engine, 
                  if_exists='replace',  # 'replace' will drop existing table and create new one
                  index=False,
                  schema='public')
        
        print("Data successfully loaded to PostgreSQL!")
        
        # Verify the data
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM public.sales_data")
            count = result.fetchone()[0]
            print(f"Number of rows in database: {count}")
            
    except Exception as e:
        print(f"Error: {str(e)}")





 #After generating the CSV, load it to PostgreSQL
if __name__ == "__main__":
    # First generate the CSV
    generate(100000, "data/sales.csv")
    
    # Then load it to PostgreSQL
    load_to_postgres()