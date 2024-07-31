from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

# Get password from environment variable
pwd = os.environ.get('PGPASS')
uid = os.environ.get('PGUID')

# Database connection parameters for SQL Server
driver = "{SQL Server Native Client 11.0}"
sql_server = "< server name >"
sql_database = "<db name>"

# Database connection parameters for PostgreSQL
postgres_host = "hostname"
postgres_db = "db name"

# Extract data from SQL Server
def extract():
    try:
        src_conn = pyodbc.connect(
            f'DRIVER={driver};SERVER={sql_server};DATABASE={sql_database};UID={uid};PWD={pwd}'
        )
        src_cursor = src_conn.cursor()
        
        # Execute query to get table names
        src_cursor.execute("""
        SELECT t.name AS table_name
        FROM sys.tables t 
        WHERE t.name IN ('DimProduct', 'DimProductSubcategory', 'DimProductCategory', 'DimSalesTerritory', 'FactInternetSales')
        """) # names of columns
        
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            # Query and load save data to dataframe
            df = pd.read_sql_query(f'SELECT * FROM {tbl.table_name}', src_conn)
            print(f"Extracted data from table {tbl.table_name}")
            load(df, tbl.table_name)
            
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

# Load data to PostgreSQL
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{postgres_host}:5432/{postgres_db}')
        
        print(f'Importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # Save df to PostgreSQL
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        
        print(f"Data imported successfully into table stg_{tbl}")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    # Call extract function
    extract()
    
except Exception as e:
    print("Error while extracting data: " + str(e))
