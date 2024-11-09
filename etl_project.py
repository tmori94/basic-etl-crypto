import requests
import pandas as pd
import sqlite3

def extract_data():
    try:
        url = 'https://api.coingecko.com/api/v3/coins/markets'
        params = {'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 10, 'page': 1}
        response = requests.get(url, params=params)
        
        # Verify if the response is positive (status code 200)
        response.raise_for_status()  # Exception if the response is not 200
        
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        # Manage all the requests in error (timeout, network issue, 404,etc.
        print(f"Error during the API request: {e}")
        return []

def transform_data(data):
    try:
        if not data:
            raise ValueError("No data to transform!")
        
        df = pd.DataFrame(data)
        df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume']]
        df.columns = ['ID', 'Symbol', 'Name', 'Current_Price', 'Market_Cap', 'Total_Volume']
        return df
    except ValueError as e:
        print(f"Data error: {e}")
        return pd.DataFrame()  # Return an empty dataframe

def load_data(df):
    try:
        if df.empty:
            raise ValueError("No data to load in the database.")
        
        conn = sqlite3.connect('crypto_data.db')
        df.to_sql('cryptos', conn, if_exists='replace', index=False)
        conn.close()
    except sqlite3.Error as e:
        # Manage DB errors
        print(f"Error during the upload of data in the DB: {e}")
    except ValueError as e:
        print(f"Error found in the data to be loaded: {e}")

def verify_data():
    try:
        # Connecting to the DB
        conn = sqlite3.connect('crypto_data.db')
        query = 'SELECT * FROM cryptos LIMIT 5' # Select first 5 rows of data
        result = pd.read_sql(query, conn)
        conn.close() # Close DB connection
        print(result)
    except sqlite3.Error as e:
        print(f"Error reading the data: {e}")


def etl_process():
    data = extract_data()
    if data:  # Only if data has been extracted correctly
        df = transform_data(data)
        if not df.empty:  # Only if data has been transformed correctly
            load_data(df)
            print("ETL process completed. Data is loaded into 'crypto_data.db'.")
            verify_data()
        else:
            print("No data to upload.")
    else:
        print("It was not possible to extract data.")

etl_process()