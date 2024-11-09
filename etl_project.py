import requests
import pandas as pd
import sqlite3

def extract_data():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 10, 'page': 1}
    response = requests.get(url, params=params)
    data = response.json()
    return data

def transform_data(data):
    df = pd.DataFrame(data)
    df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume']]
    df.columns = ['ID', 'Symbol', 'Name', 'Current_Price', 'Market_Cap', 'Total_Volume']
    return df

def load_data(df):
    conn = sqlite3.connect('crypto_data.db')
    df.to_sql('cryptos', conn, if_exists='replace', index=False)
    conn.close()

def verify_data():
    # Connessione al database
    conn = sqlite3.connect('crypto_data.db')
    query = 'SELECT * FROM cryptos LIMIT 5'  # Seleziona le prime 5 righe
    result = pd.read_sql(query, conn)
    conn.close()
    # Visualizza i primi 5 record
    print(result)

def etl_process():
    data = extract_data()
    df = transform_data(data)
    load_data(df)
    print("ETL process completed. Data is loaded into 'crypto_data.db'.")
    verify_data()

etl_process()