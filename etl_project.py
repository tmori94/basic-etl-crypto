import logging
import pandas as pd
import sqlite3

# Configure the logging
logging.basicConfig(
    filename='etl_project.log',   # Log file name
    level=logging.DEBUG,          # Minimum logging level (you can change it based on your needs)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format with timestamp
    datefmt='%Y-%m-%d %H:%M:%S'    # Timestamp format
)

def validate_data_types(df):
    """
    Validate that the data types of each column are as expected.
    Expected data types: 
        'ID', 'Symbol', 'Name' -> str
        'Current_Price', 'Market_Cap', 'Total_Volume' -> float or int
    """
    expected_dtypes = {
        'ID': str,
        'Symbol': str,
        'Name': str,
        'Current_Price': (float, int),
        'Market_Cap': (float, int),
        'Total_Volume': (float, int)
    }

    # Iterate through expected columns and check if their data types match
    for column, expected_type in expected_dtypes.items():
        if not df[column].apply(lambda x: isinstance(x, expected_type)).all():
            logging.error(f"Invalid data type in column '{column}'. Expected {expected_type}.")
            return False  # Return False if any data type is incorrect

    return True  # Return True if all data types are correct

def validate_value_ranges(df):
    """
    Validate the values in 'Current_Price', 'Market_Cap', and 'Total_Volume' columns.
    Ensure that the values are positive (i.e., greater than 0).
    """
    # Check if any of the numeric values in these columns are not greater than 0
    if not df[(df['Current_Price'] > 0) & (df['Market_Cap'] > 0)].empty:
        logging.error("Invalid values detected in 'Current_Price' or 'Market_Cap'. Values must be positive.")
        return False  # Return False if there are invalid values
    return True  # Return True if all values are valid

def transform_data(data):
    """
    Transform the raw data into a pandas DataFrame and perform basic validation checks.
    The data is expected to have specific columns, which will be validated.
    """
    try:
        if not data:
            raise ValueError("No data to transform!")  # Raise an error if no data is provided

        df = pd.DataFrame(data)  # Convert the data into a DataFrame

        # Validate for missing or null values in required columns
        required_columns = ['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume']
        missing_columns = [col for col in required_columns if col not in df.columns or df[col].isnull().any()]
        
        if missing_columns:
            # Log error if there are missing or null values in required columns
            logging.error(f"Missing or null values found in columns: {', '.join(missing_columns)}")
            raise ValueError(f"Missing or null values found in columns: {', '.join(missing_columns)}")

        df = df[required_columns]  # Select only the required columns
        df.columns = ['ID', 'Symbol', 'Name', 'Current_Price', 'Market_Cap', 'Total_Volume']  # Rename columns

        # Validate data types of the columns
        if not validate_data_types(df):
            raise ValueError("Data type validation failed.")  # Raise error if data types are incorrect

        # Validate value ranges (e.g., ensuring positive numbers)
        if not validate_value_ranges(df):
            raise ValueError("Value range validation failed.")  # Raise error if values are invalid

        logging.info("Data transformation successful.")  # Log successful transformation
        return df  # Return the transformed DataFrame

    except ValueError as e:
        logging.error(f"Data error: {e}")  # Log error message
        return pd.DataFrame()  # Return an empty DataFrame in case of error

def extract_data():
    """
    Extract data from the CoinGecko API and return it as a list of dictionaries.
    If an error occurs, it logs the error and returns an empty list.
    """
    try:
        url = 'https://api.coingecko.com/api/v3/coins/markets'
        params = {'vs_currency': 'usd', 'order': 'market_cap_desc', 'per_page': 10, 'page': 1}
        response = requests.get(url, params=params)
        
        # Verify if the response status is OK (status code 200)
        response.raise_for_status()  # Raises exception if the response is not 200
        
        data = response.json()  # Convert the response to JSON format
        logging.info("Data extracted successfully from API.")
        return data  # Return the extracted data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error during the API request: {e}")  # Log error in case of exception
        return []  # Return an empty list if an error occurs

def load_data(df):
    """
    Load the transformed data into an SQLite database. If the DataFrame is empty, logs the error.
    """
    try:
        if df.empty:
            raise ValueError("No data to load in the database.")  # Raise error if DataFrame is empty
        
        conn = sqlite3.connect('crypto_data.db')  # Connect to the SQLite database
        df.to_sql('cryptos', conn, if_exists='replace', index=False)  # Load data into the 'cryptos' table
        conn.close()  # Close the connection after loading the data
        logging.info("Data loaded into database successfully.")

    except sqlite3.Error as e:
        logging.error(f"Error during the upload of data in the DB: {e}")  # Log DB-related error
    except ValueError as e:
        logging.error(f"Error found in the data to be loaded: {e}")  # Log error if the data is invalid

def verify_data():
    """
    Verify that data has been successfully loaded into the database by querying the first 5 rows.
    """
    try:
        conn = sqlite3.connect('crypto_data.db')  # Connect to the SQLite database
        query = 'SELECT * FROM cryptos LIMIT 5'  # SQL query to fetch the first 5 rows
        result = pd.read_sql(query, conn)  # Execute the query and store the result in a DataFrame
        conn.close()  # Close the database connection
        logging.info("Data verification successful.")  # Log verification success
        print(result)  # Print the result to the console

    except sqlite3.Error as e:
        logging.error(f"Error reading the data: {e}")  # Log error if there's a problem with reading data

def etl_process():
    """
    Run the entire ETL process: Extract data, transform it, load it into the database, and verify.
    """
    data = extract_data()  # Extract data from the API
    if data:  # Only proceed if data was successfully extracted
        df = transform_data(data)  # Transform the data
        if not df.empty:  # Only load data if it's valid (not empty)
            load_data(df)  # Load the transformed data into the database
            logging.info("ETL process completed. Data is loaded into 'crypto_data.db'.")
            verify_data()  # Verify that the data was loaded correctly
        else:
            logging.warning("No data to upload.")  # Log if there was no valid data to upload
    else:
        logging.error("It was not possible to extract data.")  # Log if the data extraction failed

# Start the ETL process
etl_process()
