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
        df.col
