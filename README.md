Cryptocurrency ETL Project

This project is a simple ETL (Extract, Transform, Load) pipeline for cryptocurrency data using the CoinGecko API. It fetches the latest market data, transforms it by cleaning and validating each record, and then loads it into a local SQLite database.

Project Structure

extract_data: Sends a request to the CoinGecko API and retrieves a list of the top cryptocurrencies by market cap.
transform_data: Cleans and validates the data to ensure accurate types, checks for any missing values, and verifies that numbers are positive.
load_data: Stores the transformed data into a table in the SQLite database (crypto_data.db).
verify_data: Queries the database to check that data was loaded correctly.
Setup

Install the required packages:

pip install pandas requests
How to Run

To run the pipeline, execute the etl_process() function in your Python environment. This will log the process and output any issues to etl_project.log.

Logs and Output

All activity is logged in etl_project.log, and the final output (cryptocurrency data) is stored in the crypto_data.db SQLite database.
