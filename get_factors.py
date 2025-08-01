import yfinance as yf
import pandas as pd
import numpy as np
import requests
import json
import os

# Function to get the current list of S&P 500 tickers
def get_sp500_tickers(csv_file='sp500_companies.csv'):
    try:
        sp500_df = pd.read_csv(csv_file)
        # Using a list comprehension for a more efficient and cleaner approach
        sp500_tickers = [ticker for ticker in sp500_df['Symbol'] if isinstance(ticker, str)]
        return sp500_tickers
    except FileNotFoundError:
        print(f"Error: The file {csv_file} was not found.")
        return []

# Function to collect stock data using the yfinance library
def get_stock_data_yf(tickers, start, end):
    all_stock_data = {}
    for ticker in tickers:
        print(f"Collecting data for {ticker} using yfinance...")
        try:
            # We explicitly set auto_adjust=True to get the adjusted prices
            stock_data = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            
            if not stock_data.empty:
                # The 'Close' column now holds the adjusted price data
                stock_data['Return'] = stock_data['Close'].pct_change()
                stock_data['Return'].replace([np.inf, -np.inf], np.nan, inplace=True)
                all_stock_data[ticker] = stock_data['Return']
            else:
                print(f"No data found for {ticker} using yfinance.")
                
        except Exception as e:
            print(f"Error collecting data for {ticker} using yfinance: {e}")
    return all_stock_data

# Function to collect stock data using the Financial Modeling Prep API
def get_stock_data_fmp(tickers, api_key, start, end):
    base_url = "https://financialmodelingprep.com/api/v3/historical-price/"
    all_stock_data = {}
    for ticker in tickers:
        print(f"Collecting data for {ticker} using FMP...")
        try:
            url = f"{base_url}{ticker}?from={start}&to={end}&apikey={api_key}"
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            data = json.loads(response.text)
            
            if 'historical' in data:
                df = pd.DataFrame(data['historical'])
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df.sort_index(inplace=True)
                df['Return'] = df['adjClose'].pct_change()
                df['Return'].replace([np.inf, -np.inf], np.nan, inplace=True)
                all_stock_data[ticker] = df['Return']
            else:
                print(f"No historical data found for {ticker} on FMP.")
                
        except Exception as e:
            print(f"Error collecting data for {ticker} using FMP: {e}")
    return all_stock_data

# Main execution function
def main():
    # Set the start and end dates for data collection
    START_DATE = '2010-01-01'
    END_DATE = '2020-12-31'
    
    # Financial Modeling Prep API key (if needed as a fallback)
    FMP_API_KEY = os.getenv('FMP_API_KEY')
    
    # Get the list of S&P 500 tickers
    tickers = get_sp500_tickers()
    
    if not tickers:
        print("No tickers to process. Exiting.")
        return

    # First attempt to get data using yfinance
    stock_returns = get_stock_data_yf(tickers, START_DATE, END_DATE)

    # Fallback to FMP for tickers where yfinance failed
    failed_tickers = [ticker for ticker in tickers if ticker not in stock_returns]
    if failed_tickers and FMP_API_KEY:
        print("\nAttempting to collect data for failed tickers using FMP...")
        fmp_returns = get_stock_data_fmp(failed_tickers, FMP_API_KEY, START_DATE, END_DATE)
        stock_returns.update(fmp_returns)
    elif failed_tickers:
        print("\nNo FMP API key provided. Cannot use FMP as a fallback.")
    
    # Convert the dictionary of returns to a single DataFrame
    returns_df = pd.DataFrame(stock_returns)
    print("\nFinal DataFrame of stock returns (first 5 rows):")
    print(returns_df.head())
    
if __name__ == "__main__":
    main()