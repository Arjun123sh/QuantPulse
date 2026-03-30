import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import datetime
import os

# Configuration
SYMBOLS = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "WIPRO.NS",
    "ICICIBANK.NS", "BAJFINANCE.NS", "AXISBANK.NS", "MARUTI.NS", "SUNPHARMA.NS"
]
DB_NAME = "stock_data.db"
TABLE_NAME = "nse_stocks"

def fetch_and_process_data(symbols):
    engine = create_engine(f"sqlite:///{DB_NAME}")
    
    # Calculate date range
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=365 + 400) # Fetch extra buffer for indicators (volatility, 52w high)
    
    all_data = []

    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        try:
            # Download data
            df = yf.download(symbol, start=start_date, end=end_date)
            
            if df.empty:
                print(f"No data found for {symbol}. Skipping.")
                continue
            
            # Reset index to move Date from index to column
            df = df.reset_index()
            
            # Handle multi-index columns if present (sometimes happens with yfinance download)
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten the multi-index for simplicity
                df.columns = [col[0] for col in df.columns]

            # Handle missing values
            df = df.ffill().dropna(subset=['Close'])
            
            # Ensure proper date parsing
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            
            # Computed Columns
            # 1. daily_return
            df['daily_return'] = df['Close'].pct_change()
            
            # 2. moving_avg_7d
            df['moving_avg_7d'] = df['Close'].rolling(window=7).mean()
            
            # 3. week52_high
            df['week52_high'] = df['Close'].rolling(window=252, min_periods=1).max()
            
            # 4. week52_low
            df['week52_low'] = df['Close'].rolling(window=252, min_periods=1).min()
            
            # 5. volatility_score (rolling 30-day std dev of daily_return normalized 0-100)
            rolling_std = df['daily_return'].rolling(window=30).std()
            
            # Normalize volatility_score 0-100 across the available range
            # val_scaled = (val - min) / (max - min) * 100
            min_std = rolling_std.min()
            max_std = rolling_std.max()
            if max_std > min_std:
                df['volatility_score'] = (rolling_std - min_std) / (max_std - min_std) * 100
            else:
                df['volatility_score'] = 0.0
            
            # Add symbol column for storage
            df['symbol'] = symbol
            
            # Filter for only the last 1 year of data as requested
            one_year_ago = datetime.date.today() - datetime.timedelta(days=365)
            df = df[df['Date'] >= one_year_ago]
            
            all_data.append(df)
            
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    if all_data:
        # Combine all dataframes
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Store in SQLite
        print(f"Storing data in {DB_NAME}...")
        final_df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        print("Data storage complete.")
    else:
        print("No data fetched. Check internet connection and symbols.")

if __name__ == "__main__":
    fetch_and_process_data(SYMBOLS)
