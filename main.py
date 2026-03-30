from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
import pandas as pd
import datetime
import numpy as np
from sklearn.linear_model import LinearRegression

app = FastAPI(title="QuantPulse: NSE Stock Data API")

# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow any origin for frontend calls
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
DB_NAME = "stock_data.db"
TABLE_NAME = "nse_stocks"
engine = create_engine(f"sqlite:///{DB_NAME}")

COMPANY_MAP = {
    "RELIANCE.NS": {"name": "Reliance Industries", "sector": "Energy/Retail"},
    "TCS.NS": {"name": "Tata Consultancy Services", "sector": "IT"},
    "INFY.NS": {"name": "Infosys", "sector": "IT"},
    "HDFCBANK.NS": {"name": "HDFC Bank", "sector": "Banking"},
    "WIPRO.NS": {"name": "Wipro", "sector": "IT"},
    "ICICIBANK.NS": {"name": "ICICI Bank", "sector": "Banking"},
    "BAJFINANCE.NS": {"name": "Bajaj Finance", "sector": "Finance"},
    "AXISBANK.NS": {"name": "Axis Bank", "sector": "Banking"},
    "MARUTI.NS": {"name": "Maruti Suzuki", "sector": "Automotive"},
    "SUNPHARMA.NS": {"name": "Sun Pharmaceutical", "sector": "Healthcare"}
}

@app.get("/companies")
async def get_companies():
    """Returns a list of all stocks with name and sector."""
    return [{"symbol": s, **info} for s, info in COMPANY_MAP.items()]

@app.get("/data/{symbol}")
async def get_stock_data(symbol: str, limit: int = 30):
    """Returns the last N days of OHLCV + computed metrics for a given symbol."""
    symbol = symbol.upper()
    if symbol not in COMPANY_MAP:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found in our list.")
    
    try:
        query = f"SELECT * FROM {TABLE_NAME} WHERE symbol = '{symbol}' ORDER BY Date DESC LIMIT {limit}"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found for this symbol.")
        
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"Database error for {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while fetching stock data.")

@app.get("/summary/{symbol}")
async def get_stock_summary(symbol: str):
    """Returns 52-week high, low, avg close, and current volatility score."""
    if symbol not in COMPANY_MAP:
        raise HTTPException(status_code=404, detail="Symbol not found.")
    
    query = f"SELECT * FROM {TABLE_NAME} WHERE symbol = '{symbol}' ORDER BY Date DESC"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        raise HTTPException(status_code=404, detail="No data available for summary.")
    
    latest_row = df.iloc[0]
    
    return {
        "symbol": symbol,
        "name": COMPANY_MAP[symbol]["name"],
        "week52_high": latest_row["week52_high"],
        "week52_low": latest_row["week52_low"],
        "avg_close": df["Close"].mean(),
        "volatility_score": latest_row["volatility_score"],
        "last_updated": latest_row["Date"]
    }

@app.get("/compare")
async def compare_stocks(symbol1: str = Query(...), symbol2: str = Query(...), limit: int = 30):
    """Compares two stocks with normalized price comparison."""
    if symbol1 not in COMPANY_MAP or symbol2 not in COMPANY_MAP:
        raise HTTPException(status_code=404, detail="One or both symbols not found.")
    
    def get_normalized_data(sym, n):
        query = f"SELECT Date, Close FROM {TABLE_NAME} WHERE symbol = '{sym}' ORDER BY Date DESC LIMIT {n}"
        df = pd.read_sql(query, engine)
        df = df.sort_values("Date")
        if df.empty: return []
        
        base_price = df.iloc[0]["Close"]
        df["normalized_price"] = (df["Close"] / base_price) * 100
        return df[["Date", "normalized_price"]].to_dict(orient="records")

    return {
        symbol1: get_normalized_data(symbol1, limit),
        symbol2: get_normalized_data(symbol2, limit)
    }

@app.get("/gainers-losers")
async def get_gainers_losers():
    """Identifies top 5 gainers and losers based on the most recent daily_return."""
    latest_date_query = f"SELECT MAX(Date) as max_date FROM {TABLE_NAME}"
    latest_date = pd.read_sql(latest_date_query, engine).iloc[0]["max_date"]
    
    if not latest_date:
        raise HTTPException(status_code=404, detail="No data found in database.")
    
    query = f"SELECT symbol, daily_return, Close FROM {TABLE_NAME} WHERE Date = '{latest_date}'"
    df = pd.read_sql(query, engine)
    
    df["name"] = df["symbol"].apply(lambda x: COMPANY_MAP.get(x, {}).get("name", x))
    
    gainers = df.sort_values("daily_return", ascending=False).head(5)
    losers = df.sort_values("daily_return", ascending=True).head(5)
    
    return {
        "date": latest_date,
        "gainers": gainers.to_dict(orient="records"),
        "losers": losers.to_dict(orient="records")
    }

@app.get("/predict/{symbol}")
async def predict_stock(symbol: str):
    """Simple 7-day prediction using Linear Regression on last 60 days."""
    if symbol not in COMPANY_MAP:
        raise HTTPException(status_code=404, detail="Symbol not found.")
    
    query = f"SELECT Close FROM {TABLE_NAME} WHERE symbol = '{symbol}' ORDER BY Date DESC LIMIT 60"
    df = pd.read_sql(query, engine)
    
    if len(df) < 10:
        raise HTTPException(status_code=400, detail="Insufficient data for prediction.")
    
    # Train on historical data (in chronological order)
    y = df["Close"].values[::-1]
    X = np.arange(len(y)).reshape(-1, 1)
    
    model = LinearRegression()
    model.fit(X, y)
    
    future_X = np.arange(len(y), len(y) + 7).reshape(-1, 1)
    predictions = model.predict(future_X)
    
    return {
        "predictions": predictions.tolist(), 
        "last_price": float(y[-1])
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
