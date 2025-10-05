import requests 
import json
from datetime import date
import os
import time
import pandas as pd
import sqlite3
from datetime import datetime
import schedule


def load_data():

    # Extract
    api_key = "IN1S908NKTFZW2X3"
    companies = ["AAPL", "GOOG", "MSFT"]

    folder = "raw_data"
    os.makedirs(folder, exist_ok=True) 

    today = date.today().strftime("%Y-%m-%d")

    # Download json data for each company
    for i in companies:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={i}&apikey={api_key}'
        r = requests.get(url)
        data = r.json()
        
        fileName = f"{folder}/{i}_{today}.json"
        
        with open(fileName, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        # print(f"saved data {fileName}")


    # # transform

    names = ["AAPL", "GOOG", "MSFT"]
    all_dfs = []
    today = date.today().isoformat() 
    for name in names:
        with open(f'raw_data/{name}_{today}.json', "r") as f:
            d = json.load(f)

        time_series = d["Time Series (Daily)"]

        # Convert json into dataframe
        df = pd.DataFrame(time_series).T           
        df.reset_index(inplace=True)       

        df = df.rename(columns={
            "1. open": "open_price",
            "2. high": "high_price",
            "3. low": "low_price",
            "4. close": "close_price",
            "5. volume": "volume"
        })

        # print(df)

        columns = ["open_price", "high_price", "low_price", "close_price", "volume"]

        for i in columns:
            df[i] = df[i].astype(float)

        df["daily_change_percentage"] = ((df["close_price"] - df["open_price"])/df["open_price"])*100

        df.rename(columns={'index': 'date'}, inplace=True)  

        df["symbol"] = name

        all_dfs.append(df)


        print(name, "\n", df.head(4))


    # Merge all companies into one dataframe
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df["extraction_timestamp"] = datetime.now().isoformat()
    print(final_df.head(15))



    # # Load data

    conn = sqlite3.connect("stock_daily_data2.sqlite")
    c = conn.cursor()

    c.execute('''
    CREATE TABLE IF NOT EXISTS stock_daily_data2 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        date TEXT NOT NULL,
        open_price REAL,
        high_price REAL,
        low_price REAL,
        close_price REAL,
        volume REAL,
        daily_change_percentage REAL,
        extraction_timestamp TEXT,
        UNIQUE(symbol, date)
    )
    ''')
    conn.commit()

    for row in final_df.itertuples(index=False):
        c.execute('''
            INSERT OR IGNORE INTO stock_daily_data2
            (symbol, date, open_price, high_price, low_price, close_price, volume, daily_change_percentage, extraction_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row.symbol,
            row.date,
            row.open_price,
            row.high_price,
            row.low_price,
            row.close_price,
            row.volume,
            row.daily_change_percentage,
            datetime.now().isoformat()
        ))

    conn.commit()  
    conn.close()
    print("All data loaded into stock_daily_data")


    # Check Data
    print("check data")
    conn = sqlite3.connect("stock_daily_data2.sqlite")
    c = conn.cursor()

    for row in c.execute('SELECT * FROM stock_daily_data2;'):
        print(row)

    conn.close()

        
    print("Running stock data")


# Scheduling
schedule.every().day.at("20:56:00").do(load_data)

while True:
    schedule.run_pending()
    time.sleep(15)  

# load_data()

















