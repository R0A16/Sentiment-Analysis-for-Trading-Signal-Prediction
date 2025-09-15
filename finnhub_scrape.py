import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os

api_key = "API_KEY"
symbol = "KO"                   
output_file = "finnhub_news_ko.csv"   
start_date = datetime(2024, 9, 1) 
end_date = datetime(2025, 8, 3)   


if os.path.exists(output_file):
    existing_df = pd.read_csv(output_file)
    existing_keys = set(zip(existing_df["date"], existing_df["headline"]))
else:
    existing_df = pd.DataFrame(columns=["date", "headline", "url"])
    existing_keys = set()


def fetch_news_batch(symbol, from_date, to_date, api_key):
    url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={to_date}&token={api_key}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("Rate limit hit. Waiting 60 seconds...")
            time.sleep(60)
            return fetch_news_batch(symbol, from_date, to_date, api_key) 
        else:
            print(f"Error {response.status_code}: {response.text}")
            return []
    except Exception as e:
        print(f"Request failed: {e}")
        return []

# Iterate through dates in 7-day batches
current_date = start_date
all_news = []
while current_date <= end_date:
    batch_end_date = min(current_date + timedelta(days=7), end_date)
    
    # Finnhub uses YYYY-MM-DD
    from_date = current_date.strftime("%Y-%m-%d")
    to_date = batch_end_date.strftime("%Y-%m-%d")
    
    print(f"Fetching news from {from_date} to {to_date}...")
    news_batch = fetch_news_batch(symbol, from_date, to_date, api_key)
    
    for article in news_batch:
        article_date = datetime.fromtimestamp(article["datetime"]).strftime("%Y-%m-%d")
        article_key = (article_date, article["headline"])
        
        if article_key not in existing_keys:
            all_news.append({
                "date": article_date,
                "headline": article["headline"],
                "url": article.get("url", "")
            })
            existing_keys.add(article_key)

    if all_news:
        batch_df = pd.DataFrame(all_news)
        if os.path.exists(output_file):
            batch_df.to_csv(output_file, mode="a", header=False, index=False)
        else:
            batch_df.to_csv(output_file, index=False)

        all_news = []
    
    # Finnhub API limits 60 calls per minute or 1 call per second
    time.sleep(1.2) 
    current_date = batch_end_date + timedelta(days=1) 

print("Done")