import pandas as pd
import logging
import os
import re
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

OUTPUT_DIR = "output"
PARQUET_FILE = os.path.join(OUTPUT_DIR, "tweets.parquet")
TIMEFRAME_HOURS = 24

def clean_tweet_content(text):
    """Clean tweet text by removing URLs, mentions, hashtags, and non-ASCII characters."""
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"#\w+", "", text)
    text = re.sub(r"[^A-Za-z0-9\s.,!?$₹]+", "", text)
    return text.strip()

def process_and_store_data(raw_tweets):
    """Clean, normalize, and store collected tweet data in Parquet format."""
    if not raw_tweets:
        logging.warning("No raw tweets to process.")
        return pd.DataFrame()

    logging.info(f"Processing {len(raw_tweets)} raw tweets...")
    df = pd.DataFrame(raw_tweets)

    # 1. Deduplication
    df.drop_duplicates(subset=["tweet_id"], keep="first", inplace=True)

    # 2. Normalize timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df.dropna(subset=["timestamp"], inplace=True)

    # Ensure timezone awareness
    try:
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
    except TypeError:
        df["timestamp"] = df["timestamp"].apply(
            lambda ts: ts.tz_localize("UTC") if ts.tzinfo is None else ts.astimezone(timezone.utc)
        )

    # 3. Filter tweets from the last 24 hours
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=TIMEFRAME_HOURS)
    df = df[df["timestamp"] >= cutoff_time].copy()

    if df.empty:
        logging.warning("No tweets found from the last 24 hours after filtering.")
        return pd.DataFrame()

    logging.info(f"Found {len(df)} tweets from the last 24 hours.")

    # 4. Convert engagement metrics
    df["likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(0).astype(int)
    df["retweets"] = pd.to_numeric(df["retweets"], errors="coerce").fillna(0).astype(int)
    df["comments"] = pd.to_numeric(df["comments"], errors="coerce").fillna(0).astype(int)

    # 5. Clean content
    df["cleaned_content"] = df["content"].apply(clean_tweet_content)

    # 6. Normalize list fields
    df["mentions"] = df["mentions"].apply(lambda x: x if isinstance(x, list) else [])
    df["hashtags"] = df["hashtags"].apply(lambda x: x if isinstance(x, list) else [])

    # 7. Final schema
    final_columns = [
        "tweet_id", "timestamp", "username", "cleaned_content",
        "likes", "retweets", "comments", "mentions", "hashtags", "content"
    ]
    df_final = df[final_columns]

    # 8. Store as Parquet
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    try:
        df_final.to_parquet(PARQUET_FILE, engine="pyarrow", index=False)
        logging.info(f"Saved {len(df_final)} processed tweets to {PARQUET_FILE}")
    except Exception as e:
        logging.error(f"Failed to save data to Parquet: {e}")

    return df_final
