import pandas as pd
import logging
import os
import matplotlib.pyplot as plt
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PARQUET_FILE = 'output/tweets.parquet'
PLOT_FILE = 'output/signals_analysis.png'


def download_vader():
    """Ensure the VADER sentiment lexicon is available."""
    try:
        nltk.data.find('sentiment/vader_lexicon.zip')
    except LookupError:
        logging.info("Downloading VADER sentiment lexicon...")
        nltk.download('vader_lexicon')


def perform_sentiment_analysis(df):
    """Apply VADER sentiment analysis to tweets."""
    download_vader()
    analyzer = SentimentIntensityAnalyzer()
    df['sentiment_score'] = df['cleaned_content'].apply(
        lambda x: analyzer.polarity_scores(x)['compound']
    )
    logging.info("Sentiment analysis complete.")
    return df


def aggregate_signals(df):
    """Aggregate sentiment and engagement metrics hourly."""
    if 'timestamp' not in df.columns or df['timestamp'].isnull().all():
        logging.error("Timestamp column missing or invalid.")
        return pd.DataFrame()

    df_resample = df.set_index('timestamp')
    hourly_signals = df_resample.resample('H').agg(
        mean_sentiment=('sentiment_score', 'mean'),
        tweet_volume=('tweet_id', 'count'),
        total_likes=('likes', 'sum'),
        total_retweets=('retweets', 'sum')
    )

    hourly_signals['mean_sentiment'] = hourly_signals['mean_sentiment'].fillna(0)
    logging.info("Hourly signal aggregation complete.")
    return hourly_signals


def plot_signals(hourly_signals):
    """Plot tweet volume and sentiment trend."""
    if hourly_signals.empty:
        logging.warning("No data to plot.")
        return

    fig, ax1 = plt.subplots(figsize=(15, 7))

    color = 'tab:blue'
    ax1.set_xlabel('Time (Last 24 Hours)')
    ax1.set_ylabel('Tweet Volume', color=color)
    ax1.bar(hourly_signals.index, hourly_signals['tweet_volume'], color=color, alpha=0.6, width=0.03)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Mean Sentiment', color=color)
    ax2.plot(hourly_signals.index, hourly_signals['mean_sentiment'], color=color, marker='o')
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.axhline(0, color='grey', linestyle='--', linewidth=0.8)

    fig.tight_layout()
    plt.title('Hourly Market Sentiment vs. Tweet Volume (Last 24h)')
    plt.savefig(PLOT_FILE)
    logging.info(f"Analysis plot saved to {PLOT_FILE}")
    plt.close()


def run_analysis():
    """Run the full tweet sentiment analysis pipeline."""
    try:
        df = pd.read_parquet(PARQUET_FILE)
    except FileNotFoundError:
        logging.error(f"{PARQUET_FILE} not found. Run the scraper first.")
        return
    except Exception as e:
        logging.error(f"Error reading Parquet file: {e}")
        return

    if df.empty:
        logging.warning("No data found in Parquet file.")
        return

    df_with_sentiment = perform_sentiment_analysis(df)
    hourly_signals = aggregate_signals(df_with_sentiment)

    print("\n--- Aggregated Hourly Signals (Sample) ---")
    print(hourly_signals.tail())

    plot_signals(hourly_signals)
