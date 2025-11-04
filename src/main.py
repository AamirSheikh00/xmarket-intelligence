import logging
import time
from src.scraper import run_selenium_scraper
from src.processor import process_and_store_data
from src.analysis import run_analysis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    start_time = time.time()

    # Phase 1: Data Collection
    logging.info("Starting Phase 1: Data Collection...")
    try:
        raw_tweets = run_selenium_scraper()
        if not raw_tweets:
            logging.error("No tweets were fetched.")
            return
        logging.info(f"Collected {len(raw_tweets)} raw tweets.")
    except Exception as e:
        logging.error(f"Error during data collection: {e}")
        return

    # Phase 2: Data Processing
    logging.info("Starting Phase 2: Data Processing...")
    try:
        processed_df = process_and_store_data(raw_tweets)
        if processed_df.empty:
            logging.error("No data was saved after processing.")
            return
        logging.info(f"Processed and saved {len(processed_df)} tweets.")
    except Exception as e:
        logging.error(f"Error during data processing: {e}")
        return

    # Phase 3: Analysis & Visualization
    logging.info("Starting Phase 3: Analysis & Visualization...")
    try:
        run_analysis()
        logging.info("Analysis completed successfully.")
    except Exception as e:
        logging.error(f"Error during analysis: {e}")
        return

    end_time = time.time()
    logging.info(f"Pipeline finished in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
