import logging
import time
import random
import json
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

HASHTAGS = ["#nifty50", "#sensex", "#intraday", "#banknifty"]
TWEETS_PER_HASHTAG = 250
MIN_TWEETS_TO_MEET_GOAL = 500
COOKIE_FILE = "cookies.json"


def get_driver():
    """Initializes a Selenium WebDriver instance (configured for Chrome or Brave)."""
    options = webdriver.ChromeOptions()

    # Update this path if using Brave instead of Chrome
    brave_path = r"/usr/bin/google-chrome"

    if not os.path.exists(brave_path):
        logging.error(f"Browser executable not found at: {brave_path}")
        return None

    options.binary_location = brave_path
    options.add_argument("--start-maximized")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def load_cookies(driver):
    """Loads stored cookies to restore the login session."""
    if not os.path.exists(COOKIE_FILE):
        logging.error(f"{COOKIE_FILE} not found. Please export cookies from x.com and save them locally.")
        driver.quit()
        return False

    logging.info(f"Loading cookies from {COOKIE_FILE}...")
    with open(COOKIE_FILE, "r") as f:
        cookies = json.load(f)

    driver.get("https://x.com")

    for cookie in cookies:
        if "expires" in cookie:
            cookie["expires"] = int(cookie["expires"])
        if "sameSite" in cookie and cookie["sameSite"] not in ["Strict", "Lax", "None"]:
            del cookie["sameSite"]
        try:
            driver.add_cookie(cookie)
        except Exception:
            continue

    driver.refresh()
    time.sleep(3)
    return True


def parse_tweet(element):
    """Parses a single tweet element into a structured dictionary."""
    try:
        # Tweet ID and timestamp
        tweet_id = "N/A"
        timestamp = "N/A"
        try:
            time_element = element.find_element(By.TAG_NAME, "time")
            timestamp = time_element.get_attribute("datetime")
            link_element = time_element.find_element(By.XPATH, "./..")
            tweet_id = link_element.get_attribute("href").split("/")[-1]
        except NoSuchElementException:
            pass

        # Username
        try:
            user_data = element.find_element(By.XPATH, ".//div[@data-testid='User-Name']")
            username = user_data.find_element(By.TAG_NAME, "span").text
        except NoSuchElementException:
            username = "N/A"

        # Tweet text
        try:
            content = element.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
        except NoSuchElementException:
            content = ""

        # Engagement metrics
        def get_stat(testid):
            try:
                stat_element = element.find_element(By.XPATH, f".//div[@data-testid='{testid}']")
                stat_text = stat_element.find_element(By.XPATH, ".//span[@data-testid='app-text']").text
                if "K" in stat_text:
                    return int(float(stat_text.replace("K", "")) * 1000)
                if "M" in stat_text:
                    return int(float(stat_text.replace("M", "")) * 1_000_000)
                return int(stat_text) if stat_text else 0
            except (NoSuchElementException, ValueError):
                return 0

        comments = get_stat("reply")
        retweets = get_stat("retweet")
        likes = get_stat("like")

        mentions = [m.text for m in element.find_elements(By.XPATH, ".//a[contains(text(), '@')]")]
        hashtags = [h.text for h in element.find_elements(By.XPATH, ".//a[contains(text(), '#')]")]

        if content and tweet_id != "N/A":
            return {
                "tweet_id": tweet_id,
                "timestamp": timestamp,
                "username": username,
                "content": content,
                "likes": likes,
                "retweets": retweets,
                "comments": comments,
                "mentions": mentions,
                "hashtags": hashtags,
            }
        return None

    except Exception as e:
        logging.warning(f"Error parsing tweet: {e}")
        return None


def fetch_tweets_for_hashtag(driver, hashtag, seen_ids):
    """Fetches tweets for a given hashtag by scrolling and parsing results."""
    search_url = f"https://x.com/search?q={hashtag.replace('#', '%23')}&src=typed_query&f=live"
    driver.get(search_url)
    logging.info(f"Scraping for: {hashtag}")

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//article[@data-testid='tweet']"))
        )
    except TimeoutException:
        logging.error(f"Timeout while loading tweets for {hashtag}")
        return []

    collected_tweets = []
    tweets_on_page = 0

    while len(collected_tweets) < TWEETS_PER_HASHTAG:
        elements = driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
        if not elements or len(elements) == tweets_on_page:
            logging.info(f"No new tweets found for {hashtag}.")
            break
        tweets_on_page = len(elements)

        for el in elements:
            tweet = parse_tweet(el)
            if tweet and tweet["tweet_id"] not in seen_ids:
                collected_tweets.append(tweet)
                seen_ids.add(tweet["tweet_id"])
                if len(collected_tweets) >= TWEETS_PER_HASHTAG:
                    break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(2.5, 4.5))  # mimic human scrolling

    logging.info(f"Collected {len(collected_tweets)} tweets for {hashtag}")
    return collected_tweets


def run_selenium_scraper():
    """Runs the full Selenium scraping pipeline."""
    driver = None
    all_tweets = []
    try:
        driver = get_driver()
        if not driver:
            return []

        if not load_cookies(driver):
            return []

        seen_ids = set()

        for hashtag in HASHTAGS:
            new_tweets = fetch_tweets_for_hashtag(driver, hashtag, seen_ids)
            all_tweets.extend(new_tweets)
            logging.info(f"Total tweets collected so far: {len(all_tweets)}")

            if len(all_tweets) >= MIN_TWEETS_TO_MEET_GOAL:
                logging.info("Reached target tweet count. Stopping scrape.")
                break

    except Exception as e:
        logging.error(f"Scraping error: {e}")
    finally:
        if driver:
            driver.quit()

    logging.info(f"Total unique tweets collected: {len(all_tweets)}")
    return all_tweets


if __name__ == "__main__":
    raw_tweets = run_selenium_scraper()
    if raw_tweets:
        print(f"\nCollected {len(raw_tweets)} tweets.")
        print("\nSample Tweet:\n", raw_tweets[0])
    else:
        print("No tweets were collected.")
