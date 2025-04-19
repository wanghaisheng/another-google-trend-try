import sqlite3
import pandas as pd
from pytrends.request import TrendReq
import time
import datetime
import os
import logging
import random

# --- Configuration ---
# !!! Populate this list with keywords from the Backlinko image or your chosen ones
ROOT_KEYWORDS = [
    'youtube', 'facebook', 'amazon', 'google', 'weather', 'translate', 'com',
    'instagram', 'walmart', 'ebay', 'yahoo', 'youtubec', 'you', 'netflix',
    'news', 'craigslist', 'mail', 'roblox', 'gmail', 'trump', 'twitter',
    'map', 'fox', 'target', '123movies', 'coronavirus', 'nfl', 'the youtube',
    'maps', 'pinterest', 'calculator', 'ups', 'espn', 'classroom', 'hotmail',
    'macy\'s', 'bitcoin', 'linkedin', 'nba', 'msn', 'usps', 'food', 'near',
    'tiktok', 'login', 'covid', 'fox news', 'tv', 'games', 'on'
]
# Limit root keywords for testing if needed:
# ROOT_KEYWORDS = ['youtube', 'weather', 'bitcoin']

MAX_LEVELS = 3  # How deep to iterate (Level 1 = root, Level 2 = rising from root, etc.)
TIMEZONE = 360  # US Central Timezone offset in minutes from UTC (adjust if needed)
LANGUAGE = 'en-US'
GEOLOCATION = 'US' # Target country
TIMEFRAME = 'today 3-m' # Trends timeframe (last 3 months)
SLEEP_DELAY_MIN = 5  # Minimum seconds to wait between requests
SLEEP_DELAY_MAX = 10 # Maximum seconds to wait between requests
DATA_DIR = "data"
DB_FILENAME_FORMAT = "{date}.db"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions ---
def init_db(db_path):
    """Initializes the SQLite database and creates the table if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trends_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level INTEGER NOT NULL,
            root_keyword TEXT NOT NULL,
            parent_keyword TEXT NOT NULL,
            discovered_keyword TEXT NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('Query', 'Topic')),
            rising_value TEXT, -- Stores '% increase' or 'Breakout'
            search_date TEXT NOT NULL
        )
    ''')
    conn.commit()
    logging.info(f"Database initialized at {db_path}")
    return conn

def insert_trend_data(conn, data):
    """Inserts a row of trend data into the database."""
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO trends_data (level, root_keyword, parent_keyword, discovered_keyword, type, rising_value, search_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (data['level'], data['root_keyword'], data['parent_keyword'], data['discovered_keyword'], data['type'], data['rising_value'], data['search_date']))
        conn.commit()
        # logging.debug(f"Inserted: {data['discovered_keyword']} (Parent: {data['parent_keyword']})")
    except sqlite3.Error as e:
        logging.error(f"Database error inserting {data['discovered_keyword']}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error inserting {data['discovered_keyword']}: {e}")


# --- Google Trends Functions ---
def get_rising_trends(pytrends_instance, keyword, timeframe, geo, lang, tz):
    """Fetches rising related queries and topics for a given keyword."""
    rising_results = []
    delay = random.uniform(SLEEP_DELAY_MIN, SLEEP_DELAY_MAX)
    logging.info(f"Querying trends for: '{keyword}'. Waiting {delay:.2f}s...")
    time.sleep(delay)

    try:
        pytrends_instance.build_payload([keyword], cat=0, timeframe=timeframe, geo=geo, gprop='')

        # Get Related Queries
        related_queries = pytrends_instance.related_queries()
        if keyword in related_queries and 'rising' in related_queries[keyword] and isinstance(related_queries[keyword]['rising'], pd.DataFrame):
            rising_df_queries = related_queries[keyword]['rising']
            for index, row in rising_df_queries.iterrows():
                rising_results.append({
                    'keyword': row['query'],
                    'type': 'Query',
                    'value': str(row['value']) # Store value (e.g., 5000 for +5000% or could be Breakout if pytrends changes)
                })
            logging.info(f"Found {len(rising_df_queries)} rising queries for '{keyword}'")
        else:
             logging.warning(f"No rising queries data found for '{keyword}' or unexpected format.")


        # Get Related Topics (Requires slight delay sometimes)
        time.sleep(random.uniform(1, 3)) # Small extra delay before related topics
        related_topics = pytrends_instance.related_topics()
        if keyword in related_topics and 'rising' in related_topics[keyword] and isinstance(related_topics[keyword]['rising'], pd.DataFrame):
             rising_df_topics = related_topics[keyword]['rising']
             # The topic DataFrame has 'topic_title' and 'topic_type'
             for index, row in rising_df_topics.iterrows():
                 # Use 'topic_title' as the keyword discovered
                 rising_results.append({
                    'keyword': row['topic_title'],
                    'type': 'Topic',
                    'value': str(row['value']) # Store value
                 })
             logging.info(f"Found {len(rising_df_topics)} rising topics for '{keyword}'")
        else:
             logging.warning(f"No rising topics data found for '{keyword}' or unexpected format.")

    except Exception as e:
        logging.error(f"Error fetching trends for '{keyword}': {e}")
        # Consider specific error handling for rate limits (TooManyRequestsError if pytrends raises it)
    return rising_results

# --- Main Scraping Logic ---
def scrape_trends_iteratively(root_keywords, max_levels, db_conn):
    """Performs the iterative scraping process."""
    pytrends = TrendReq(hl=LANGUAGE, tz=TIMEZONE)
    today_str = datetime.date.today().isoformat()

    # Use a set to avoid processing the same keyword multiple times within a run
    processed_keywords = set()
    # Queue stores tuples: (keyword_to_process, current_level, root_keyword_origin)
    keywords_to_process = [(kw, 1, kw) for kw in root_keywords]

    processed_count = 0

    while keywords_to_process:
        current_keyword, current_level, root_kw_origin = keywords_to_process.pop(0) # FIFO queue

        if current_keyword in processed_keywords:
            logging.info(f"Skipping already processed: '{current_keyword}'")
            continue

        if current_level > max_levels:
            logging.info(f"Max level reached for branch starting from '{current_keyword}' (Root: {root_kw_origin})")
            continue

        processed_keywords.add(current_keyword)
        processed_count += 1
        logging.info(f"Processing Level {current_level}: '{current_keyword}' (Root: {root_kw_origin}) - Item {processed_count}")

        rising_items = get_rising_trends(pytrends, current_keyword, TIMEFRAME, GEOLOCATION, LANGUAGE, TIMEZONE)

        if not rising_items:
            logging.warning(f"No rising items found for '{current_keyword}'. Branch terminated.")
            continue

        for item in rising_items:
            db_data = {
                'level': current_level,
                'root_keyword': root_kw_origin,
                'parent_keyword': current_keyword,
                'discovered_keyword': item['keyword'],
                'type': item['type'],
                'rising_value': item['value'],
                'search_date': today_str
            }
            insert_trend_data(db_conn, db_data)

            # Add the newly discovered keyword to the queue for the next level
            if current_level < max_levels:
                 # Avoid adding duplicates to the processing queue immediately if they were just processed
                 if item['keyword'] not in processed_keywords:
                    keywords_to_process.append((item['keyword'], current_level + 1, root_kw_origin))
                 else:
                    logging.debug(f"'{item['keyword']}' was recently processed, not adding to queue again immediately.")


    logging.info(f"Scraping complete. Processed {processed_count} unique keywords/topics.")

# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()
    logging.info("Starting Google Trends iterative scraping process...")

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Setup database
    today_date_str = datetime.date.today().strftime('%Y-%m-%d')
    db_filename = DB_FILENAME_FORMAT.format(date=today_date_str)
    db_path = os.path.join(DATA_DIR, db_filename)
    conn = None # Initialize conn to None

    try:
        conn = init_db(db_path)
        scrape_trends_iteratively(ROOT_KEYWORDS, MAX_LEVELS, conn)
    except Exception as e:
        logging.exception("An unexpected error occurred during the main execution.") # Log the full traceback
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    end_time = time.time()
    logging.info(f"Script finished in {end_time - start_time:.2f} seconds.")
    logging.info(f"Data saved to: {db_path}")
