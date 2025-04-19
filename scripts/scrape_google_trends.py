import sqlite3
import pandas as pd
from pytrends.request import TrendReq
import time
import datetime
import os
import logging
import random
import sys # Import sys for sys.exit()

# --- Configuration ---
# Define the path to your CSV file
CSV_ROOT_KEYWORDS_PATH = 'L1.csv' # <-- MODIFIED: Path to CSV

# ROOT_KEYWORDS = [...] # <-- REMOVED: No longer hardcoding the list here

MAX_LEVELS = 3
TIMEZONE = 360
LANGUAGE = 'en-US'
GEOLOCATION = 'US'
TIMEFRAME = 'today 3-m'
SLEEP_DELAY_MIN = 5
SLEEP_DELAY_MAX = 10
DATA_DIR = "data"
DB_FILENAME_FORMAT = "{date}.db"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions ---
# (init_db function remains the same)
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
            rising_value TEXT,
            search_date TEXT NOT NULL
        )
    ''')
    conn.commit()
    logging.info(f"Database initialized at {db_path}")
    return conn

# (insert_trend_data function remains the same)
def insert_trend_data(conn, data):
    """Inserts a row of trend data into the database."""
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO trends_data (level, root_keyword, parent_keyword, discovered_keyword, type, rising_value, search_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (data['level'], data['root_keyword'], data['parent_keyword'], data['discovered_keyword'], data['type'], data['rising_value'], data['search_date']))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database error inserting {data['discovered_keyword']}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error inserting {data['discovered_keyword']}: {e}")


# --- Google Trends Functions ---
# (get_rising_trends function remains the same)
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
                    'value': str(row['value'])
                })
            logging.info(f"Found {len(rising_df_queries)} rising queries for '{keyword}'")
        else:
             logging.warning(f"No rising queries data found for '{keyword}' or unexpected format.")


        # Get Related Topics
        time.sleep(random.uniform(1, 3)) # Small extra delay
        related_topics = pytrends_instance.related_topics()
        if keyword in related_topics and 'rising' in related_topics[keyword] and isinstance(related_topics[keyword]['rising'], pd.DataFrame):
             rising_df_topics = related_topics[keyword]['rising']
             for index, row in rising_df_topics.iterrows():
                 rising_results.append({
                    'keyword': row['topic_title'],
                    'type': 'Topic',
                    'value': str(row['value'])
                 })
             logging.info(f"Found {len(rising_df_topics)} rising topics for '{keyword}'")
        else:
             logging.warning(f"No rising topics data found for '{keyword}' or unexpected format.")

    except Exception as e:
        logging.error(f"Error fetching trends for '{keyword}': {e}")
    return rising_results


# --- Main Scraping Logic ---
# (scrape_trends_iteratively function remains the same, it accepts the list as input)
def scrape_trends_iteratively(root_keywords_list, max_levels, db_conn): # Changed arg name for clarity
    """Performs the iterative scraping process."""
    pytrends = TrendReq(hl=LANGUAGE, tz=TIMEZONE)
    today_str = datetime.date.today().isoformat()

    processed_keywords = set()
    keywords_to_process = [(kw, 1, kw) for kw in root_keywords_list] # Use the passed list
    processed_count = 0

    while keywords_to_process:
        current_keyword, current_level, root_kw_origin = keywords_to_process.pop(0)

        if current_keyword in processed_keywords:
            logging.debug(f"Skipping already processed: '{current_keyword}'") # Changed to debug
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

            if current_level < max_levels:
                 if item['keyword'] not in processed_keywords:
                    keywords_to_process.append((item['keyword'], current_level + 1, root_kw_origin))
                 else:
                    logging.debug(f"'{item['keyword']}' was recently processed, not adding to queue again immediately.")

    logging.info(f"Scraping complete. Processed {processed_count} unique keywords/topics.")


# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()
    logging.info("Starting Google Trends iterative scraping process...")

    # --- Load Root Keywords from CSV --- <-- MODIFIED SECTION
    root_keywords_from_csv = []
    try:
        if not os.path.exists(CSV_ROOT_KEYWORDS_PATH):
            logging.error(f"Error: Root keyword CSV file not found at '{CSV_ROOT_KEYWORDS_PATH}'. Please create it.")
            sys.exit(1) # Exit if CSV not found

        df_keywords = pd.read_csv(CSV_ROOT_KEYWORDS_PATH)

        # Check if the required column exists
        if 'RootKeyword' not in df_keywords.columns:
            logging.error(f"Error: CSV file '{CSV_ROOT_KEYWORDS_PATH}' must contain a column named 'RootKeyword'.")
            sys.exit(1) # Exit if column is missing

        # Drop rows where 'RootKeyword' is NaN or empty, convert to string list
        root_keywords_from_csv = df_keywords['RootKeyword'].dropna().astype(str).tolist()

        if not root_keywords_from_csv:
             logging.error(f"Error: No keywords found in '{CSV_ROOT_KEYWORDS_PATH}' or the 'RootKeyword' column is empty.")
             sys.exit(1) # Exit if no keywords loaded

        logging.info(f"Successfully loaded {len(root_keywords_from_csv)} root keywords from {CSV_ROOT_KEYWORDS_PATH}")

    except pd.errors.EmptyDataError:
         logging.error(f"Error: The CSV file '{CSV_ROOT_KEYWORDS_PATH}' is empty.")
         sys.exit(1) # Exit if CSV is empty
    except Exception as e:
        logging.error(f"Error reading root keywords from CSV '{CSV_ROOT_KEYWORDS_PATH}': {e}")
        sys.exit(1) # Exit on other read errors
    # --- End Load Root Keywords ---


    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Setup database
    today_date_str = datetime.date.today().strftime('%Y-%m-%d')
    db_filename = DB_FILENAME_FORMAT.format(date=today_date_str)
    db_path = os.path.join(DATA_DIR, db_filename)
    conn = None

    try:
        conn = init_db(db_path)
        # Pass the list loaded from CSV to the scraping function
        scrape_trends_iteratively(root_keywords_from_csv, MAX_LEVELS, conn) # <-- MODIFIED: Pass loaded list
    except Exception as e:
        logging.exception("An unexpected error occurred during the main execution.")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    end_time = time.time()
    logging.info(f"Script finished in {end_time - start_time:.2f} seconds.")
    logging.info(f"Data saved to: {db_path}")
