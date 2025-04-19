# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
from trendspy import Trends # Import the new library
# Note: trendspy might raise requests exceptions, but let's catch broadly first
# from requests.exceptions import RequestException
import time
import datetime
import os
import logging
import random
import sys

# --- Configuration ---
CSV_ROOT_KEYWORDS_PATH = 'L1.csv' # Path to your CSV file
TEST_KEYWORD = 'game' # Keyword for the pre-check test
MAX_LEVELS = 3  # How deep to iterate
# BATCH_SIZE = 5 # REMOVED - trendspy likely doesn't support batching here
TIMEZONE_STR = 'UTC' # trendspy might prefer string timezone names? Check docs/examples if needed. Standard TZ offset might work too. Let's start simple.
# TZ Offset: If needed, pytrends used 360 for CST. Unclear trendspy format.
LANGUAGE = 'en-US' # Host Language
GEOLOCATION = 'US' # Target country
TIMEFRAME = 'today 3-m' # Trends timeframe (trendspy seems flexible)
SLEEP_DELAY_AFTER_KEYWORD_MIN = 8  # Min seconds *after processing* a keyword fully
SLEEP_DELAY_AFTER_KEYWORD_MAX = 15 # Max seconds *after processing* a keyword fully
SLEEP_DELAY_BETWEEN_CALLS = 1 # Small delay between queries/topics calls for the *same* keyword
RETRY_SLEEP = 45    # Seconds to wait before retrying a failed API call
MAX_RETRIES = 2     # Max number of retries for a failed API call
DATA_DIR = "data"
DB_FILENAME_FORMAT = "trendspy_{date}.db" # Different DB name prefix

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions ---
# (init_db and insert_trend_data remain the same, but using new DB name format)
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
            rising_value TEXT, -- Will likely be 'N/A' for trendspy
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
    except sqlite3.Error as e:
        logging.error(f"Database error inserting {data['discovered_keyword']}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error inserting {data['discovered_keyword']}: {e}")


# --- Trendspy API Function ---
# NEW function to handle single keyword calls with trendspy
def get_related_terms_single(trends_instance: Trends, keyword: str, timeframe: str, geo: str, lang: str) -> list:
    """Fetches related queries and topics for a SINGLE keyword using trendspy."""
    results = []
    logging.debug(f"Fetching related terms for single keyword: '{keyword}'")

    # --- Get Related Queries ---
    related_queries_data = None
    try:
        logging.debug(f"Attempting related_queries for '{keyword}'")
        # Pass parameters directly if the method supports them (assumption)
        related_queries_data = trends_instance.related_queries(keyword, timeframe=timeframe, geo=geo, hl=lang)
        logging.debug(f"related_queries call successful for '{keyword}'")

        if related_queries_data:
            # Process results - *** ASSUMING list of objects/dicts ***
            for item in related_queries_data:
                 discovered_kw = None
                 if isinstance(item, dict):
                     discovered_kw = item.get('query', item.get('keyword')) # Guessing keys
                 elif hasattr(item, 'query'):
                     discovered_kw = item.query
                 elif hasattr(item, 'keyword'):
                     discovered_kw = item.keyword
                 elif isinstance(item, str): # If it just returns strings
                     discovered_kw = item

                 if discovered_kw:
                     results.append({
                         'keyword': str(discovered_kw), # Ensure string
                         'type': 'Query',
                         'value': 'N/A' # Rising/Top status unknown with trendspy (assumption)
                     })
            logging.debug(f"Processed {len(results)} related queries for '{keyword}'.")
        else:
            logging.debug(f"No related queries data structure returned for '{keyword}'.")

    except Exception as e:
        logging.error(f"Error during related_queries call for '{keyword}': {e}", exc_info=True)
        # Continue to try fetching topics even if queries fail

    # Small delay between call types
    time.sleep(SLEEP_DELAY_BETWEEN_CALLS)

    # --- Get Related Topics ---
    related_topics_data = None
    initial_results_count = len(results) # Count before adding topics
    try:
        logging.debug(f"Attempting related_topics for '{keyword}'")
        related_topics_data = trends_instance.related_topics(keyword, timeframe=timeframe, geo=geo, hl=lang)
        logging.debug(f"related_topics call successful for '{keyword}'")

        if related_topics_data:
             # Process results - *** ASSUMING list of objects/dicts ***
            for item in related_topics_data:
                 discovered_kw = None
                 if isinstance(item, dict):
                     discovered_kw = item.get('topic_title', item.get('keyword')) # Guessing keys
                 elif hasattr(item, 'topic_title'):
                      discovered_kw = item.topic_title
                 elif hasattr(item, 'keyword'):
                     discovered_kw = item.keyword
                 elif isinstance(item, str): # If it just returns strings
                     discovered_kw = item

                 if discovered_kw:
                     results.append({
                         'keyword': str(discovered_kw), # Ensure string
                         'type': 'Topic',
                         'value': 'N/A' # Rising/Top status unknown
                     })
            logging.debug(f"Processed {len(results) - initial_results_count} related topics for '{keyword}'.")
        else:
            logging.debug(f"No related topics data structure returned for '{keyword}'.")

    except Exception as e:
        logging.error(f"Error during related_topics call for '{keyword}': {e}", exc_info=True)
        # Function will return whatever was gathered from queries if topics fail

    logging.debug(f"Finished fetching related terms for '{keyword}'. Found {len(results)} total.")
    return results


# --- Main Scraping Logic ---
# MODIFIED function for single calls and manual retries
def scrape_trends_iteratively(root_keywords_list, max_levels, db_conn):
    """Performs iterative scraping using trendspy (single keyword calls)."""
    try:
        trends = Trends() # Initialize trendspy instance
        # Potentially set proxy here if needed: trends.set_proxy(...)
        logging.info("Trendspy instance initialized.")
    except Exception as e:
        logging.error("Failed to initialize Trendspy instance.", exc_info=True)
        return # Cannot proceed

    today_str = datetime.date.today().isoformat()
    processed_keywords = set()
    keywords_to_process = [(kw, 1, kw) for kw in root_keywords_list]
    processed_count = 0
    item_num = 0 # Use simple counter instead of batch num

    while keywords_to_process:
        item_num += 1
        # --- Get next keyword ---
        current_keyword, current_level, root_kw_origin = keywords_to_process.pop(0) # FIFO

        logging.info(f"\n--- Processing Item {item_num} / Level {current_level} ---")
        logging.info(f"Keyword: '{current_keyword}' (Root: {root_kw_origin})")
        logging.info(f"Queue size remaining: {len(keywords_to_process)}")

        # --- Check if already processed or max level ---
        if current_keyword in processed_keywords:
            logging.debug(f"Skipping already processed: '{current_keyword}'")
            continue

        if current_level > max_levels:
            logging.debug(f"Max level ({max_levels}) reached for '{current_keyword}'. Will not process.")
            # Mark as processed to avoid re-queueing if encountered again
            processed_keywords.add(current_keyword)
            continue

        # --- Attempt to Fetch Data with Retries ---
        related_items = None
        retries = 0
        success = False
        while retries <= MAX_RETRIES and not success:
            try:
                # Call the function that handles *both* queries and topics internally
                related_items = get_related_terms_single(
                    trends, current_keyword, TIMEFRAME, GEOLOCATION, LANGUAGE
                )
                success = True # If no exception was raised, the call sequence completed

            # Catch broad exceptions here, as specific trendspy/requests errors aren't documented clearly
            except Exception as e:
                retries += 1
                logging.error(f"Error fetching data for '{current_keyword}' (Attempt {retries}/{MAX_RETRIES+1}): {e}", exc_info=False) # Don't need full traceback on retry logs
                if retries > MAX_RETRIES:
                    logging.error(f"Max retries failed for '{current_keyword}'. Skipping.")
                    break # Stop retrying
                else:
                    logging.warning(f"Retrying in {RETRY_SLEEP}s...")
                    time.sleep(RETRY_SLEEP)

        # --- Mark as processed regardless of success to avoid infinite loops ---
        processed_keywords.add(current_keyword)
        if success:
             processed_count += 1 # Increment count only if fetch logic completed

        # --- Process Results (if fetch succeeded) ---
        if success and related_items:
            logging.info(f"Found {len(related_items)} related items for '{current_keyword}'.")
            for item in related_items:
                # --- Insert into DB ---
                db_data = {
                    'level': current_level, # Level of the parent
                    'root_keyword': root_kw_origin,
                    'parent_keyword': current_keyword,
                    'discovered_keyword': item['keyword'],
                    'type': item['type'],
                    'rising_value': item['value'], # Will be 'N/A'
                    'search_date': today_str
                }
                insert_trend_data(db_conn, db_data)

                # --- Add to Queue for Next Level ---
                next_level = current_level + 1
                if next_level <= max_levels:
                    discovered_kw = item['keyword']
                    if discovered_kw not in processed_keywords and not any(q[0] == discovered_kw for q in keywords_to_process):
                         keywords_to_process.append((discovered_kw, next_level, root_kw_origin))
                         logging.debug(f"Added to queue: '{discovered_kw}' (L{next_level}, Root: {root_kw_origin})")
                    else:
                         logging.debug(f"'{discovered_kw}' already processed or in queue, not adding again.")
        elif success:
            logging.info(f"No related items found for '{current_keyword}'.")
        else:
             logging.warning(f"Data fetch failed for '{current_keyword}' after retries.")

        # --- Sleep After Processing Each Keyword ---
        sleep_duration = random.uniform(SLEEP_DELAY_AFTER_KEYWORD_MIN, SLEEP_DELAY_AFTER_KEYWORD_MAX)
        logging.debug(f"Sleeping for {sleep_duration:.2f}s after processing '{current_keyword}'...")
        time.sleep(sleep_duration)


    logging.info(f"\nScraping complete. Attempted processing for {item_num} items (unique processed count might differ due to skips/retries).")


# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()
    logging.info("Starting Google Trends iterative scraping process (trendspy)...")

    # --- Load Root Keywords from CSV ---
    root_keywords_from_csv = []
    try:
        # (CSV Loading logic remains the same)
        if not os.path.exists(CSV_ROOT_KEYWORDS_PATH):
             logging.error(f"Error: Root keyword CSV file not found at '{CSV_ROOT_KEYWORDS_PATH}'. Please create it.")
             sys.exit(1)
        df_keywords = pd.read_csv(CSV_ROOT_KEYWORDS_PATH)
        if 'RootKeyword' not in df_keywords.columns:
             logging.error(f"Error: CSV file '{CSV_ROOT_KEYWORDS_PATH}' must contain a column named 'RootKeyword'.")
             sys.exit(1)
        root_keywords_from_csv = df_keywords['RootKeyword'].dropna().astype(str).tolist()
        if not root_keywords_from_csv:
              logging.error(f"Error: No keywords found in '{CSV_ROOT_KEYWORDS_PATH}' or the 'RootKeyword' column is empty.")
              sys.exit(1)
        logging.info(f"Successfully loaded {len(root_keywords_from_csv)} root keywords from {CSV_ROOT_KEYWORDS_PATH}")
    except pd.errors.EmptyDataError:
         logging.error(f"Error: The CSV file '{CSV_ROOT_KEYWORDS_PATH}' is empty.")
         sys.exit(1)
    except Exception as e:
        logging.error(f"Error reading root keywords from CSV '{CSV_ROOT_KEYWORDS_PATH}': {e}", exc_info=True)
        sys.exit(1)
    # --- End Load Root Keywords ---

    # --- Initialize Trendspy Instance for Test ---
    try:
        trends_test_instance = Trends()
        logging.info("Trendspy instance initialized for pre-check test.")
    except Exception as e:
        logging.error("Failed to initialize Trendspy instance for test.", exc_info=True)
        sys.exit(1)
    # --- End Initialize ---

    # --- Run Pre-Check Test ---
    logging.info(f"--- Running pre-check test with keyword: '{TEST_KEYWORD}' ---")
    test_passed = False
    try:
        # Call the function that wraps the API calls
        test_results = get_related_terms_single(trends_test_instance, TEST_KEYWORD, TIMEFRAME, GEOLOCATION, LANGUAGE)

        # Check if the function executed without crashing fatally and returned a list (even if empty)
        if isinstance(test_results, list):
            logging.info(f"Pre-check test function executed successfully (returned list).")
            if test_results:
                logging.info(f"Pre-check test found {len(test_results)} related items for '{TEST_KEYWORD}'.")
            else:
                 logging.warning(f"Pre-check test found no related items for '{TEST_KEYWORD}' (this might be normal).")
            test_passed = True
        else:
             logging.error(f"CRITICAL: Pre-check test function did not return a list. Possible silent failure within get_related_terms_single (check logs).")
             test_passed = False

    except Exception as e:
         # Catch any unexpected error *directly* from the test call itself
         logging.error(f"CRITICAL: Pre-check test call failed with unexpected error.", exc_info=True)
         test_passed = False

    if not test_passed:
        logging.error("--- Pre-check test failed. Exiting script. ---")
        sys.exit(1)

    logging.info(f"--- Pre-check test passed. Proceeding with main scraping process. ---")
    # --- End Pre-Check Test ---


    # --- Main Process ---
    os.makedirs(DATA_DIR, exist_ok=True)
    today_date_str = datetime.date.today().strftime('%Y-%m-%d')
    db_filename = DB_FILENAME_FORMAT.format(date=today_date_str)
    db_path = os.path.join(DATA_DIR, db_filename)
    conn = None

    try:
        conn = init_db(db_path)
        scrape_trends_iteratively(root_keywords_from_csv, MAX_LEVELS, conn)
    except Exception as e:
        logging.exception("An unexpected error occurred during the main scraping execution.")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    end_time = time.time()
    logging.info(f"Script finished in {end_time - start_time:.2f} seconds.")
    logging.info(f"Data saved to: {db_path}")
