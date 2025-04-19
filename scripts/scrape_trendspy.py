# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
from trendspy import Trends # Import the new library
# Import specific exception if needed, but catch broadly first
# from trendspy.client import TrendsQuotaExceededError
import time
import datetime
import os
import logging
import random
import sys

# --- Configuration ---
CSV_ROOT_KEYWORDS_PATH = 'backlinko_root_keywords.csv' # Path to your CSV file
TEST_KEYWORD = 'game' # Keyword for the pre-check test
MAX_LEVELS = 3  # How deep to iterate
# BATCH_SIZE = 5 # REMOVED
TIMEZONE_STR = 'UTC' # Keep simple for now
LANGUAGE = 'en-US' # Seems unsupported directly in related_ calls
GEOLOCATION = 'US' # Seems unsupported directly in related_ calls
TIMEFRAME = 'today 3-m' # Seems unsupported directly in related_ calls
SLEEP_DELAY_AFTER_KEYWORD_MIN = 8  # Min seconds *after processing* a keyword fully
SLEEP_DELAY_AFTER_KEYWORD_MAX = 15 # Max seconds *after processing* a keyword fully
SLEEP_DELAY_BETWEEN_CALLS = 1 # Small delay between queries/topics calls for the *same* keyword
RETRY_SLEEP = 45    # Seconds to wait before retrying a general failed API call
QUOTA_RETRY_SLEEP_MULTIPLIER = 1.5 # Multiply RETRY_SLEEP by this for quota errors
MAX_RETRIES = 2     # Max number of retries for a failed API call
DATA_DIR = "data"
DB_FILENAME_FORMAT = "trendspy_{date}.db" # Different DB name prefix

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Functions ---
# (init_db and insert_trend_data remain the same)
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
# REVISED to add suggested referer header and use correct parsing keys
def get_related_terms_single(trends_instance: Trends, keyword: str) -> list:
    """Fetches related queries and topics for a SINGLE keyword using trendspy.
       Adds a referer header as suggested by TrendsQuotaExceededError.
       Uses keys 'query' and 'topic_title' based on example notebook.
    """
    results = []
    # Use a plausible referer header
    headers = {'Referer': 'https://trends.google.com/'}
    logging.debug(f"Fetching related terms for single keyword: '{keyword}' using headers: {headers}")

    # --- Get Related Queries ---
    related_queries_data = None
    try:
        logging.debug(f"Attempting related_queries for '{keyword}'")
        # Pass the headers argument - Assuming the methods accept it based on error message
        related_queries_data = trends_instance.related_queries(keyword, headers=headers)
        logging.debug(f"related_queries call successful for '{keyword}'")

        if related_queries_data and isinstance(related_queries_data, list):
            processed_count = 0
            for item in related_queries_data:
                 discovered_kw = None
                 if isinstance(item, dict):
                     discovered_kw = item.get('query') # Key from examples
                 elif hasattr(item, 'query'):
                     discovered_kw = item.query
                 elif isinstance(item, str):
                     discovered_kw = item

                 if discovered_kw:
                     results.append({
                         'keyword': str(discovered_kw),
                         'type': 'Query',
                         'value': 'N/A'
                     })
                     processed_count += 1
                 else:
                     logging.warning(f"Could not extract query from item: {item} for keyword '{keyword}'")
            logging.debug(f"Processed {processed_count} related queries for '{keyword}'.")
        elif related_queries_data:
             logging.warning(f"related_queries for '{keyword}' returned unexpected type: {type(related_queries_data)}")
        else:
            logging.debug(f"No related queries data structure returned for '{keyword}'.")

    # Catch specific Trendspy exceptions if available, otherwise broad Exception
    # It seems trendspy.client might not expose TrendsQuotaExceededError publicly easily,
    # so we might need to rely on the broad Exception or inspect the exception type/message.
    # For now, keep the broad Exception but re-raise specific errors if caught by message later.
    except Exception as e:
        # Check if it's the quota error based on type name or message if possible
        if 'TrendsQuotaExceededError' in str(type(e)):
            logging.error(f"Quota Error during related_queries for '{keyword}': {e}")
            raise e # Re-raise to be caught by the main loop's retry logic
        else:
            logging.error(f"Error during related_queries processing for '{keyword}': {e}", exc_info=True)
            raise e # Re-raise other errors too

    # Small delay between call types
    time.sleep(SLEEP_DELAY_BETWEEN_CALLS)

    # --- Get Related Topics ---
    related_topics_data = None
    initial_results_count = len(results)
    try:
        logging.debug(f"Attempting related_topics for '{keyword}'")
        # Pass the headers argument
        related_topics_data = trends_instance.related_topics(keyword, headers=headers)
        logging.debug(f"related_topics call successful for '{keyword}'")

        if related_topics_data and isinstance(related_topics_data, list):
            processed_count = 0
            for item in related_topics_data:
                 discovered_kw = None
                 if isinstance(item, dict):
                     discovered_kw = item.get('topic_title') # Key from examples
                 elif hasattr(item, 'topic_title'):
                      discovered_kw = item.topic_title
                 elif isinstance(item, str):
                     discovered_kw = item

                 if discovered_kw:
                     results.append({
                         'keyword': str(discovered_kw),
                         'type': 'Topic',
                         'value': 'N/A'
                     })
                     processed_count += 1
                 else:
                      logging.warning(f"Could not extract topic_title from item: {item} for keyword '{keyword}'")
            logging.debug(f"Processed {processed_count} related topics for '{keyword}'.")
        elif related_topics_data:
            logging.warning(f"related_topics for '{keyword}' returned unexpected type: {type(related_topics_data)}")
        else:
            logging.debug(f"No related topics data structure returned for '{keyword}'.")

    except Exception as e:
        if 'TrendsQuotaExceededError' in str(type(e)):
             logging.error(f"Quota Error during related_topics for '{keyword}': {e}")
             raise e # Re-raise for retry logic
        else:
            logging.error(f"Error during related_topics processing for '{keyword}': {e}", exc_info=True)
            raise e # Re-raise other errors

    logging.debug(f"Finished fetching related terms for '{keyword}'. Found {len(results)} total.")
    return results


# --- Main Scraping Logic ---
# REVISED retry block to catch specific quota error message/type
def scrape_trends_iteratively(root_keywords_list, max_levels, db_conn):
    """Performs iterative scraping using trendspy (single keyword calls)."""
    try:
        trends = Trends()
        logging.info("Trendspy instance initialized.")
    except Exception as e:
        logging.error("Failed to initialize Trendspy instance.", exc_info=True)
        return

    today_str = datetime.date.today().isoformat()
    processed_keywords = set()
    keywords_to_process = [(kw, 1, kw) for kw in root_keywords_list]
    processed_count = 0
    item_num = 0

    while keywords_to_process:
        item_num += 1
        current_keyword, current_level, root_kw_origin = keywords_to_process.pop(0)

        logging.info(f"\n--- Processing Item {item_num} / Level {current_level} ---")
        logging.info(f"Keyword: '{current_keyword}' (Root: {root_kw_origin})")
        logging.info(f"Queue size remaining: {len(keywords_to_process)}")

        if current_keyword in processed_keywords:
            logging.debug(f"Skipping already processed: '{current_keyword}'")
            continue

        if current_level > max_levels:
            logging.debug(f"Max level ({max_levels}) reached for '{current_keyword}'. Will not process.")
            processed_keywords.add(current_keyword)
            continue

        # --- Attempt to Fetch Data with Retries ---
        related_items = None
        retries = 0
        success = False
        while retries <= MAX_RETRIES and not success:
            is_quota_error = False # Flag to check error type
            try:
                related_items = get_related_terms_single(trends, current_keyword)
                success = True

            # Catch broad exceptions and check if it's the quota error
            except Exception as e:
                retries += 1
                # Check based on type name string as direct import might be tricky
                if 'TrendsQuotaExceededError' in str(type(e)):
                     is_quota_error = True
                     logging.error(f"Quota Exceeded fetching data for '{current_keyword}' (Attempt {retries}/{MAX_RETRIES+1}): {e}")
                else:
                     logging.error(f"General Error fetching data for '{current_keyword}' (Attempt {retries}/{MAX_RETRIES+1}): {e}", exc_info=False)

                if retries > MAX_RETRIES:
                    logging.error(f"Max retries failed for '{current_keyword}'. Skipping.")
                    break # Stop retrying this keyword
                else:
                    # Use longer sleep for quota errors
                    current_retry_sleep = RETRY_SLEEP * QUOTA_RETRY_SLEEP_MULTIPLIER if is_quota_error else RETRY_SLEEP
                    logging.warning(f"Retrying {'quota issue' if is_quota_error else 'general error'} in {current_retry_sleep:.0f}s...")
                    time.sleep(current_retry_sleep)

        # --- Mark as processed regardless of success ---
        processed_keywords.add(current_keyword)
        if success:
             processed_count += 1

        # --- Process Results ---
        if success:
            if related_items:
                logging.info(f"Found {len(related_items)} related items for '{current_keyword}'.")
                for item in related_items:
                    db_data = {
                        'level': current_level,
                        'root_keyword': root_kw_origin,
                        'parent_keyword': current_keyword,
                        'discovered_keyword': item['keyword'],
                        'type': item['type'],
                        'rising_value': item['value'], # 'N/A'
                        'search_date': today_str
                    }
                    insert_trend_data(conn, db_data)
                    next_level = current_level + 1
                    if next_level <= max_levels:
                        discovered_kw = item['keyword']
                        if discovered_kw not in processed_keywords and not any(q[0] == discovered_kw for q in keywords_to_process):
                             keywords_to_process.append((discovered_kw, next_level, root_kw_origin))
                             logging.debug(f"Added to queue: '{discovered_kw}' (L{next_level}, Root: {root_kw_origin})")
                        else:
                             logging.debug(f"'{discovered_kw}' already processed or in queue, not adding again.")
            else:
                logging.info(f"No related items found for '{current_keyword}'.")
        else:
             logging.warning(f"Data fetch failed for '{current_keyword}' after retries.")

        # --- Sleep After Processing Each Keyword ---
        sleep_duration = random.uniform(SLEEP_DELAY_AFTER_KEYWORD_MIN, SLEEP_DELAY_AFTER_KEYWORD_MAX)
        logging.debug(f"Sleeping for {sleep_duration:.2f}s after processing '{current_keyword}'...")
        time.sleep(sleep_duration)

    logging.info(f"\nScraping complete. Attempted processing for {item_num} items. Successfully completed fetch sequence for {processed_count} keywords.")


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
        # Call the revised function which adds headers
        test_results = get_related_terms_single(trends_test_instance, TEST_KEYWORD)

        if isinstance(test_results, list):
            logging.info(f"Pre-check test function executed successfully (returned list).")
            if test_results:
                logging.info(f"Pre-check test found {len(test_results)} related items for '{TEST_KEYWORD}'.")
            else:
                 logging.warning(f"Pre-check test found no related items for '{TEST_KEYWORD}' (this might be normal).")
            test_passed = True
        else:
             logging.error(f"CRITICAL: Pre-check test function did not return a list.")
             test_passed = False

    # Catch broad exceptions and check for quota error signature
    except Exception as e:
         if 'TrendsQuotaExceededError' in str(type(e)):
              logging.error(f"CRITICAL: Pre-check test failed due to Quota Error: {e}. The runner IP is likely blocked.", exc_info=False)
         else:
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
