import sqlite3
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError # Import specific exception
import time
import datetime
import os
import logging
import random
import sys

# --- Configuration ---
CSV_ROOT_KEYWORDS_PATH = 'L1.csv' # Path to your CSV file
MAX_LEVELS = 3  # How deep to iterate (Level 1 = root, Level 2 = rising from root, etc.)
BATCH_SIZE = 5  # Process up to 5 keywords per request
TIMEZONE = 360  # US Central Timezone offset in minutes from UTC (adjust if needed)
LANGUAGE = 'en-US'
GEOLOCATION = 'US' # Target country
TIMEFRAME = 'today 3-m' # Trends timeframe (last 3 months)
SLEEP_DELAY_MIN = 15 # Min seconds between batches
SLEEP_DELAY_MAX = 30 # Max seconds between batches
RETRY_SLEEP = 60    # Seconds to wait before retrying a failed batch
MAX_RETRIES = 2     # Max number of retries for a failed batch
DATA_DIR = "data"
DB_FILENAME_FORMAT = "{date}.db"

# --- Logging Setup ---
# Change level to logging.DEBUG to see detailed step logs
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
    except sqlite3.Error as e:
        logging.error(f"Database error inserting {data['discovered_keyword']}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error inserting {data['discovered_keyword']}: {e}")


# --- Google Trends Functions ---
# MODIFIED function with more granular logging and enhanced error handling
def get_rising_trends_batch(pytrends_instance, keyword_list, timeframe, geo, lang, tz):
    """Fetches rising related queries and topics for a BATCH of keywords.
       Includes more granular logging and enhanced error handling for internal pytrends issues.
    """
    batch_results = {kw: [] for kw in keyword_list} # Initialize results dict

    if not keyword_list:
        return batch_results

    logging.info(f"Querying trends for BATCH: {keyword_list}")

    try:
        # --- Step 1: Build Payload ---
        logging.debug(f"Attempting build_payload for: {keyword_list}")
        pytrends_instance.build_payload(keyword_list, cat=0, timeframe=timeframe, geo=geo, gprop='')
        logging.debug(f"build_payload successful for: {keyword_list}")

        # --- Step 2: Get Related Queries ---
        related_queries_data = None # Initialize
        try:
            logging.debug(f"Attempting related_queries for: {keyword_list}")
            related_queries_data = pytrends_instance.related_queries()
            logging.debug(f"related_queries call successful for: {keyword_list}")
        except IndexError as e:
            # Log specific internal error during related_queries fetch
            logging.error(f"INNER IndexError getting related QUERIES for batch {keyword_list}: {e}", exc_info=True) # Log traceback
            # Continue, related_queries_data remains None
        except Exception as e:
            logging.error(f"INNER Unexpected Error getting related QUERIES for batch {keyword_list}: {e}", exc_info=True) # Log traceback
            # Continue, data remains None

        if related_queries_data:
             for keyword in keyword_list:
                 try: # Add inner try-except for processing each keyword's data
                     if keyword in related_queries_data and 'rising' in related_queries_data[keyword] and isinstance(related_queries_data[keyword]['rising'], pd.DataFrame):
                         rising_df_queries = related_queries_data[keyword]['rising']
                         for index, row in rising_df_queries.iterrows():
                             batch_results[keyword].append({
                                 'keyword': row['query'],
                                 'type': 'Query',
                                 'value': str(row['value'])
                             })
                         logging.debug(f"Processed {len(rising_df_queries)} rising queries for '{keyword}' in batch.")
                     # Optional: Add else for logging if key 'rising' not found etc.
                     # else:
                     #    logging.debug(f"No 'rising' queries data structure found for '{keyword}' in batch response.")
                 except KeyError as ke:
                      logging.warning(f"KeyError processing related QUERIES results for '{keyword}' in batch {keyword_list}: {ke}. Data structure might be incomplete.")
                 except Exception as inner_e:
                      logging.error(f"Unexpected error processing related QUERIES results for '{keyword}' in batch {keyword_list}: {inner_e}", exc_info=True)
        else:
            logging.debug(f"No related_queries_data structure to process for batch: {keyword_list}")


        # --- Step 3: Get Related Topics ---
        time.sleep(random.uniform(1, 3)) # Keep delay
        related_topics_data = None # Initialize
        try:
            logging.debug(f"Attempting related_topics for: {keyword_list}")
            related_topics_data = pytrends_instance.related_topics()
            logging.debug(f"related_topics call successful for: {keyword_list}")
        except IndexError as e:
            # Log specific internal error during related_topics fetch
            logging.error(f"INNER IndexError getting related TOPICS for batch {keyword_list}: {e}", exc_info=True) # Log traceback
            # Continue, related_topics_data remains None
        except Exception as e:
            logging.error(f"INNER Unexpected Error getting related TOPICS for batch {keyword_list}: {e}", exc_info=True) # Log traceback
            # Continue, data remains None

        if related_topics_data:
            for keyword in keyword_list:
                 try: # Add inner try-except for processing each keyword's data
                    if keyword in related_topics_data and 'rising' in related_topics_data[keyword] and isinstance(related_topics_data[keyword]['rising'], pd.DataFrame):
                         rising_df_topics = related_topics_data[keyword]['rising']
                         for index, row in rising_df_topics.iterrows():
                             batch_results[keyword].append({
                                'keyword': row['topic_title'],
                                'type': 'Topic',
                                'value': str(row['value'])
                             })
                         logging.debug(f"Processed {len(rising_df_topics)} rising topics for '{keyword}' in batch.")
                    # Optional: Add else for logging if key 'rising' not found etc.
                    # else:
                    #     logging.debug(f"No 'rising' topics data structure found for '{keyword}' in batch response.")
                 except KeyError as ke:
                      logging.warning(f"KeyError processing related TOPICS results for '{keyword}' in batch {keyword_list}: {ke}. Data structure might be incomplete.")
                 except Exception as inner_e:
                      logging.error(f"Unexpected error processing related TOPICS results for '{keyword}' in batch {keyword_list}: {inner_e}", exc_info=True)
        else:
             logging.debug(f"No related_topics_data structure to process for batch: {keyword_list}")

    # Keep the existing broader error handling
    except ResponseError as e:
        logging.error(f"OUTER Google Trends API Response Error for batch {keyword_list}: {e}. Status code: {e.response.status_code}", exc_info=True)
        # Re-raise the specific error to be caught by the retry logic in the main loop
        raise e
    except IndexError as e: # Explicitly catch IndexError at the outer level too
         logging.error(f"OUTER IndexError during fetch for batch {keyword_list}: {e}", exc_info=True)
         # Let it return empty results as the inner handlers should have caught specific cases
         pass
    except Exception as e:
        logging.error(f"OUTER Unexpected Error during fetch for batch {keyword_list}: {e}", exc_info=True)
        # Let it return empty results
        pass

    logging.debug(f"Finished get_rising_trends_batch for: {keyword_list}")
    return batch_results


# --- Main Scraping Logic ---
# MODIFIED function to handle batching and retries
def scrape_trends_iteratively(root_keywords_list, max_levels, db_conn):
    """Performs the iterative scraping process with batching and retries."""
    pytrends = TrendReq(hl=LANGUAGE, tz=TIMEZONE)
    today_str = datetime.date.today().isoformat()

    processed_keywords = set()
    # Queue stores tuples: (keyword_to_process, current_level, root_keyword_origin)
    keywords_to_process = [(kw, 1, kw) for kw in root_keywords_list]
    processed_count = 0
    batch_num = 0

    while keywords_to_process:
        batch_num += 1
        # --- Prepare Batch ---
        # Take up to BATCH_SIZE items from the front of the queue
        num_to_take = min(len(keywords_to_process), BATCH_SIZE)
        current_batch_tuples = keywords_to_process[:num_to_take]
        current_batch_keywords = [t[0] for t in current_batch_tuples]
        # Remove the processed batch from the *start* of the queue
        del keywords_to_process[:num_to_take]

        logging.info(f"\n--- Starting Batch {batch_num} (Size: {len(current_batch_keywords)}) ---")
        logging.info(f"Keywords: {current_batch_keywords}")
        logging.info(f"Queue size remaining: {len(keywords_to_process)}")

        batch_results = {}
        retries = 0
        success = False

        # --- Attempt to Fetch Batch Data with Retries ---
        while retries <= MAX_RETRIES and not success:
            try:
                batch_results = get_rising_trends_batch(pytrends, current_batch_keywords, TIMEFRAME, GEOLOCATION, LANGUAGE, TIMEZONE)
                success = True # If no exception from get_rising_trends_batch was raised to here, consider it a success
            except ResponseError as e:
                # Specifically handle potential rate limits (often status code 429)
                if e.response.status_code == 429 or retries >= MAX_RETRIES:
                    logging.error(f"Rate limit likely hit or max retries ({MAX_RETRIES}) reached for batch. Skipping batch: {current_batch_keywords}. Error: {e}")
                    batch_results = {kw: [] for kw in current_batch_keywords} # Ensure it's an empty dict to avoid processing errors later
                    break # Stop retrying this batch
                else:
                    retries += 1
                    logging.warning(f"Attempt {retries}/{MAX_RETRIES} failed for batch {current_batch_keywords} due to ResponseError. Retrying in {RETRY_SLEEP}s...")
                    time.sleep(RETRY_SLEEP)
            except Exception as e: # Catch other unexpected errors raised from get_rising_trends_batch
                logging.error(f"Non-API error raised during fetch for batch {current_batch_keywords}: {e}. Skipping batch.")
                batch_results = {kw: [] for kw in current_batch_keywords} # Ensure empty dict
                break # Stop retrying this batch

        # --- Process Batch Results ---
        if not batch_results:
             logging.warning(f"Batch {batch_num} resulted in no data or was skipped due to errors.")
             # Add sleep even if skipped to avoid hammering API if there's a persistent issue
             if keywords_to_process:
                 sleep_duration = random.uniform(SLEEP_DELAY_MIN, SLEEP_DELAY_MAX)
                 logging.info(f"--- Sleeping for {sleep_duration:.2f}s after failed/empty batch ---")
                 time.sleep(sleep_duration)
             continue # Skip processing if the batch failed entirely or returned nothing


        for keyword_tuple in current_batch_tuples:
            current_keyword, current_level, root_kw_origin = keyword_tuple

            # Check if processed already (less likely needed with set but safe)
            if current_keyword in processed_keywords:
                logging.debug(f"Skipping already processed: '{current_keyword}' within batch {batch_num}")
                continue

            # Check level constraint
            if current_level > max_levels:
                logging.debug(f"Max level ({max_levels}) reached for branch starting from '{current_keyword}' (Root: {root_kw_origin}). Will not queue children.")
                # Still mark as processed, but don't add children
                processed_keywords.add(current_keyword)
                processed_count += 1
                continue # Don't process children

            processed_keywords.add(current_keyword)
            processed_count += 1
            logging.info(f"Processing Level {current_level}: '{current_keyword}' (Root: {root_kw_origin}) - Item {processed_count} (from Batch {batch_num})")

            # Get results for this specific keyword from the batch results dictionary
            rising_items = batch_results.get(current_keyword, []) # Use .get for safety

            if not rising_items:
                logging.debug(f"No rising items found for '{current_keyword}' in this batch's results.")
                # Continue to the next keyword in the batch

            for item in rising_items:
                # --- Insert into DB ---
                db_data = {
                    'level': current_level, # Store the level of the *parent*
                    'root_keyword': root_kw_origin,
                    'parent_keyword': current_keyword,
                    'discovered_keyword': item['keyword'],
                    'type': item['type'],
                    'rising_value': item['value'],
                    'search_date': today_str
                }
                insert_trend_data(db_conn, db_data)

                # --- Add to Queue for Next Level ---
                next_level = current_level + 1
                if next_level <= max_levels:
                    discovered_kw = item['keyword']
                    # Check if the discovered item should be queued
                    # Avoid adding duplicates to the processing queue
                    # Check both processed set and the current queue list
                    if discovered_kw not in processed_keywords and not any(q[0] == discovered_kw for q in keywords_to_process):
                         # Append to the end of the main queue
                         keywords_to_process.append((discovered_kw, next_level, root_kw_origin))
                         logging.debug(f"Added to queue: '{discovered_kw}' (L{next_level}, Root: {root_kw_origin})")
                    else:
                         logging.debug(f"'{discovered_kw}' already processed or in queue, not adding again.")
                # else: # Optional: Log if max level reached for children
                #     logging.debug(f"Max level reached, not queueing children of '{current_keyword}'")


        # --- Sleep Between Batches ---
        if keywords_to_process: # Only sleep if there's more work to do
            sleep_duration = random.uniform(SLEEP_DELAY_MIN, SLEEP_DELAY_MAX)
            logging.info(f"--- Batch {batch_num} Complete. Sleeping for {sleep_duration:.2f}s before next batch ---")
            time.sleep(sleep_duration)
        else:
            logging.info(f"--- Batch {batch_num} Complete. No more items in queue. ---")


    logging.info(f"\nScraping complete. Processed {processed_count} unique keywords/topics across {batch_num} batches.")


# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()
    logging.info("Starting Google Trends iterative scraping process (Batch Mode)...")

    # --- Load Root Keywords from CSV ---
    root_keywords_from_csv = []
    try:
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
        scrape_trends_iteratively(root_keywords_from_csv, MAX_LEVELS, conn)
    except Exception as e:
        # Log any uncaught exception from the main process
        logging.exception("An unexpected error occurred during the main scraping execution.")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    end_time = time.time()
    logging.info(f"Script finished in {end_time - start_time:.2f} seconds.")
    logging.info(f"Data saved to: {db_path}")
