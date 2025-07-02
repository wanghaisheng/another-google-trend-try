import json
import urllib.parse
from datetime import datetime, timedelta
from curl_cffi import requests
import time
import os


def build_payload(keywords, timeframe='now 1-H', geo=''):
    token_payload = {
        'hl': 'en-US',
        'tz': '0',
        'req': {
            'comparisonItem': [{'keyword': keyword, 'time': timeframe, 'geo': geo} for keyword in keywords],
            'category': 0,
            'property': ''
        }
    }
    token_payload['req'] = json.dumps(token_payload['req'])
    return token_payload

def convert_to_desired_format(raw_data):
    trend_data = {'TOP': {}, 'RISING': {}}

    if 'rankedList' in raw_data.get('default', {}):
        for item in raw_data['default']['rankedList']:
            for entry in item.get('rankedKeyword', []):
                query = entry.get('query')
                value = entry.get('value')
                if query and value:
                    link = entry.get('link', '')
                    trend_type = link.split('=')[-1].split('&')[0].upper() if link else None

                    if trend_type in ['TOP', 'RISING']:
                        trend_data[trend_type][query] = value
    return trend_data

def get_google_cookies(impersonate_version='chrome110'):
    with requests.Session() as session:
        session.get("https://www.google.com", impersonate=impersonate_version)
        return session.cookies

def fetch_trends_data(keywords, days_ago=7, geo='US', hl='en-US', max_retries=5, browser_version='chrome110', browser_switch_retries=2):
    browser_versions = ['chrome110', 'edge101', 'chrome107', 'chrome104', 'chrome100', 'chrome101', 'chrome99']
    current_browser_version_index = browser_versions.index(browser_version)
    cookies = get_google_cookies(impersonate_version=browser_versions[current_browser_version_index])

    for browser_retry in range(browser_switch_retries + 1):
        data_fetched = False
        with requests.Session() as s:
            # phase 1: token
            for retry in range(max_retries):
                time.sleep(2)
                token_payload = build_payload(keywords)
                url = 'https://trends.google.com/trends/api/explore'
                params = urllib.parse.urlencode(token_payload)
                full_url = f"{url}?{params}"
                response = s.get(full_url, impersonate=browser_versions[current_browser_version_index], cookies=cookies)
                if response.status_code == 200:
                    content = response.text[4:]
                    try:
                        data = json.loads(content)
                        widgets = data['widgets']
                        tokens = {}
                        request = {}
                        for widget in widgets:
                            if widget['id'] == 'RELATED_QUERIES':
                                tokens['related_queries'] = widget['token']
                                request['related_queries'] = widget['request']
                        break
                    except json.JSONDecodeError:
                        print(f"Failed to decode JSON while fetching token, retrying {retry + 1}/{max_retries}")
                else:
                    print(f"Error {response.status_code} while fetching token, retrying {retry + 1}/{max_retries}")
            else:
                print(f"Exceeded maximum retry attempts ({max_retries}) while fetching token. Exiting...")
                return None

            # phase 2: trends data
            for retry in range(max_retries):
                time.sleep(5)
                req_string = json.dumps(request['related_queries'], separators=(',', ':'))
                encoded_req = urllib.parse.quote(req_string, safe=':,+')
                url = f"https://trends.google.com/trends/api/widgetdata/relatedsearches?hl={hl}&tz=0&req={encoded_req}&token={tokens['related_queries']}&tz=0"
                response = s.get(url, impersonate=browser_versions[current_browser_version_index], cookies=cookies)
                print(f"URL: {url}")
                if response.status_code == 200:
                    content = response.text[5:]
                    try:
                        file_name = f"trends_data_{os.getpid()}.json"
                        with open(file_name, 'w') as json_file:
                            json_file.write(content)
                        
                        # Remove first line from the file
                        with open(file_name, 'r') as f:
                            lines = f.readlines()[1:]
                        with open(file_name, 'w') as f:
                            f.writelines(lines)
                        
                        # Load JSON content from the file
                        with open(file_name, 'r') as json_file:
                            data = json.load(json_file)
                        
                        # Extract and print queries and values from both rankedLists separately
                        for item in data['default']['rankedList'][0]['rankedKeyword']:
                            print(f"Top: {item['query']}, Value: {item['value']}")
                        
                        for item in data['default']['rankedList'][1]['rankedKeyword']:
                            print(f"Rising: {item['query']}, Value: {item['value']}")
                        
                        return content
                    except json.JSONDecodeError:
                        print(f"Failed to decode JSON while fetching trends data, retrying {retry + 1}/{max_retries}")
                else:
                    print(f"Error {response.status_code} while fetching trends data, retrying {retry + 1}/{max_retries}")
            else:
                print(f"Exceeded maximum retry attempts ({max_retries}) while fetching trends data.")

        if not data_fetched and browser_retry < browser_switch_retries:
            time.sleep(5)
            current_browser_version_index = (current_browser_version_index + 1) % len(browser_versions)
            print(f"Switching browser version to {browser_versions[current_browser_version_index]} and retrying...")

    print(f"Exceeded maximum browser switch attempts ({browser_switch_retries}). Exiting...")
    return None

# Example
keywords = ["test"]
trends_data = fetch_trends_data(keywords)
print(trends_data)
