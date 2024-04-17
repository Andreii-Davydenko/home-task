import requests
import json
from bs4 import BeautifulSoup
import logging
from ratelimit import limits, sleep_and_retry, RateLimitException
import time


# Set up logging
logging.basicConfig(level=logging.INFO)

# Define the rate limit parameters
CALLS = 10
PERIOD = 60
RETRY_TIMES = 3  # Number of retries
RETRY_WAIT = 5    # Wait time between retries in seconds

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_html_content(api_key, url, delay=1.0):
    """
    Fetches the HTML content from the specified URL using a provided API key.
    This function is rate-limited and implements a retry mechanism to handle transient network errors.
    
    Parameters:
    api_key (str): API key for the proxy service.
    url (str): URL of the target website.
    delay (float, optional): Delay in seconds between retries. Default is 1.0 seconds.

    Returns:
    bytes: The fetched HTML content, or None if fetching fails.
    """
    
    for attempt in range(RETRY_TIMES):
        try:
            response = requests.get(
                url='https://proxy.scrapeops.io/v1/',
                params={
                    'api_key': api_key,
                    'url': url,
                    'render_js': 'true',
                },
                timeout=10  # Timeout in seconds to avoid hanging requests
            )
            response.raise_for_status()  # Raises HTTPError for HTTP errors
            return response.content

        except requests.HTTPError as e:
            # Handle specific HTTP errors
            logging.error(f"HTTP error encountered: {e}")
            return None

        except requests.RequestException as e:
            # General request exception handling
            if attempt < RETRY_TIMES - 1:
                logging.warning(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds. Error: {e}")
                time.sleep(delay)
            else:
                logging.error(f"Failed to fetch URL content after {RETRY_TIMES} attempts: {e}")
                return None

        except RateLimitException as e:
            # Specific handling for rate limit exceptions
            logging.error(f"Rate limit exceeded. Error: {e}")
            return None

        except Exception as e:
            # Catch-all for any other unexpected exceptions
            logging.error(f"Unexpected error: {e}")
            return None


def parse_html_to_json(html_content):
    """
    Parses HTML content to extract and return JSON data.

    This function takes HTML content, typically a response from a web request, and attempts to parse it
    to extract JSON data.

    Parameters:
    html_content (bytes): HTML content to be parsed.

    Returns:
    dict: Parsed JSON data if successful; None if the parsing fails.
    """
    try:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Attempt to extract and parse the JSON data from the HTML body
        json_data = json.loads(soup.body.text)
        return json_data

    except json.JSONDecodeError as jde:
        # Log specific error for JSON parsing issues
        logging.error("JSON parsing error: The response is not in JSON format.")
        raise jde  # Re-raise the exception for upstream handling

    except Exception as e:
        # Log any other unexpected errors during parsing
        logging.error(f"Unexpected error while parsing HTML to JSON: {e}")
        raise e  # Re-raise the exception for upstream handling


def extract_user_info(json_data):
    """
    Extracts user information from JSON data and logs the extracted information.

    This function processes a JSON object expected to contain a list of items under the 'data' key.
    Each item should represent channel data, including nested user information.

    The function iterates through each item, extracts relevant information, and logs it for further use.

    Parameters:
    json_data (dict): JSON data containing user and channel information.
    """
    try:
        # Validate that the JSON data contains the expected structure.
        if not json_data or 'data' not in json_data or not isinstance(json_data['data'], list):
            raise ValueError('Invalid or missing "data" field in JSON data.')

        # Iterate over each item in the JSON data.
        for item in json_data['data']:
            channel_info = item.get('channel', {})
            user_info = channel_info.get('user', {})
            
            # Extract relevant user information.
            extracted_user_info = {
                'user_id': user_info.get('id'),
                'username': user_info.get('username'),
                'bio': user_info.get('bio'),
                'social_media': {
                    'instagram': user_info.get('instagram'),
                    'twitter': user_info.get('twitter'),
                    'youtube': user_info.get('youtube'),
                    'discord': user_info.get('discord'),
                    'tiktok': user_info.get('tiktok'),
                    'facebook': user_info.get('facebook')
                },
                'profilepic': user_info.get('profilepic')
            }

            # Log the extracted information.
            logging.info(f"Channel Info: {channel_info}")
            logging.info(f"User Info: {extracted_user_info}")

    except ValueError as ve:
        # Raise the specific error encountered.
        raise ValueError("Data extraction error: {ve}")

    except Exception as e:
        # Raise any other unexpected errors.
        raise ValueError("Unexpected error during data extraction: {e}")


def main():
    # API key for accessing the ScrapeOps Proxy service.
    api_key = '6401ffb8-e4d8-4449-b647-c0d9e59638cf'

    # The target URL from which I should scrape data.
    # This URL points to a specific page on kick.com that lists featured livestreams.
    target_url = 'https://kick.com/stream/featured-livestreams/en'

    try:
        # Call the function `get_html_content` to fetch the HTML content from the target URL.
        # The function uses the provided API key and the target URL to make the request.
        # It is wrapped with rate limiting and retry mechanisms to handle API constraints and transient network issues.
        html_content = get_html_content(api_key, target_url)

        # Check if the HTML content was successfully retrieved.
        # If `html_content` is None, it means there was an error in fetching the content, and the script should not proceed further.
        if not html_content:
            raise ValueError("Failed to retrieve HTML content.")
        
        # Parse the HTML content to extract JSON data.
        # This function converts the HTML response into a JSON structure, making it easier to work with the data programmatically.
        json_data = parse_html_to_json(html_content)

        if not json_data:
            raise ValueError("Failed to parse HTML content to JSON.")

        # Extract user information from the parsed JSON data.
        # This function iterates through the JSON data to find and print user-related information, such as usernames, bios, and social media links.
        extract_user_info(json_data)
    except Exception as e:
        raise ValueError("An error occurred: {e}")            

if __name__ == "__main__":
    main()
