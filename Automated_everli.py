import re
import requests
import socket
import uuid
import os
import shutil
import json
import tempfile
import logging
import glob
import time
import urllib.parse
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any
from DrissionPage import ChromiumPage, ChromiumOptions
import pandas as pd
import random 
import secrets
from pytz import timezone

zone_dubai = timezone('Asia/Dubai')
source_file_ID = 267162
stores=pd.read_csv("stores.csv")

class SimplifiedTokenExtractor:
    """Simplified token extraction using only vAuthToken cookie"""
    
    def __init__(self, page, logger):
        self.page = page
        self.logger = logger
        self.captured_token = None
        
    def extract_vauth_token_from_cookies(self) -> Optional[str]:
        """Extract vAuthToken from browser cookies"""
        try:
            self.logger.info("Attempting to extract vAuthToken from cookies")
            
            time.sleep(3)
            
            cookies = self.page.cookies()
            
            for cookie in cookies:
                cookie_name = cookie.get('name', '')
                cookie_value = cookie.get('value', '')
                
                if cookie_name == 'vAuthToken' and cookie_value:
                    if len(cookie_value) > 10 and cookie_value != 'null':
                        self.logger.info(f"vAuthToken found in cookies: {cookie_value[:20]}...")
                        self.captured_token = cookie_value
                        return cookie_value
                        
            self.logger.warning("vAuthToken cookie not found")
            return None
                        
        except Exception as e:
            self.logger.warning(f"Error extracting vAuthToken from cookies: {e}")
            return None
    
    def extract_token_with_retries(self, max_attempts: int = 5) -> Optional[str]:
        """Extract vAuthToken with multiple attempts"""
        self.logger.info("Starting vAuthToken extraction with retries")
        
        for attempt in range(max_attempts):
            self.logger.info(f"Token extraction attempt {attempt + 1}/{max_attempts}")
            
            token = self.extract_vauth_token_from_cookies()
            
            if token and token != 'null':
                self.logger.info(f"Successfully extracted vAuthToken on attempt {attempt + 1}")
                return token
            
            if attempt < max_attempts - 1:
                self.logger.info(f"Attempt {attempt + 1} failed, waiting 5 seconds before retry...")
                time.sleep(5)
        
        self.logger.error("Failed to extract vAuthToken after all attempts")
        return None


class HeaderManager:
    """Enhanced header management for API requests"""
    
    def __init__(self, logger):
        self.logger = logger
        self.session_id = None
        self.device_fingerprint = None
        
    def generate_device_fingerprint(self) -> str:
        """Generate a consistent device fingerprint for the session"""
        if not self.device_fingerprint:
            self.device_fingerprint = str(uuid.uuid4())
        return self.device_fingerprint
    
    def generate_session_id(self) -> str:
        """Generate a session ID that persists across requests"""
        if not self.session_id:
            self.session_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, secrets.token_hex(8)))
        return self.session_id
    
    def get_random_user_agent(self) -> str:
        """Get a random but realistic user agent"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
        ]
        return random.choice(user_agents)
    
    def get_random_screen_resolution(self) -> str:
        """Get a random but common screen resolution"""
        resolutions = [
            "1920x1080", "1366x768", "1536x864", "1440x900",
            "1280x720", "1600x900", "1024x768", "1680x1050"
        ]
        return random.choice(resolutions)
    
    def generate_base_headers(self, authentication_token: Optional[str] = None) -> Dict[str, str]:
        """Generate base headers that should be consistent across requests"""
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,it;q=0.7',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': self.get_random_user_agent(),
            'x-s24-client': 'website/8.4.1',
            'x-s24-country': 'ITA',
            'x-s24-device-resolution': self.get_random_screen_resolution(),
            'x-s24-tracking': 'false',
            'x-s24-whitelabel': 'it.everli.com',
            'user-session': self.generate_session_id(),
            'x-device-id': self.generate_device_fingerprint(),
            'origin': 'https://it.everli.com',
            'referer': 'https://it.everli.com/',
            'dnt': '1',
            'connection': 'keep-alive',
        }
        
        if authentication_token:
            token = str(authentication_token).strip().strip('"\'')
            if token and token != 'null':
                if not token.startswith('Bearer '):
                    token = f'Bearer {token}'
                headers['authorization'] = token
            
        return headers
    
    def generate_request_specific_headers(self, 
                                       request_type: str = 'GET',
                                       referer: Optional[str] = None,
                                       origin: Optional[str] = None) -> Dict[str, str]:
        """Generate headers specific to the request type"""
        specific_headers = {}
        
        if request_type.upper() == 'POST':
            specific_headers.update({
                'content-type': 'application/json',
                'priority': 'u=1, i'
            })
        elif request_type.upper() == 'GET':
            specific_headers.update({
                'priority': 'u=1, i'
            })
            
        if referer:
            specific_headers['referer'] = referer
            
        if origin:
            specific_headers['origin'] = origin
            
        specific_headers['x-request-id'] = str(uuid.uuid4())
        specific_headers['x-timestamp'] = str(int(time.time()))
        
        return specific_headers
    
    def get_headers_for_api_call(self, 
                                authentication_token: str,
                                request_type: str = 'GET',
                                endpoint_url: str = '',
                                referer: Optional[str] = None) -> Dict[str, str]:
        """Get complete headers for an API call"""
        base_headers = self.generate_base_headers(authentication_token)
        
        if not referer and 'categories' in endpoint_url:
            referer = 'https://it.everli.com/'
        elif not referer:
            referer = 'https://it.everli.com/'
            
        specific_headers = self.generate_request_specific_headers(
            request_type=request_type,
            referer=referer,
            origin='https://it.everli.com'
        )
        
        final_headers = {**base_headers, **specific_headers}
        
        self.logger.debug(f"Generated headers for {request_type} {endpoint_url}")
        self.logger.debug(f"Auth header present: {'authorization' in final_headers}")
        
        return final_headers
    
    def validate_token(self, token: str) -> bool:
        """Validate that the token looks correct"""
        if not token or not isinstance(token, str) or token == 'null':
            return False
            
        clean_token = token.strip().strip('"\'')
        if clean_token.startswith('Bearer '):
            clean_token = clean_token.replace('Bearer ', '')
            
        if len(clean_token) < 10:
            return False
            
        if ' ' in clean_token or '\n' in clean_token or '\r' in clean_token:
            return False
            
        return True


class EverliRegistrationBot: 
    MAIL_TM_API = "https://api.mail.tm"
    LOG_DIR = "logs"
    
    def __init__(self):
        self.machine_id = socket.gethostname()
        self.job_id = str(uuid.uuid4())
        self.logger = self._setup_logging()
        self.header_manager = HeaderManager(self.logger)
        self.authentication_token = None  
        self._cleanup_old_logs()
    
    def _setup_logging(self) -> logging.Logger:
        try:
            os.makedirs(self.LOG_DIR, exist_ok=True)
            
            logger = logging.getLogger('everli_bot')
            logger.setLevel(logging.INFO)
            logger.handlers.clear()
            
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
            )
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            
            today = datetime.now().strftime('%Y-%m-%d')
            log_filename = os.path.join(self.LOG_DIR, f'everli_bot_{today}.log')
            file_handler = logging.FileHandler(log_filename, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(file_formatter)
            
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(console_formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            
            logger.info(f"Logging initialized. Job ID: {self.job_id}")
            return logger
            
        except Exception as e:
            print(f"Failed to setup logging: {e}")
            raise
    
    def _cleanup_old_logs(self, retention_days: int = 7) -> None:
        """Remove log files older than the specified retention period."""
        try:
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            log_pattern = os.path.join(self.LOG_DIR, 'everli_bot_*.log')
            
            for log_file in glob.glob(log_pattern):
                try:
                    file_date_str = os.path.basename(log_file).replace('everli_bot_', '').replace('.log', '')
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    
                    if file_date < cutoff_date:
                        os.remove(log_file)
                        self.logger.info(f"Removed old log file: {log_file}")
                        
                except (ValueError, OSError) as e:
                    self.logger.warning(f"Could not process log file {log_file}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error during log cleanup: {e}")
    
    @staticmethod
    def human_delay(min_seconds: float = 0.5, max_seconds: float = 1.5) -> None:
        """Simulate human-like delays in automation."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def create_temporary_email(self) -> Tuple[str, str, str]:
        """Create a temporary email account using Mail.tm API."""
        max_retries = 3
        base_delay = 3
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Creating temporary email account (attempt {attempt + 1})")
                
                response = requests.get(f"{self.MAIL_TM_API}/domains", timeout=10)
                response.raise_for_status()
                domain = response.json()["hydra:member"][0]["domain"]
                
                local_part = "".join(
                    random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10)
                )
                email = f"{local_part}@{domain}"
                password = "PASworrd@123!"
                
                create_response = requests.post(
                    f"{self.MAIL_TM_API}/accounts",
                    json={"address": email, "password": password},
                    timeout=10
                )
                create_response.raise_for_status()
                
                token_response = requests.post(
                    f"{self.MAIL_TM_API}/token",
                    json={"address": email, "password": password},
                    timeout=10
                )
                token_response.raise_for_status()
                token = token_response.json()["token"]
                
                self.logger.info(f"Successfully created temporary email: {email}")
                return email, password, token
                
            except Exception as e:
                self.logger.warning(f"Mail.tm creation attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  
                    time.sleep(delay)
                else:
                    self.logger.error("Failed to create temporary email after all retries")
                    raise Exception("Could not create temporary email account")
    
    def poll_for_confirmation_email(self, token: str, timeout: int = 300) -> str:
        """Poll Mail.tm for confirmation email with verification link."""
        headers = {"Authorization": f"Bearer {token}"}
        deadline = time.time() + timeout
        poll_interval = 10
        
        self.logger.info(f"Polling for confirmation email (timeout: {timeout}s)")
        
        while time.time() < deadline:
            try:
                response = requests.get(f"{self.MAIL_TM_API}/messages", headers=headers, timeout=10)
                response.raise_for_status()
                messages = response.json().get("hydra:member", [])
                
                if messages:
                    message_id = messages[0]["id"]
                    msg_response = requests.get(
                        f"{self.MAIL_TM_API}/messages/{message_id}",
                        headers=headers,
                        timeout=10
                    )
                    msg_response.raise_for_status()
                    message = msg_response.json()
                    
                    content = message.get("html") or message.get("text") or ""
                    if isinstance(content, list):
                        content = "".join(content)
                    
                    match = re.search(r'(https://it\.everli\.com[^\s"<]+)', content)
                    if match and match.group(1) != "https://it.everli.com/":
                        link = match.group(1)
                        self.logger.info(f"Found confirmation link: {link}")
                        return link
                    
                    fallback_match = re.search(r'https://[^\s"<]+', content)
                    if fallback_match and fallback_match.group(0) != "https://it.everli.com/":
                        link = fallback_match.group(0)
                        self.logger.info(f"Using fallback confirmation link: {link}")
                        return link
                    
                    self.logger.debug("Email received but no usable confirmation link found")
                
            except Exception as e:
                self.logger.warning(f"Error while polling for email: {e}")
            
            time.sleep(poll_interval)
        
        raise RuntimeError(f"No confirmation email received within {timeout} seconds")
    
    def type_text_humanlike(self, element, text: str, delay: float = 0.1) -> None:
        """Type text into an element with human-like delays between characters."""
        try:
            element.clear()
            for char in text:
                element.input(char)
                time.sleep(delay)
            self.logger.debug(f"Successfully typed text into element")
        except Exception as e:
            self.logger.error(f"Failed to type text: {e}")
            raise
    
    def wait_for_password_input(self, page, max_attempts: int = 10) -> object:
        """Wait for password input field to appear on the page."""
        for attempt in range(max_attempts):
            try:
                inputs = page.eles("tag:input")
                for input_elem in inputs:
                    if input_elem.attrs.get("type") == "password":
                        self.logger.info("Password input field found")
                        return input_elem
                
                self.logger.debug(f"Password input not found, attempt {attempt + 1}")
                time.sleep(2)
                
            except Exception as e:
                self.logger.warning(f"Error while searching for password input: {e}")
                time.sleep(2)
        
        raise Exception("Password input field not found after maximum attempts")
    
    def click_continue_with_email(self, page, max_attempts: int = 10) -> None:
        for attempt in range(max_attempts):
            try:
                self.logger.debug(f"Looking for 'Continue with Email' button (attempt {attempt + 1})")
                buttons = page.eles("tag:button")
                
                for button in buttons:
                    button_text = button.text.strip().lower()
                    if "continue with email" in button_text:
                        page.run_js(
                            'arguments[0].scrollIntoView({behavior: "smooth", block: "center"});',
                            button,
                        )
                        time.sleep(1)
                        
                        button.click()
                        self.logger.info("Successfully clicked 'Continue with Email' button")
                        return
                
                time.sleep(2)
                
            except Exception as e:
                self.logger.warning(f"Error clicking continue button: {e}")
                time.sleep(2)
        
        raise Exception("'Continue with Email' button not found or not clickable")
    
    def setup_browser(self) -> Tuple[ChromiumPage, str]:
        try:
            temp_profile = tempfile.mkdtemp(prefix="everli_profile_")
            
            options = ChromiumOptions()
            options.headless(False)
            options.set_argument("--start-maximized")
            options.set_argument("--disable-blink-features=AutomationControlled")
            options.set_argument("--disable-extensions")
            options.set_user_data_path(temp_profile)
            
            page = ChromiumPage(options)
            self.logger.info(f"Browser initialized with temp profile: {temp_profile}")
            
            return page, temp_profile
            
        except Exception as e:
            self.logger.error(f"Failed to setup browser: {e}")
            raise
    
    def debug_api_response(self, url: str, response: requests.Response) -> None:
        """Debug API response to understand what's happening."""
        self.logger.info(f"=== API DEBUG INFO ===")
        self.logger.info(f"URL: {url}")
        self.logger.info(f"Status Code: {response.status_code}")
        self.logger.info(f"Response Headers: {dict(response.headers)}")
        
        try:
            response_json = response.json()
            self.logger.info(f"Response JSON keys: {list(response_json.keys()) if isinstance(response_json, dict) else 'Not a dict'}")
            if isinstance(response_json, dict) and 'error' in response_json:
                self.logger.error(f"API Error: {response_json}")
        except:
            self.logger.info(f"Response Text (first 500 chars): {response.text[:500]}")
        
        self.logger.info(f"=== END DEBUG INFO ===")
    
    def test_token_with_simple_call(self) -> bool:
        if not self.authentication_token:
            return False
            
        test_urls = [
            "https://api.everli.com/sm/api/v3/locations",
            "https://api.everli.com/sm/api/v3/user/addresses", 
            "https://api.everli.com/sm/api/v3/user/profile",
            "https://api.everli.com/sm/api/v3/categories",
            "https://api.everli.com/sm/api/v3/stores"
        ]
        
        for test_url in test_urls:
            try:
                headers = self.get_headers_for_request(self.authentication_token, test_url)
                response = requests.get(test_url, headers=headers, timeout=10)
                
                self.logger.info(f"Testing token with {test_url}: Status {response.status_code}")
                
                if response.status_code == 200:
                    self.logger.info("Token is working!")
                    return True
                elif response.status_code == 401:
                    self.logger.warning("Token is unauthorized")
                    return False
                elif response.status_code == 405:
        
                    self.logger.info(f"Endpoint {test_url} doesn't support GET, but token format accepted")
                    continue
                elif response.status_code in [403, 404]:
                    # Forbidden or Not Found - token might be valid, just wrong endpoint
                    self.logger.info(f"Endpoint {test_url} returned {response.status_code}, trying next...")
                    continue
                else:
                    self.debug_api_response(test_url, response)
                    
            except Exception as e:
                self.logger.debug(f"Test failed for {test_url}: {e}")
                continue
        
        self.logger.info("Token appears valid (no 401 errors received)")
        return True
    
    def clean_url(self, url: str) -> str:
        if url.startswith('https://') and '///' not in url:
            return url
        
        if '://' in url:
            protocol, rest = url.split('://', 1)
            rest = re.sub(r'/+', '/', rest)
            return f"{protocol}://{rest}"
        
        url = re.sub(r'/+', '/', url)
        return url
    
    def make_api_request_with_retry(self, url: str, headers: Dict[str, str], 
                                  params: Optional[Dict] = None, max_retries: int = 3) -> requests.Response:
        if '///' in url or (url.count('//') > 1 and not url.startswith('http')):
            self.logger.warning(f"Detected malformed URL, attempting to clean: {url}")
            url = self.clean_url(url)
            self.logger.info(f"Cleaned URL: {url}")
        
        for attempt in range(max_retries):
            try:
                # Generate fresh headers for each attempt
                if self.authentication_token:
                    headers = self.get_headers_for_request(self.authentication_token, url)
                
                self.logger.debug(f"Making request to: {url}")
                self.logger.debug(f"Auth header: {headers.get('authorization', 'MISSING')[:50]}...")
                
                response = requests.get(url, headers=headers, params=params, timeout=30)
                
                # Debug the response
                if response.status_code != 200:
                    self.debug_api_response(url, response)
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"API request attempt {attempt + 1} failed: {e}")
                
                # If it's a 401 error, try to refresh the token
                if "401" in str(e) and self.authentication_token:
                    self.logger.info("Got 401 error, attempting to refresh session...")
                    if self.refresh_authentication():
                        self.logger.info("Session refreshed, retrying with new token...")
                        headers = self.get_headers_for_request(self.authentication_token, url)
                        continue
                    else:
                        # Try testing the token with a simple call
                        self.logger.info("Testing current token validity...")
                        if not self.test_token_with_simple_call():
                            self.logger.error("Token appears to be invalid. Cannot proceed.")
                            raise
                
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
    
    def refresh_authentication(self) -> bool:
        """Try to refresh the authentication token by making a session call."""
        try:
            # Try to refresh by calling a profile or session endpoint
            refresh_urls = [
                "https://api.everli.com/sm/api/v3/user/profile",
                "https://api.everli.com/sm/api/v3/user/session",
                "https://api.everli.com/sm/api/v3/auth/refresh"
            ]
            
            current_headers = self.get_headers_for_request(self.authentication_token)
            
            for refresh_url in refresh_urls:
                try:
                    refresh_response = requests.get(refresh_url, headers=current_headers, timeout=10)
                    
                    if refresh_response.status_code == 200:
                        # Check if response contains a new token
                        try:
                            response_data = refresh_response.json()
                            token_fields = ['token', 'access_token', 'auth_token', 'authToken']
                            
                            for field in token_fields:
                                if field in response_data and response_data[field]:
                                    new_token = str(response_data[field]).strip()
                                    if new_token and new_token != self.authentication_token:
                                        self.logger.info(f"Found refreshed token in response: {new_token[:20]}...")
                                        self.authentication_token = new_token
                                        return True
                        except json.JSONDecodeError:
                            pass
                        
                        # Check response headers for new token
                        auth_header = refresh_response.headers.get('authorization')
                        if auth_header and 'Bearer' in auth_header:
                            new_token = auth_header.replace('Bearer ', '').strip()
                            if new_token and new_token != self.authentication_token:
                                self.logger.info(f"Found refreshed token in headers: {new_token[:20]}...")
                                self.authentication_token = new_token
                                return True
                                
                        self.logger.info("Session refresh call successful, token may still be valid")
                        return True
                        
                except Exception as e:
                    self.logger.debug(f"Refresh attempt with {refresh_url} failed: {e}")
                    continue
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Failed to refresh authentication: {e}")
            return False
    
    def validate_token_with_api_test(self, token: str) -> bool:
        """Test the token with an actual API endpoint that should work"""
        try:
            if not self.header_manager.validate_token(token):
                return False
                
            headers = self.header_manager.get_headers_for_api_call(token)
            
            # Try different test endpoints that might work
            test_endpoints = [
                "https://api.everli.com/sm/api/v3/locations",
                "https://api.everli.com/sm/api/v3/user/addresses",
                "https://api.everli.com/sm/api/v3/user/profile",
                "https://api.everli.com/sm/api/v3/categories",
                "https://api.everli.com/sm/api/v3/stores"
            ]
            
            unauthorized_count = 0
            
            for endpoint in test_endpoints:
                try:
                    response = requests.get(endpoint, headers=headers, timeout=10)
                    self.logger.debug(f"Test endpoint {endpoint} returned status: {response.status_code}")
                    
                    if response.status_code == 200:
                        self.logger.info(f"Token validation successful with endpoint: {endpoint}")
                        return True
                    elif response.status_code == 401:
                        self.logger.warning(f"Token unauthorized for endpoint: {endpoint}")
                        unauthorized_count += 1
                        continue
                    elif response.status_code == 405:
                        # Method not allowed means endpoint exists and token format is accepted
                        self.logger.info(f"Endpoint {endpoint} doesn't support GET but accepts token format")
                        continue
                    elif response.status_code in [403, 404]:
                        # These might just mean the endpoint doesn't exist or isn't accessible
                        # but the token could still be valid
                        continue
                except Exception as e:
                    self.logger.debug(f"Error testing endpoint {endpoint}: {e}")
                    continue
            
            # If we got 401 from all endpoints, token is probably invalid
            if unauthorized_count == len(test_endpoints):
                self.logger.error("Token appears invalid - got 401 from all test endpoints")
                return False
            
            # If we get here without getting 401 from all endpoints, token is likely valid
            self.logger.info("Token validation completed - proceeding with extracted token")
            return True
                
        except Exception as e:
            self.logger.error(f"Token validation error: {e}")
            return False
    
    def validate_and_test_token(self, token: str) -> bool:
        """Test if the token works by making a simple API call."""
        return self.validate_token_with_api_test(token)
    
    def register_and_confirm(self) -> Optional[str]:
        """
        Complete registration and confirmation process for Everli account.
        SIMPLIFIED: Only extract vAuthToken from cookies.
        """
        page = None
        temp_profile = None
        
        try:
            self.logger.info("Starting Everli account registration process")
            email, password, token = self.create_temporary_email()
            
            page, temp_profile = self.setup_browser()
            
            self.logger.info("Navigating to Everli website")
            page.get("https://it.everli.com/")
            self.human_delay()
            
            try:
                accept_button = page.ele('x://button[contains(text(), "Accept")]', timeout=5)
                if accept_button:
                    accept_button.click()
                    self.logger.info("Cookie consent accepted")
            except Exception:
                self.logger.debug("No cookie consent dialog found")
            
            self.logger.info("Entering address and selecting store")
            try:
                address_input = page.ele('css:input[placeholder*="via Marco Polo"]', timeout=10)
                address_input.click()
                address_input.clear()
                address_input.input("3,via Gaudenzio Ferrari, Milano")
                self.human_delay(1, 2)
                address_input.input("\ue015\ue007") 
                time.sleep(4)
                
                lidl_found = False
                for attempt in range(5):
                    try:
                        lidl_element = page.ele('x://span[contains(text(),"Lidl")]', timeout=2)
                        if lidl_element:
                            page.run_js(
                                'arguments[0].scrollIntoView({behavior: "smooth", block: "center"});',
                                lidl_element,
                            )
                            time.sleep(1)
                            lidl_element.click()
                            self.logger.info("Lidl store selected")
                            lidl_found = True
                            break
                    except Exception:
                        time.sleep(1)
                
                if not lidl_found:
                    raise Exception("Could not find or select Lidl store")
                    
            except Exception as e:
                self.logger.error(f"Failed to set address/store: {e}")
                raise
            
            # Step 5: Open registration modal
            self.logger.info("Opening registration modal")
            try:
                menu_button = page.ele("css:svg.is-ico-menu", timeout=10)
                menu_button.click()
                
                login_selectors = [
                    "//button[contains(text(),'Sign up') or contains(text(),'Registrati')]",
                    "//span[contains(text(),'Sign up') or contains(text(),'Registrati')]",
                    "//div[contains(text(),'Sign up') or contains(text(),'Log in')]",
                    "//*[contains(text(),'Log in') or contains(text(),'Accedi')]",
                ]
                modal_opened = False
                for selector in login_selectors:
                    try:
                        element = page.ele(f"x:{selector}", timeout=2)
                        if element:
                            element.click()
                            self.logger.info(f"Registration modal opened using selector: {selector}")
                            modal_opened = True
                            break
                    except Exception:
                        continue
                if not modal_opened:
                    raise Exception("Could not open registration modal")
                    
            except Exception as e:
                self.logger.error(f"Failed to open registration modal: {e}")
                raise
            
            # Step 6: Enter email and continue
            self.logger.info("Entering email address")
            try:
                email_input = page.ele('css:input[type="email"]', timeout=10)
                email_input.clear()
                self.type_text_humanlike(email_input, email)
                self.human_delay()
                
                self.click_continue_with_email(page)
            except Exception as e:
                self.logger.error(f"Failed to enter email: {e}")
                raise
            
            # Step 7: Enter password and submit
            self.logger.info("Entering password and submitting form")
            try:
                password_input = self.wait_for_password_input(page)
                self.type_text_humanlike(password_input, password)
                self.human_delay()
                submit_button = page.ele('css:button.vader-button[type="submit"]', timeout=10)
                submit_button.click()
                self.logger.info("Registration form submitted")
                
            except Exception as e:
                self.logger.error(f"Failed to submit registration form: {e}")
                raise
            
            self.logger.info("Waiting for confirmation email")
            try:
                confirmation_link = self.poll_for_confirmation_email(token)
                self.logger.info(f"Following confirmation link: {confirmation_link}")

                page.get(confirmation_link)
                page.wait.load_start()
                
                self.logger.info("Waiting for email confirmation page to load...")
                time.sleep(5)
                
                self.logger.info("Waiting for automatic login after email confirmation...")
                
                max_wait_time = 60  
                login_detected = False
                start_time = time.time()
                
                while time.time() - start_time < max_wait_time:
                    current_url = page.url
                    self.logger.debug(f"Current URL: {current_url}")
                    
                    if 'registration-email-confirm' not in current_url:
                        self.logger.info("Detected redirect from confirmation page - login likely successful")
                        login_detected = True
                        break
                    
                    try:
                        login_indicators = page.eles("css:.user-menu, .profile, .logout, .account-menu", timeout=2)
                        if login_indicators:
                            self.logger.info("Found login indicators on page")
                            login_detected = True
                            break
                    except:
                        pass
                    
                    try:
                        continue_buttons = page.eles("x://button[contains(text(),'Continue') or contains(text(),'Continua') or contains(text(),'Go to') or contains(text(),'Vai a')]", timeout=2)
                        if continue_buttons:
                            self.logger.info("Found continue button, clicking it...")
                            continue_buttons[0].click()
                            time.sleep(3)
                    except:
                        pass
                    
                    time.sleep(2)
                
                if not login_detected:
                    self.logger.warning("No clear login detection, but proceeding with token extraction...")
                
                self.logger.info("Waiting for cookies to be set...")
                time.sleep(10)
                
                try:
                    self.logger.info("Navigating to main site to ensure cookies are set...")
                    page.get("https://it.everli.com/")
                    time.sleep(5)
                    
                    try:
                        store_links = page.eles("css:a[href*='store'], a[href*='categories']", timeout=5)
                        if store_links:
                            self.logger.info("Clicking on store/category link to ensure session...")
                            store_links[0].click()
                            time.sleep(5)
                    except:
                        pass
                        
                    try:
                        menu_elements = page.eles("css:.menu, .user-menu, .account", timeout=3)
                        if menu_elements:
                            self.logger.info("Interacting with menu to ensure session...")
                            menu_elements[0].click()
                            time.sleep(3)
                    except:
                        pass
                        
                except Exception as nav_error:
                    self.logger.warning(f"Error during site navigation: {nav_error}")
                
                extractor = SimplifiedTokenExtractor(page, self.logger)
                
                self.logger.info("Starting vAuthToken extraction from cookies...")
                self.authentication_token = extractor.extract_token_with_retries(max_attempts=5)
                
                if self.authentication_token and self.authentication_token != 'null':
                    self.logger.info(f"Successfully extracted vAuthToken: {self.authentication_token[:20]}...")
                    
                    if self.validate_token_with_api_test(self.authentication_token):
                        self.logger.info("vAuthToken validation successful")
                        return self.authentication_token
                    else:
                        self.logger.warning("vAuthToken validation failed, but proceeding anyway")
                        return self.authentication_token
                else:
                    self.logger.error("Failed to extract valid vAuthToken from cookies")
                    
                    # Final attempt: wait longer and try again
                    self.logger.info("Final attempt: waiting longer and trying vAuthToken extraction again...")
                    time.sleep(15)
                    
                    # Try one more navigation to trigger cookie setting
                    try:
                        page.get("https://it.everli.com/s")  # Try to go to shop page
                        time.sleep(5)
                    except:
                        pass
                    
                    self.authentication_token = extractor.extract_token_with_retries(max_attempts=3)
                    
                    if self.authentication_token and self.authentication_token != 'null':
                        self.logger.info(f"vAuthToken extracted on final attempt: {self.authentication_token[:20]}...")
                        return self.authentication_token
                    else:
                        self.logger.error("All vAuthToken extraction attempts failed - no valid token found")

            except Exception as e:
                self.logger.error(f"Failed to confirm email or extract token: {e}")
                raise

        except Exception as e:
            self.logger.error(f"Registration process failed: {e}")

        finally:
            # Only clean up AFTER we have successfully extracted the token
            if page:
                try:
                    # Give a final moment before closing
                    time.sleep(2)
                    page.quit()
                    self.logger.info("Browser session closed")
                except Exception as e:
                    self.logger.warning(f"Error closing browser: {e}")

            if temp_profile and os.path.exists(temp_profile):
                try:
                    shutil.rmtree(temp_profile)
                    self.logger.info(f"Temporary profile cleaned up: {temp_profile}")
                except Exception as e:
                    self.logger.warning(f"Error cleaning up temp profile: {e}")

        return self.authentication_token
    
    def get_headers_for_request(self, authentication_token: str = None, endpoint_url: str = '') -> Dict[str, str]:
        """Get properly formatted headers for API requests with fresh session data."""
        # Use stored token if none provided
        token_to_use = authentication_token or self.authentication_token
        
        if not token_to_use or token_to_use == 'null':
            self.logger.error("No valid authentication token available for headers")
            # Return headers without auth token
            return self.header_manager.generate_base_headers()
        
        # Generate completely fresh headers for each request
        headers = self.header_manager.get_headers_for_api_call(
            authentication_token=token_to_use,
            request_type='GET',
            endpoint_url=endpoint_url
        )
        
        # Add some additional headers that might be required for session maintenance
        headers.update({
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'x-requested-with': 'XMLHttpRequest',
            'dnt': '1',
        })
        
        # Add timestamp to ensure freshness
        headers['x-timestamp'] = str(int(time.time()))
        headers['x-request-time'] = datetime.now().isoformat()
        
        return headers


# Updated helper function for generating headers (replaces your old function)
def generate_random_headers(authentication_token: str) -> Dict[str, str]:
    """
    Legacy function for backward compatibility.
    Use HeaderManager.get_headers_for_api_call() for new code.
    """
    logger = logging.getLogger('everli_bot')
    header_manager = HeaderManager(logger)
    return header_manager.get_headers_for_api_call(authentication_token)


# Main execution code with improvements
def main_execution():
    """Main execution function with improved error handling and structure"""
   
    
    data_products = pd.DataFrame()
    i = 0
    stores_processed = []
    bot = EverliRegistrationBot()
    output_path = "Everli_scrapper_products.csv"
    
    authentication_token = bot.register_and_confirm()
    
    if not authentication_token or authentication_token == 'null':
        bot.logger.error("Failed to obtain valid vAuthToken. Exiting.")
        return
    
    bot.logger.info(f" vAuthToken obtained successfully: {authentication_token[:20]}...")
    
    headers = bot.get_headers_for_request(authentication_token)
    
    bot.logger.info(f"AUTHORIZATION HEADER: {headers.get('authorization', 'NOT FOUND')}")
    
    # Test the token 
    if bot.test_token_with_simple_call():
        bot.logger.info("vAuthToken verified and working!")
    else:
        bot.logger.warning("vAuthToken verification failed, but proceeding anyway...")
    

    while i < len(stores):
        try:
            store_name = stores['name'][i]
            bot.logger.info(f"Processing store {i}: {store_name}")

            area_id = stores['area_id'][i]
            url_id = stores['Url_id'][i]
            currency_id = stores['currency_id'][i]
            country_id = stores['country_id'][i]
            src_id = stores['src_id'][i]
            
            # Debug the original link
            original_link = stores['link'][i]
            bot.logger.debug(f"Original store link: {original_link}")
            
            store_link = original_link.replace('everli://app', '')
            bot.logger.debug(f"After replacing 'everli://app': {store_link}")
            
            # Clean up the store link to avoid double slashes
            if store_link.startswith('/'):
                store_link = store_link[1:]  # Remove leading slash
                bot.logger.debug(f"After removing leading slash: {store_link}")
            
            # Construct the URL properly
            categories_url = f'https://api.everli.com/sm/api/v3/{store_link}/categories/tree'
            bot.logger.info(f"Final categories URL: {categories_url}")
            
            # IMPORTANT: Generate fresh headers for each store
            headers = bot.get_headers_for_request(authentication_token, categories_url)
            
            # Use the improved retry mechanism
            resp = bot.make_api_request_with_retry(categories_url, headers)
            
            try:
                category_json = resp.json()
                bot.logger.debug(f"Categories response keys: {list(category_json.keys())}")
                
                if 'data' not in category_json:
                    bot.logger.warning(f"No 'data' in categories response for {store_name}")
                    bot.logger.debug(f"Response content: {category_json}")
                    i += 1
                    continue
                
                if 'menu' not in category_json['data']:
                    bot.logger.warning(f"No 'menu' in categories data for {store_name}")
                    bot.logger.debug(f"Data keys: {list(category_json['data'].keys())}")
                    i += 1
                    continue
                
                category_index = next((h for h, m in enumerate(category_json['data']['menu']) if 'items' in m), None)
                if category_index is None:
                    bot.logger.warning(f"No product categories found in menu for {store_name}")
                    i += 1
                    continue

                raw_categories = category_json['data']['menu'][category_index]['items']
                categories = pd.DataFrame(columns=['name', 'link', 'parent_name'])

                for parent in raw_categories:
                    parent_name = parent['name']
                    categories.loc[len(categories)] = [parent_name, parent['link'], '']
                    for child in parent.get('branch', []):
                        categories.loc[len(categories)] = [child['name'], child['link'], parent_name]

                categories = categories[categories['parent_name'] != ''].reset_index(drop=True)
                all_products = pd.DataFrame()
                
                bot.logger.info(f"Found {len(categories)} categories for {store_name}")

                for j in range(len(categories)):
                    subcategory_name = categories.loc[j, 'name']
                    parent_category = categories.loc[j, 'parent_name']
                    category_link = categories.loc[j, 'link'].replace('#/', '')
                    
                    # Clean up category link
                    if category_link.startswith('/'):
                        category_link = category_link[1:]

                    bot.logger.info(f"Fetching products for: {parent_category} > {subcategory_name}")
                    product_url = f'https://api.everli.com/sm/api/v3/{category_link}'
                    params = {'take': '100000000', 'skip': '0'}
                    
                    # Generate fresh headers for each category request
                    headers = bot.get_headers_for_request(authentication_token, product_url)
                    
                    # Use improved request method with proper headers
                    try:
                        prod_resp = bot.make_api_request_with_retry(product_url, headers, params)
                        product_json = prod_resp.json()

                        if not product_json.get('data'):
                            bot.logger.warning(f"No 'data' in product response for {subcategory_name}")
                            continue

                        if 'body' not in product_json['data'] or not product_json['data']['body']:
                            bot.logger.warning(f"No products found for {subcategory_name} (empty 'body')")
                            continue

                        category_products = pd.DataFrame()
                        for section in product_json['data']['body']:
                            bot.logger.debug(f"Widget type: {section.get('widget_type')}")
                            product_list = section.get('list', [])
                            if not product_list:
                                continue
                            try:
                                products = pd.json_normalize(product_list)
                                products['cat_name_org'] = parent_category
                                products['sub_cat_name_org'] = subcategory_name
                                products['scrape_timestamp'] = datetime.now(zone_dubai).strftime("%Y-%m-%d %H:%M:%S")
                                category_products = pd.concat([category_products, products])
                                bot.logger.info(f"Found {len(products)} products in {subcategory_name}")
                            except Exception as parse_err:
                                bot.logger.warning(f"Parsing error in {subcategory_name}: {parse_err}")

                        all_products = pd.concat([all_products, category_products])
                        
                    except Exception as cat_e:
                        bot.logger.warning(f"Error fetching {subcategory_name}: {cat_e}")
                        continue
                        
                    time.sleep(0.5)

                all_products['store_name'] = store_name
                all_products['store_id'] = stores['id'][i]
                all_products['Source_file_id'] = source_file_ID
                all_products['Url_id'] = url_id
                all_products['currency_id'] = currency_id
                all_products['area_id'] = area_id
                all_products['country_id'] = country_id
                all_products['src_id'] = src_id

                write_header = not os.path.exists(output_path)
                all_products.to_csv(output_path, mode='a', header=write_header, index=False, encoding='utf-8-sig')
                bot.logger.info(f"Store {store_name} ({i}) processed and written to CSV with {len(all_products)} rows.")

                stores_processed.append(i)
                
            except Exception as json_e:
                bot.logger.error(f"Error processing categories JSON for {store_name}: {json_e}")
                
            i += 1
            
            time.sleep(1)

        except Exception as e:
            bot.logger.error(f"Error while processing store index {i} ({stores['name'][i] if i < len(stores) else 'Unknown'}): {e}")
            i += 1 
            continue

    
    bot.logger.info("Processing completed successfully")


if __name__ == "__main__":
    main_execution()