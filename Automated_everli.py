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
import csv
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any
from DrissionPage import ChromiumPage, ChromiumOptions
import pandas as pd
import random 
import secrets
from pytz import timezone

# Add timezone for Dubai if needed
zone_dubai = timezone('Asia/Dubai')
source_file_ID = 267162
stores=pd.read_csv("stores.csv")
class SimplifiedTokenExtractor:    
    def __init__(self, page, logger):
        self.page = page
        self.logger = logger
        self.captured_token = None    
    def extract_vauth_token_from_cookies(self) -> Optional[str]:
        try:
            self.logger.log_info("Attempting to extract vAuthToken from cookies")
            time.sleep(3)
            cookies = self.page.cookies()
            for cookie in cookies:
                cookie_name = cookie.get('name', '')
                cookie_value = cookie.get('value', '')
                if cookie_name == 'vAuthToken' and cookie_value:
                    if len(cookie_value) > 10 and cookie_value != 'null':
                        self.logger.log_success(f"vAuthToken found in cookies: {cookie_value}")
                        self.captured_token = cookie_value
                        return cookie_value   
            self.logger.log_warning("vAuthToken cookie not found")
            return None                
        except Exception as e:
            self.logger.log_error(f"Error extracting vAuthToken from cookies: {e}")
            return None
class HeaderManager:    
    def __init__(self, logger):
        self.logger = logger
        self.session_id = None
        self.device_fingerprint = None
    def get_headers_for_api_call(self, 
                                authentication_token: str,
                                request_type: str = 'GET',
                                endpoint_url: str = '',
                                referer: Optional[str] = None) -> Dict[str, str]:
        base_headers = self.generate_base_headers(authentication_token)
        
        if referer:
            base_headers['referer'] = referer
        self.logger.log_debug(f"Generated headers for {request_type} {endpoint_url}")
        self.logger.log_debug(f"Auth header present: {'authorization' in base_headers}")

        return base_headers
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
            'accept': 'application/json, text/plain, /',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'if-none-match': 'W/"50d7e1bef6bfcedccfeec1e1b5e1fb9b"',
            'origin': 'https://it.everli.com',
            'priority': 'u=1, i',
            'referer': 'https://it.everli.com/',
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
        }  
        if authentication_token:
            token = str(authentication_token).strip().strip('"\'')
            if token and token != 'null':
                if not token.startswith('Bearer '):
                    token = f'Bearer {token}'
                headers['authorization'] = token
        return headers

class StructuredLogger:
    """CSV-only structured logger for scraper operations"""
    
    def __init__(self, log_dir: str, scraper_name: str, source: str, schedule: str, machine_id: str, job_id: int = 1):
        self.log_dir = log_dir
        self.scraper_name = scraper_name
        self.source = source
        self.schedule = schedule
        self.machine_id = machine_id
        self.job_id = job_id
        self.start_time = time.time()
        self.last_step_time=self.start_time
        
        
        # Create log directory
        os.makedirs(log_dir, exist_ok=True)
        
        # Setup CSV logger only
        self.csv_log_path = os.path.join(log_dir, f'scraper_logs_{datetime.now().strftime("%Y-%m-%d")}.csv')
        self._setup_csv_logger()
        
        # Current context tracking
        self.current_category = ""
        self.current_subcategory = ""
        self.current_product_url = ""
        self.current_status = "in_progress"
    
    def _setup_csv_logger(self):
        """Setup CSV logging with headers"""
        self.csv_fieldnames = [
            'asctime', 'levelname', 'filename', 'funcName', 'lineno',
            'scraper_name', 'source', 'schedule', 'machine_id', 'job_id',
            'category', 'subcategory', 'product_url', 'duration',
            'status', 'error_message', 'data_size', 'inconsistent_data_count', 'message'
        ]
        
        # Create CSV file with headers if it doesn't exist
        if not os.path.exists(self.csv_log_path):
            with open(self.csv_log_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_fieldnames)
                writer.writeheader()
    
    def _get_caller_info(self):
        """Get information about the calling function"""
        import inspect
        frame = inspect.currentframe()
        try:
            # Go up the stack to find the actual caller (skip _log_to_csv and log_* methods)
            caller_frame = frame.f_back.f_back.f_back
            if caller_frame:
                return {
                    'filename': os.path.basename(caller_frame.f_code.co_filename),
                    'funcName': caller_frame.f_code.co_name,
                    'lineno': caller_frame.f_lineno
                }
        finally:
            del frame
        return {'filename': 'unknown', 'funcName': 'unknown', 'lineno': 0}
    
    def _log_to_csv(self, level: str, message: str, error_message: str = "", 
                data_size: int = 0, inconsistent_data_count: int = 0):
        """Log structured data to CSV and print to console"""
        caller_info = self._get_caller_info()
        total_seconds = time.time() - self.last_step_time
        self.last_step_time=time.time()
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)
        duration = f"{minutes}m {seconds}s"
        
        
        log_entry = {
            'asctime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'levelname': level,
            'filename': caller_info['filename'],
            'funcName': caller_info['funcName'],
            'lineno': caller_info['lineno'],
            'scraper_name': self.scraper_name,
            'source': self.source,
            'schedule': self.schedule,
            'machine_id': self.machine_id,
            'job_id': self.job_id,
            'category': self.current_category,
            'subcategory': self.current_subcategory,
            'product_url': self.current_product_url,
            'duration': duration,
            'status': self.current_status,
            'error_message': error_message,
            'data_size': data_size,
            'inconsistent_data_count': inconsistent_data_count,
            'message': message
        }
        
        # Print to console
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{timestamp} - {level} - {message}")
        
        try:
            with open(self.csv_log_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_fieldnames)
                writer.writerow(log_entry)
        except Exception as e:
            print(f"Failed to write to CSV log: {e}")
    
    def set_context(self, category: str = "", subcategory: str = "", 
                   product_url: str = "", status: str = ""):
        """Update current context for logging"""
        if category:
            self.current_category = category
        if subcategory:
            self.current_subcategory = subcategory
        if product_url:
            self.current_product_url = product_url
        if status:
            self.current_status = status
    
    def log_info(self, message: str, data_size: int = 0, inconsistent_data_count: int = 0):
        """Log info level message"""
        self._log_to_csv("INFO", message, data_size=data_size, 
                        inconsistent_data_count=inconsistent_data_count)
    
    def log_warning(self, message: str, data_size: int = 0, inconsistent_data_count: int = 0):
        """Log warning level message"""
        self._log_to_csv("WARNING", message, data_size=data_size, 
                        inconsistent_data_count=inconsistent_data_count)
    
    def log_error(self, message: str, error: Exception = None, data_size: int = 0, 
                 inconsistent_data_count: int = 0):
        """Log error level message"""
        error_message = str(error) if error else ""
        self.set_context(status="fail")
        self._log_to_csv("ERROR", message, error_message=error_message, 
                        data_size=data_size, inconsistent_data_count=inconsistent_data_count)
    
    def log_debug(self, message: str, data_size: int = 0, inconsistent_data_count: int = 0):
        """Log debug level message"""
        self._log_to_csv("DEBUG", message, data_size=data_size, 
                        inconsistent_data_count=inconsistent_data_count)
    
    def log_job_start(self):
        """Log job start"""
        self.set_context(status="starting")
        self.log_info(f"Job {self.job_id} started for {self.scraper_name}")
    
    def log_job_end(self, total_data_size: int = 0):
        """Log job completion"""
        self.set_context(status="ending")
        duration = round(time.time() - self.start_time, 2)
        self.log_info(f"Job {self.job_id} completed. Total duration: {duration}s", 
                     data_size=total_data_size)
    
    def log_success(self, message: str, data_size: int = 0):
        """Log successful operation"""
        self.set_context(status="success")
        self.log_info(message, data_size=data_size)
class EverliRegistrationBot: 
    MAIL_TM_API = "https://api.mail.tm"
    LOG_DIR = "Everli_logs"
    def __init__(self):
        self.machine_id = socket.gethostname()
        self.job_id = str(uuid.uuid4())
        self.logger = StructuredLogger(
            log_dir=self.LOG_DIR,
            scraper_name="everli_scraper",
            source="it.everli.com",
            schedule="daily",  
            machine_id=self.machine_id,
            job_id=self.job_id
        )
        self.header_manager = HeaderManager(self.logger)
        self.authentication_token = None  
        self.session = requests.Session()  
        self.last_keep_alive = time.time()
        self._cleanup_old_logs()
    def _cleanup_old_logs(self, retention_days: int = 7) -> None:
        try:
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            log_pattern = os.path.join(self.LOG_DIR, 'scraper_logs_*.csv')
            for log_file in glob.glob(log_pattern):
                try:
                    file_date_str = os.path.basename(log_file).replace('scraper_logs_', '').replace('.csv', '')
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    if file_date < cutoff_date:
                        os.remove(log_file)
                        self.logger.log_info(f"Removed old log file: {log_file}")  
                except (ValueError, OSError) as e:
                        self.logger.log_warning(f"Could not process log file {log_file}: {e}") 
        except Exception as e:
                self.logger.log_error(f"Error during log cleanup", e)
    
    @staticmethod
    def human_delay(min_seconds: float = 0.5, max_seconds: float = 1.5) -> None:
        """Simulate human-like delays in automation."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    def refresh_authentication(self, max_retries: int = 3, base_delay: float = 5.0) -> bool:
        try:
            self.logger.log_info("Attempting to extend session with keep-alive request")
            keep_alive_url = "https://api.everli.com/sm/api/v3/stores?latitude=45.46427&longitude=9.18951"
            headers = self.get_headers_for_request(self.authentication_token, keep_alive_url)
            params = {'skip': '0', 'take': '10'} 
            for attempt in range(max_retries):
                try:
                    response = self.session.get(keep_alive_url, headers=headers, params=params, timeout=10)
                    if response.status_code == 200:
                        self.logger.log_success("Session extended successfully via keep-alive request")
                        self.last_keep_alive = time.time()
                        return True
                    elif response.status_code == 429:
                        delay = base_delay * (2 ** attempt)
                        self.logger.log_warning(f"Rate limited (429) on keep-alive attempt {attempt + 1}. Waiting {delay}s before retry.")
                        time.sleep(delay)
                        continue
                    elif response.status_code == 401:
                        self.logger.log_warning("Keep-alive request failed with 401, attempting re-registration")
                        break
                    else:
                        self.logger.log_warning(f"Keep-alive request failed with status {response.status_code}")
                        break
                except Exception as e:
                    self.logger.log_warning(f"Keep-alive attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        self.logger.log_info(f"Retrying keep-alive after {delay}s")
                        time.sleep(delay)
                    continue
            self.logger.log_info("Falling back to re-registration after keep-alive failures")
            try:
                page, temp_profile = self.setup_browser()
                page.get("https://it.everli.com/")
                time.sleep(2)
                self.logout_current_session(page)
                page.quit()
                if temp_profile and os.path.exists(temp_profile):
                    shutil.rmtree(temp_profile)
                    self.logger.log_info(f"Cleaned up temp profile before re-registration: {temp_profile}")
            except Exception as e:
                self.logger.log_warning(f"Failed to fully logout: {e}")

            new_token = self.register_and_confirm()
            if new_token and new_token != 'null':
                self.authentication_token = new_token
                self.session.cookies.set('vAuthToken', new_token, domain='it.everli.com')
                self.logger.log_success(f"New vAuthToken obtained: {new_token}")
                self.last_keep_alive = time.time()
                return True
            else:
                self.logger.log_error("Failed to obtain new valid token")
                return False

        except Exception as e:
            self.logger.log_error(f"Re-registration failed: {e}")
            return False
    def create_temporary_email(self) -> Tuple[str, str, str]:
        """Create a temporary email account using Mail.tm API."""
        max_retries = 3
        base_delay = 3
        for attempt in range(max_retries):
            try:
                self.logger.log_info(f"Creating temporary email account (attempt {attempt + 1})")
                
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
                email_token = token_response.json()["token"]
                
                self.logger.log_info(f"Successfully created temporary email: {email}")
                return email, password, email_token
            except Exception as e:
                self.logger.log_warning(f"Mail.tm creation attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  
                    time.sleep(delay)
                else:
                    self.logger.log_error("Failed to create temporary email after all retries")
                    raise Exception("Could not create temporary email account")
    def poll_for_confirmation_email(self, email_token: str, timeout: int = 300) -> str:
        """Poll Mail.tm for confirmation email with verification link."""
        deadline = time.time() + timeout
        poll_interval = 10
        
        self.logger.log_info(f"Polling for confirmation email (timeout: {timeout}s)")
        
        while time.time() < deadline:
            try:
                headers = {"Authorization": f"Bearer {email_token}"}
                response = requests.get(f"{self.MAIL_TM_API}/messages", headers=headers, timeout=10)
                response.raise_for_status()
                messages = response.json().get("hydra:member", [])
                if messages:
                    message_id = messages[0]["id"]
                    msg_response = requests.get(f"{self.MAIL_TM_API}/messages/{message_id}",headers=headers,timeout=10)
                    msg_response.raise_for_status()
                    message = msg_response.json()
                    content = message.get("html") or message.get("text") or ""
                    if isinstance(content, list):
                        content = "".join(content)
                    match = re.search(r'(https://it\.everli\.com[^\s"<]+)', content)
                    if match and match.group(1) != "https://it.everli.com/":
                        link = match.group(1)
                        self.logger.log_success(f"Found confirmation link: {link}")
                        self.logger.set_context(product_url=link)
                        return link
                    fallback_match = re.search(r'https://[^\s"<]+', content)
                    if fallback_match and fallback_match.group(0) != "https://it.everli.com/":
                        link = fallback_match.group(0)
                        self.logger.log_info(f"Using fallback confirmation link: {link}")
                        self.logger.set_context(product_url=link)
                        return link
                    self.logger.log_debug("Email received but no usable confirmation link found")
            except Exception as e:
                self.logger.log_warning(f"Error while polling for email: {e}")
            time.sleep(poll_interval)
        raise RuntimeError(f"No confirmation email received within {timeout} seconds")
    def type_text_humanlike(self, element, text: str, delay: float = 0.1) -> None:
        """Type text into an element with human-like delays between characters."""
        try:
            element.clear()
            for char in text:
                element.input(char)
                time.sleep(delay)
            self.logger.log_debug(f"Successfully typed text into element")
        except Exception as e:
            self.logger.log_error(f"Failed to type text: {e}")
            raise
    def wait_for_password_input(self, page, max_attempts: int = 10) -> object:
        """Wait for password input field to appear on the page."""
        for attempt in range(max_attempts):
            try:
                inputs = page.eles("tag:input")
                for input_elem in inputs:
                    if input_elem.attrs.get("type") == "password":
                        self.logger.log_info("Password input field found")
                        return input_elem
                
                self.logger.log_debug(f"Password input not found, attempt {attempt + 1}")
                time.sleep(2)
            except Exception as e:
                self.logger.log_warning(f"Error while searching for password input: {e}")
                time.sleep(2)
        raise Exception("Password input field not found after maximum attempts")
    def click_continue_with_email(self, page, max_attempts: int = 10) -> None:
        """Find and click the 'Continue with Email' button."""
        for attempt in range(max_attempts):
            try:
                self.logger.log_debug(f"Looking for 'Continue with Email' button (attempt {attempt + 1})")
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
                        self.logger.log_success("Successfully clicked 'Continue with Email' button")
                        return
                time.sleep(2)
            except Exception as e:
                self.logger.log_warning(f"Error clicking continue button: {e}")
                time.sleep(2)
        raise Exception("'Continue with Email' button not found or not clickable")
    def setup_browser(self) -> Tuple[ChromiumPage, str]:
        """Set up Chrome browser with temporary profile."""
        try:
            temp_profile = tempfile.mkdtemp(prefix="everli_profile_")
            options = ChromiumOptions()
            options.headless(False)
            options.set_argument("--start-maximized")
            options.set_argument("--disable-blink-features=AutomationControlled")
            options.set_argument("--disable-extensions")
            options.set_user_data_path(temp_profile)
            page = ChromiumPage(options)
            self.logger.log_info(f"Browser initialized with temp profile: {temp_profile}")
            return page, temp_profile
        except Exception as e:
            self.logger.log_error(f"Failed to setup browser: {e}")
            raise
    def extract_token_from_page(self, page) -> Optional[str]:
        """Extract vAuthToken using the SimplifiedTokenExtractor"""
        try:
            token_extractor = SimplifiedTokenExtractor(page, self.logger)
            token = token_extractor.extract_vauth_token_from_cookies()
            return token
        except Exception as e:
            self.logger.log_error(f"Error extracting token: {e}")
            return None
    def logout_current_session(self, page):
        try:
            self.logger.log_info("Attempting to clear session before logout")
            page.run_js("window.localStorage.clear();")
            page.run_js("window.sessionStorage.clear();")
            page.delete_all_cookies()
            self.logger.log_success("Cleared cookies and storage")
        except Exception as e:
            self.logger.log_warning(f"Error while clearing session data: {e}")


    def register_and_confirm(self) -> Optional[str]:
        page = None
        temp_profile = None
        try:
            # Step 1: Create temporary email
            self.logger.log_info("Starting Everli account registration process")
            email, password, email_token = self.create_temporary_email()
            # Step 2: Setup browser
            page, temp_profile = self.setup_browser()
            self.page=page
            # Step 3: Navigate to Everli and handle cookies
            self.logger.log_info("Navigating to Everli website")
            page.get("https://it.everli.com/")
            self.human_delay()
            # Accept cookies
            try:
                accept_button = page.ele('x://button[contains(text(), "Accept")]', timeout=5)
                if accept_button:
                    accept_button.click()
                    self.logger.log_success("Cookie consent accepted")
            except Exception:
                self.logger.log_debug("No cookie consent dialog found")
            # Step 4: Enter address and select Lidl
            self.logger.log_info("Entering address and selecting store")
            try:
                address_input = page.ele('css:input[placeholder*="via Marco Polo"]', timeout=10)
                address_input.click()
                address_input.clear()
                address_input.input("3,via Gaudenzio Ferrari, Milano")
                self.human_delay(1, 2)
                address_input.input("\ue015\ue007")
                time.sleep(4)
                # Select Lidl
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
                            self.logger.log_success("Lidl store selected")
                            lidl_found = True
                            break
                    except Exception:
                        time.sleep(1)
                if not lidl_found:
                    raise Exception("Could not find or select Lidl store") 
            except Exception as e:
                self.logger.log_error(f"Failed to set address/store: {e}")
                raise
            
            # Step 5: Open registration modal
            self.logger.log_info("Opening registration modal")
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
                            self.logger.log_success(f"Registration modal opened using selector: {selector}")
                            modal_opened = True
                            break
                    except Exception:
                        continue
                if not modal_opened:
                    raise Exception("Could not open registration modal")  
            except Exception as e:
                self.logger.log_error(f"Failed to open registration modal: {e}")
                raise
            self.logger.log_info("Entering email address")
            try:
                email_input = page.ele('css:input[type="email"]', timeout=10)
                email_input.clear()
                self.type_text_humanlike(email_input, email)
                self.human_delay()
                self.click_continue_with_email(page)
            except Exception as e:
                self.logger.log_error(f"Failed to enter email: {e}")
                raise
            # Step 7: Enter password and submit
            self.logger.log_info("Entering password and submitting form")
            try:
                password_input = self.wait_for_password_input(page)
                self.type_text_humanlike(password_input, password)
                self.human_delay()
                submit_button = page.ele('css:button.vader-button[type="submit"]', timeout=10)
                submit_button.click()
                self.logger.log_success("Registration form submitted")
            except Exception as e:
                self.logger.log_error(f"Failed to submit registration form: {e}")
                raise
            # Step 8: Get confirmation link and complete login flow
            self.logger.log_info("Waiting for confirmation email")
            try:
                confirmation_link = self.poll_for_confirmation_email(email_token)
                self.logger.log_info(f"Following confirmation link: {confirmation_link}")
                # Navigate to confirmation link
                page.get(confirmation_link)
                page.wait.load_start()
                # Wait for initial page load
                self.logger.log_info("Waiting for email confirmation page to load...")
                time.sleep(5)
                # Look for confirmation success and automatic redirect/login
                self.logger.log_info("Waiting for automatic login after email confirmation...")
                # Check if we're redirected to main site (logged in)
                max_wait_time = 60  
                login_detected = False
                start_time = time.time()
                while time.time() - start_time < max_wait_time:
                    current_url = page.url
                    self.logger.log_debug(f"Current URL: {current_url}")
                    if 'registration-email-confirm' not in current_url:
                        self.logger.log_success("Detected redirect from confirmation page - login likely successful")
                        login_detected = True
                        token_extractor = SimplifiedTokenExtractor(page, self.logger)
                        self.authentication_token = token_extractor.extract_vauth_token_from_cookies()
                        if self.authentication_token:
                            self.logger.log_success(f"vAuthToken extracted successfully: {self.authentication_token}")
                        else:
                            self.logger.log_warning("vAuthToken not found in cookies after redirect")
                        break
                    time.sleep(2)               
                if self.authentication_token and self.authentication_token != 'null':
                    self.logger.log_success(f"Successfully extracted vAuthToken: {self.authentication_token}")
                    time.sleep(15)
            except Exception as e:
                self.logger.log_error(f"Failed to confirm email or extract token: {e}")
                raise
        except Exception as e:
            self.logger.log_error(f"Registration process failed: {e}")
        finally:
            if page:
                try:
                    # time.sleep(2)
                    # page.quit()
                    self.logger.log_info("Browser session closed")
                except Exception as e:
                    self.logger.log_warning(f"Error closing browser: {e}")
            if temp_profile and os.path.exists(temp_profile):
                try:
                    shutil.rmtree(temp_profile)
                    self.logger.log_success(f"Temporary profile cleaned up: {temp_profile}")
                except Exception as e:
                    self.logger.log_warning(f"Error cleaning up temp profile: {e}")
        return self.authentication_token
    def get_headers_for_request(self, authentication_token: str = None, endpoint_url: str = '') -> Dict[str, str]:
        """Get properly formatted headers for API requests with fresh session data."""
        token_to_use = authentication_token or self.authentication_token
        if not token_to_use or token_to_use == 'null':
            self.logger.log_error("No valid authentication token available for headers")
            return self.header_manager.generate_base_headers()
        headers = self.header_manager.get_headers_for_api_call(
            authentication_token=token_to_use,
            request_type='GET',
            endpoint_url=endpoint_url
        )
        headers['x-timestamp'] = str(int(time.time()))
        headers['x-request-time'] = datetime.now().isoformat()
        
        return headers
def main_execution():
    start_index = 0
    stores_done = []
    master_csv_path = "DataProducts.csv"
    checkpoint_file = "scraper_checkpoint.json"
    data_products = pd.DataFrame()
    bot = EverliRegistrationBot()
    bot.logger.log_job_start()
    total_data_size = 0

    # Load checkpoint if exists
    checkpoint = {'store_index': 0, 'category_index': 0, 'last_processed_product_id': None}
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
            start_index = checkpoint['store_index']
            bot.logger.log_info(f"Loaded checkpoint: Starting from store index {start_index}, category index {checkpoint['category_index']}")
        except Exception as e:
            bot.logger.log_warning(f"Failed to load checkpoint: {e}. Starting from scratch.")
    # Load existing products to prevent duplicates
    existing_products = set()
    if os.path.exists(master_csv_path):
        try:
            existing_df = pd.read_csv(master_csv_path)
            if 'id' in existing_df.columns:
                existing_products = set(existing_df['id'].astype(str))
            bot.logger.log_info(f"Loaded {len(existing_products)} existing product IDs from {master_csv_path}")
        except Exception as e:
            bot.logger.log_warning(f"Failed to load existing products: {e}")
    # Obtain authentication token
    authentication_token = bot.register_and_confirm()
    if not authentication_token or authentication_token == 'null':
        bot.logger.log_error("Failed to obtain valid vAuthToken. Exiting.")
        bot.logger.log_job_end(total_data_size)
        return
    bot.logger.log_success(f"vAuthToken obtained successfully: {authentication_token}")
    headers = bot.get_headers_for_request(authentication_token)
    while start_index < len(stores):
        i = start_index
        start_time = datetime.now()
        bot.logger.log_info(f"Start processing Store {i} - {stores['name'][i]}")
        try:
            product_full_batch = pd.DataFrame()
            area_id = stores['area_id'][i]
            url_id = stores['Url_id'][i]
            currency_id = stores['currency_id'][i]
            country_id = stores['country_id'][i]
            src_id = stores['src_id'][i]
            store_link = stores['link'][i].replace('everli://app', '')
            page = f"https://api.everli.com/sm/api/v3/{store_link}/categories/tree"
            # Make API request for categories
            resp = requests.get(page, headers=headers)
            if resp.status_code == 429:
                bot.logger.log_error("Got 429 error at store level. Refreshing token and retrying.")
                if bot.refresh_authentication():
                    authentication_token = bot.authentication_token
                    headers = bot.get_headers_for_request(authentication_token)
                    continue
                else:
                    raise Exception("Failed to refresh authentication token")
            if resp.status_code != 200:
                raise Exception(f"Blocked or invalid token (status {resp.status_code})")
            categories_json = resp.json()
            categories_list = []
            for h in range(len(categories_json['data']['menu'])):
                if 'items' in categories_json['data']['menu'][h]:
                    ind = h
                    cat_list = categories_json['data']['menu'][ind]['items']
                    for cat in cat_list:
                        parent_name = cat['name']
                        categories_list.append({'name': parent_name, 'link': cat['link'], 'parent_name': ''})
                        for sub_cat in cat.get('branch', []):
                            categories_list.append({'name': sub_cat['name'], 'link': sub_cat['link'], 'parent_name': parent_name})
            categories_df = pd.DataFrame(categories_list)
            categories_df = categories_df[categories_df['parent_name'] != ''].reset_index(drop=True)
            bot.logger.log_success(f"Categories found: {len(categories_df)}")
            products_from_all_categories = pd.DataFrame()
            # Start from checkpoint category index
            j = checkpoint.get('category_index', 0)
            while j < len(categories_df):
                try:
                    bot.logger.set_context(category=cat, subcategory=sub_cat)
                    bot.logger.log_info(f"Scraping category {j+1}/{len(categories_df)} - {categories_df.loc[j, 'name']}")
                    cat = categories_df.loc[j, 'parent_name']
                    sub_cat = categories_df.loc[j, 'name']
                    cat_link = categories_df.loc[j, 'link'].replace('#/', '')
                    params = {'take': '100000000', 'skip': '0'}
                    time.sleep(1.5)
                    prod_resp = requests.get(f"https://api.everli.com/sm/api/v3/{cat_link}", params=params, headers=headers)
                    if prod_resp.status_code == 429:
                        bot.logger.log_debug(f"429 error at category {j} — refreshing token and retrying")
                        if bot.refresh_authentication():
                            authentication_token = bot.authentication_token
                            headers = bot.get_headers_for_request(authentication_token)
                            continue
                        else:
                            raise Exception("Failed to refresh authentication token")
                    prod_resp.raise_for_status()
                    prod_data = prod_resp.json()
                    subcategory_products = pd.DataFrame()
                    # Process products in the category
                    product_list = []
                    for block in prod_data['data']['body']:
                        if block.get('widget_type') == 'vertical-list':
                            product_list.extend(block.get('list', []))
                    # Filter out already processed products
                    start_processing = True if not checkpoint.get('last_processed_product_id') else False
                    products_processed=0
                    for product in product_list:
                        product_id = str(product.get('id'))
                        if product_id == checkpoint.get('last_processed_product_id'):
                            start_processing = True
                            continue
                        if start_processing and product_id not in existing_products:
                            product_df = pd.json_normalize([product])
                            product_df['cat_name_org'] = cat
                            product_df['sub_cat_name_org'] = sub_cat
                            product_df['nw'] = datetime.now(zone_dubai).strftime("%Y-%m-%d %H:%M:%S")
                            subcategory_products = pd.concat([subcategory_products, product_df])
                            existing_products.add(product_id)
                            products_processed += 1


                    if not subcategory_products.empty:
                        products_from_all_categories = pd.concat([products_from_all_categories, subcategory_products])
                        product_size = len(subcategory_products.to_csv(index=False).encode('utf-8'))
                        total_data_size += product_size
                        bot.logger.log_success(f"Processed {products_processed} products from category {sub_cat}", 
                                             data_size=product_size)
                    # Update checkpoint after each category
                    checkpoint = {
                        'store_index': i,
                        'category_index': j + 1,
                        'last_processed_product_id': None
                    }
                    with open(checkpoint_file, 'w') as f:
                        json.dump(checkpoint, f)
                    bot.logger.log_debug(f"Updated checkpoint: store {i}, category {j+1}")
                    j += 1
                except Exception as e:
                    bot.logger.log_error(f"Error at category {j}: {str(e)}")
                    # Save checkpoint with last processed product
                    if subcategory_products.empty:
                        checkpoint['last_processed_product_id'] = None
                    else:
                        last_product_id = str(subcategory_products['id'].iloc[-1]) if 'id' in subcategory_products.columns else None
                        checkpoint['last_processed_product_id'] = last_product_id
                    with open(checkpoint_file, 'w') as f:
                        json.dump(checkpoint, f)
                    bot.logger.log_debug(f"Checkpoint saved due to error: {checkpoint}")
                    if "429" in str(e):
                        bot.logger.log_error("Got 429 error at category level. Refreshing token.")
                        if bot.refresh_authentication():
                            authentication_token = bot.authentication_token
                            headers = bot.get_headers_for_request(authentication_token)
                            time.sleep(5)
                            continue
                        else:
                            bot.logger.log_error("Failed to refresh authentication token. Moving to next store.")
                            start_index += 1
                            checkpoint = {'store_index': start_index, 'category_index': 0, 'last_processed_product_id': None}
                            with open(checkpoint_file, 'w') as f:
                                json.dump(checkpoint, f)
                            break
                    else:
                        bot.logger.log_info("Retrying category after error...")
                        if bot.refresh_authentication():
                            authentication_token = bot.authentication_token
                            headers = bot.get_headers_for_request(authentication_token)
                            time.sleep(5)
                            continue
                        else:
                            bot.logger.log_error("Failed to refresh authentication token. Moving to next store.")
                            start_index += 1
                            checkpoint = {'store_index': start_index, 'category_index': 0, 'last_processed_product_id': None}
                            with open(checkpoint_file, 'w') as f:
                                json.dump(checkpoint, f)
                            break
            if not products_from_all_categories.empty:
                product_full_batch = products_from_all_categories.copy()
                product_full_batch['store_name'] = stores['name'][i]
                product_full_batch['store_id'] = stores['id'][i]
                product_full_batch['source_file_id'] = source_file_ID
                product_full_batch['url_id'] = url_id
                product_full_batch['currency_id'] = currency_id
                product_full_batch['area_id'] = area_id
                product_full_batch['country_id'] = country_id
                product_full_batch['src_id'] = src_id
                # Append to CSV
                product_full_batch.to_csv(
                    master_csv_path,
                    mode='a',
                    index=False,
                    header=not os.path.exists(master_csv_path)
                )
                data_products = pd.concat([data_products, product_full_batch], ignore_index=True)
                batch_size = len(product_full_batch.to_csv(index=False).encode('utf-8'))
                bot.logger.log_success(f"Appended {len(product_full_batch)} products to {master_csv_path}", 
                                     data_size=batch_size)
                stores_done.append(i)
            else:
                bot.logger.log_error(f"No data saved for store {i}: empty product set")

            duration = round((datetime.now() - start_time).total_seconds() / 60, 2)
            bot.logger.log_info(f"Duration for store {i}: {duration} min")
            start_index += 1
            checkpoint = {'store_index': start_index, 'category_index': 0, 'last_processed_product_id': None}
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
        except Exception as e:
            bot.logger.log_error(f"Critical error at store {i}: {str(e)}")
            checkpoint['store_index'] = i
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
            if "429" in str(e):
                bot.logger.log_error("Got 429 error at store level. Refreshing token and retrying.")
                if bot.refresh_authentication():
                    authentication_token = bot.authentication_token
                    headers = bot.get_headers_for_request(authentication_token)
                    time.sleep(5)
                    continue
                else:
                    bot.logger.log_error("Failed to refresh authentication token. Moving to next store.")
                    start_index += 1
                    checkpoint = {'store_index': start_index, 'category_index': 0, 'last_processed_product_id': None}
                    with open(checkpoint_file, 'w') as f:
                        json.dump(checkpoint, f)
            else:
                bot.logger.log_info("Retrying store after error...")
                if bot.refresh_authentication():
                    authentication_token = bot.authentication_token
                    headers = bot.get_headers_for_request(authentication_token)
                    time.sleep(5)
                    continue
                else:
                    bot.logger.log_error("Failed to refresh authentication token. Moving to next store.")
                    start_index += 1
                    checkpoint = {'store_index': start_index, 'category_index': 0, 'last_processed_product_id': None}
                    with open(checkpoint_file, 'w') as f:
                        json.dump(checkpoint, f)
    bot.logger.log_job_end(total_data_size)
   

if __name__ == "__main__":
    main_execution()