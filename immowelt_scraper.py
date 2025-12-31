import os
import re
import json
import time
import random
from datetime import datetime
from urllib.parse import urljoin
from supabase import Client
from curl_cffi import requests
from logger_config import setup_logger

# Setup logger
logger = setup_logger('immowelt_scraper')


class ImmoweltClient:
    """Immowelt API Client with session management, listing scraping, and contact functionality."""
    
    # API URLs
    LOGIN_START_URL = "https://r.meinbereich.immowelt.de"
    REFRESH_URL = "https://signin.immowelt.de/refresh"
    SEARCH_API_URL = "https://www.immowelt.de/serp-bff/search"
    CONTACT_API_URL = "https://www.immowelt.de/contact-request-service/contacting"
    
    # User Agent
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    
    # Required cookie/token keys
    WANTED_KEYS = [
        "did",
        "auth0",
        "auth0_compat",
        "did_compat",
        "oauth.access.token",
        "oauth.access.expiration"
    ]
    
    def __init__(self):
        # Session tokens (no persistent session, create new request each time)
        self.tokens = {}
        self.session_created_at = None
        
        # Setup proxy from ROTATING_PROXY environment variable
        rotating_proxy = os.getenv('ROTATING_PROXY')
        self.proxies = {
            'http': rotating_proxy,
            'https': rotating_proxy
        }
        logger.info(f"🔒 Using ROTATING_PROXY: {rotating_proxy.split('@')[-1] if rotating_proxy and '@' in rotating_proxy else rotating_proxy}")
    
    # ---------------------------------------------------
    # Token Management
    # ---------------------------------------------------
    def get_cookie_jar(self) -> dict:
        """Build cookie jar from tokens for requests."""
        return {
            "did": self.tokens.get("did"),
            "did_compat": self.tokens.get("did_compat"),
            "auth0": self.tokens.get("auth0"),
            "auth0_compat": self.tokens.get("auth0_compat"),
        }
    
    def extract_tokens_from_cookies(self, cookies) -> dict:
        """Extract required tokens from response cookies."""
        tokens = {}
        if hasattr(cookies, 'items'):
            for name, value in cookies.items():
                if name in self.WANTED_KEYS:
                    tokens[name] = value
        return tokens
    
    def set_tokens_from_dict(self, tokens: dict):
        """Set tokens from dictionary."""
        self.tokens = {k: v for k, v in tokens.items() if k in self.WANTED_KEYS}
    
    def get_session_dict(self) -> dict:
        """Return session details as a dictionary for storage."""
        return {
            **self.tokens,
            'session_created_at': self.session_created_at or datetime.now().isoformat()
        }
    
    def set_session_from_dict(self, session_data: dict):
        """Load session from a dictionary."""
        self.tokens = {k: v for k, v in session_data.items() if k in self.WANTED_KEYS}
        self.session_created_at = session_data.get('session_created_at')
    
    # ---------------------------------------------------
    # Login Flow
    # ---------------------------------------------------
    def login(self, email: str, password: str) -> bool:
        """
        Complete login flow for Immowelt.
        Returns True on success.
        """
        try:
            logger.info(f"🔐 Starting login for {email}...")
            
            # Create a temporary session just for login flow
            session = requests.Session(impersonate="chrome107")
            session.headers.update({
                "User-Agent": self.USER_AGENT,
                "Accept": "*/*",
            })
            
            # Step 1: Get initial login page
            r1 = session.get(self.LOGIN_START_URL, allow_redirects=True, proxies=self.proxies)
            login_page_url = r1.url
            
            # Extract state parameter
            m = re.search(r"state=([^&]+)", login_page_url)
            if not m:
                logger.error("❌ Could not extract OIDC state from login page")
                return False
            
            state = m.group(1)
            
            # Step 2: Submit credentials
            payload = {
                "state": state,
                "username": email,
                "password": password,
            }
            
            r2 = session.post(
                login_page_url,
                data=payload,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://signin.immowelt.de",
                    "Referer": login_page_url,
                },
                allow_redirects=False,
                proxies=self.proxies
            )
            
            # Step 3: Follow redirects to complete OAuth flow
            next_url = r2.headers.get("Location")
            
            while next_url:
                if next_url.startswith("/"):
                    next_url = urljoin("https://signin.immowelt.de", next_url)
                
                r = session.get(next_url, allow_redirects=False, proxies=self.proxies)
                next_url = r.headers.get("Location")
            
            # Extract tokens from session cookies before it gets destroyed
            self.tokens = self.extract_tokens_from_cookies(session.cookies)
            
            if not self.tokens.get("oauth.access.token"):
                logger.error(f"❌ Login failed for {email} - no access token found")
                return False
            
            self.session_created_at = datetime.now().isoformat()
            logger.info(f"✅ Login successful for {email}")
            
            # Session will be destroyed when function exits
            return True
            
        except Exception as e:
            logger.error(f"❌ Login failed for {email}: {e}")
            return False
    
    # ---------------------------------------------------
    # Token Refresh
    # ---------------------------------------------------
    def refresh_session(self) -> bool:
        """Refresh tokens using existing cookies."""
        max_retries = 20
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.info(f"♻️ Refreshing tokens (retry {attempt}/{max_retries})...")
                    time.sleep(2)  # Wait before retry
                else:
                    logger.info("♻️ Refreshing tokens...")
                
                # Build cookie jar from current tokens
                cookie_jar = self.get_cookie_jar()
                
                # Fresh request with current cookies
                r = requests.get(
                    self.REFRESH_URL,
                    impersonate="chrome107",
                    headers={
                        "User-Agent": self.USER_AGENT,
                        "Accept": "*/*",
                        "Origin": "https://www.immowelt.de",
                        "Referer": "https://www.immowelt.de/",
                        "Sec-Fetch-Site": "same-site",
                        "Sec-Fetch-Mode": "cors",
                        "Sec-Fetch-Dest": "empty",
                    },
                    cookies=cookie_jar,
                    proxies=self.proxies
                )
                
                # Check for captcha or 403 in response
                if r.status_code == 403 or 'captcha' in r.text.lower() or '403' in r.text.lower():
                    logger.warning(f"⚠️ Captcha/403 detected (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        continue
                
                if r.status_code != 200:
                    logger.error(f"❌ Token refresh failed: {r.status_code}")
                    if attempt < max_retries - 1:
                        continue
                    return False
                
                # Extract updated tokens from response cookies
                new_tokens = self.extract_tokens_from_cookies(r.cookies)
                if new_tokens:
                    self.tokens.update(new_tokens)
                self.session_created_at = datetime.now().isoformat()
                
                logger.info("✅ Tokens refreshed successfully")
                return True
                
            except Exception as e:
                logger.error(f"❌ Token refresh error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                return False
        
        return False
    
    # ---------------------------------------------------
    # Listing Search
    # ---------------------------------------------------
    def search_listings(self, config: dict) -> list:
        """
        Search for listings using configuration parameters.
        
        Config should contain:
        - criteria: {distributionTypes, estateTypes, location, etc.}
        - paging: {page, size, order}
        
        Returns list of listings with id, url, and published timestamp.
        """
        if not self.tokens.get("oauth.access.token"):
            logger.error("❌ Not authenticated - cannot search listings")
            return []
        
        # Build payload from config
        criteria = config.get('criteria', {})
        paging = config.get('paging', {
            'page': 1,
            'size': 50,
            'order': 'DateDesc'
        })
        
        payload = {
            "criteria": criteria,
            "paging": paging
        }
        
        headers = {
            "user-agent": self.USER_AGENT,
            "accept": "*/*",
            "content-type": "application/json; charset=utf-8",
            "origin": "https://www.immowelt.de",
            "referer": "https://www.immowelt.de/classified-search",
            "Sec-Fetch-Site": "same-origin",
        }
        
        # Log applied filters
        filter_summary = []
        if criteria:
            for key, value in criteria.items():
                # if key != 'location':  # Skip location as it's complex
                filter_summary.append(f"{key}={value}")
        logger.info(f"🔍 Searching listings with filters: {', '.join(filter_summary) if filter_summary else 'none'}")
        
        max_retries = 20
        
        # Build cookie jar from current tokens
        cookie_jar = self.get_cookie_jar()
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.info(f"🔍 Retrying search (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(2)  # Wait before retry
                
                # Fresh request each time
                response = requests.post(
                    self.SEARCH_API_URL,
                    impersonate="chrome107",
                    headers={
                        "user-agent": self.USER_AGENT,
                        "accept": "*/*",
                        "content-type": "application/json; charset=utf-8",
                        "origin": "https://www.immowelt.de",
                        "referer": "https://www.immowelt.de/classified-search",
                        "Sec-Fetch-Site": "same-origin",
                    },
                    cookies=cookie_jar,
                    json=payload,
                    proxies=self.proxies,
                    timeout=30
                )
                
                # Check for captcha or 403 in response
                if response.status_code == 403 or 'captcha' in response.text.lower() or '403' in response.text.lower():
                    logger.warning(f"⚠️ Captcha/403 detected during search (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        continue
                
                if response.status_code != 200:
                    print(response.text)
                    logger.error(f"❌ Search request failed: {response.status_code}")
                    if attempt < max_retries - 1:
                        continue
                    return []
                
                data = response.json()
                current_time = datetime.now()
                
                # Extract listings
                listings = []
                if 'classifieds' in data:
                    for item in data['classifieds']:
                        listing = {
                            'id': item['id'],
                            'url': f"https://www.immowelt.de/expose/{item['id']}",
                            'title': item.get('title', 'Unknown'),
                            'published': current_time.isoformat(timespec="seconds")  # Use current time as Immowelt doesn't provide timestamp
                        }
                        listings.append(listing)
                
                logger.info(f"✅ Found {len(listings)} listings")
                return listings
                
            except Exception as e:
                logger.error(f"❌ Error searching listings (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                return []
        
        return []
    
    # ---------------------------------------------------
    # Contact Listing
    # ---------------------------------------------------
    def contact_listing(self, listing_id: str, contact_form: dict) -> bool:
        """
        Send a contact message to a listing.
        
        contact_form should contain:
        - salutation (e.g., "mr" or "ms")
        - firstName
        - lastName
        - email
        - message
        - householdType
        - workStatus
        - netMonthlyIncome
        - preferredMoveInDate
        """
        if not self.tokens.get("oauth.access.token"):
            logger.error("❌ Not authenticated - cannot contact listing")
            return False
        
        # Build payload
        payload = {
            "salutation": contact_form.get("salutation", "mr"),
            "firstName": contact_form.get("firstName"),
            "lastName": contact_form.get("lastName"),
            "email": contact_form.get("email"),
            "phoneNumber": contact_form.get("phoneNumber", ""),
            "message": contact_form.get("message"),
            "isOwner": False,
            "newsletterOptout": False,
            "newsletterOptInPreference": "email-sms",
            "householdType": contact_form.get("householdType", "1"),
            "workStatus": contact_form.get("workStatus", "6"),
            "netMonthlyIncome": contact_form.get("netMonthlyIncome", "5"),
            "preferredMoveInDate": contact_form.get("preferredMoveInDate", "4"),
            "fullName": "",
            "language": "de",
            "classifiedId": listing_id,
            "brand": "immowelt",
            "houseHoldType": contact_form.get("householdType", "1"),
            "totalNetIncomeBeforeTaxRange": contact_form.get("netMonthlyIncome", "5"),
            "platform": "Website",
        }
        
        # Build cookie jar for request
        cookie_jar = self.get_cookie_jar()
        
        logger.info(f"📤 Contacting listing {listing_id}...")
        
        max_retries = 20
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.info(f"📤 Retrying contact for listing {listing_id} (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(2)  # Wait before retry
                
                # Fresh request each time
                response = requests.post(
                    self.CONTACT_API_URL,
                    impersonate="chrome107",
                    headers={
                        "user-agent": self.USER_AGENT,
                        "accept": "application/json",
                        "content-type": "text/plain;charset=UTF-8",
                        "origin": "https://www.immowelt.de",
                        "referer": "https://www.immowelt.de",
                        "authorization": f"Bearer {self.tokens.get('oauth.access.token')}"
                    },
                    cookies=cookie_jar,
                    json=payload,
                    proxies=self.proxies,
                    timeout=30
                )
                
                # Check for captcha or 403 in response
                if response.status_code == 403 or 'captcha' in response.text.lower() or '403' in response.text.lower():
                    logger.warning(f"⚠️ Captcha/403 detected for listing {listing_id} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        continue
                
                if response.status_code in [200, 201]:
                    logger.info(f"✅ Successfully contacted listing {listing_id}")
                    return True
                else:
                    logger.error(f"❌ Failed to contact listing {listing_id}: {response.status_code}")
                    logger.error(f"   Response: {response.text[:500]}")
                    if attempt < max_retries - 1:
                        continue
                    return False
                    
            except Exception as e:
                logger.error(f"❌ Error contacting listing {listing_id} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                return False
        
        return False


# ===================================================
# UTILITY FUNCTIONS
# ===================================================

def ensure_valid_session(client: ImmoweltClient, account: dict, supabase: Client) -> bool:
    """
    Ensures the client has a valid session for authenticated requests.
    Checks session age - if older than 50 minutes (tokens valid for 60 min), proactively refreshes.
    
    Returns True if session is valid/refreshed, False if no session or login failed.
    """
    session_details = account.get('session_details')
    
    # No session at all - skip (session must be created from frontend)
    if not session_details:
        logger.warning(f"⚠️ [{account['email']}] No session found. Session must be created from frontend first.")
        return False
    
    # Load existing session
    client.set_session_from_dict(session_details)
    
    # Check session age (tokens valid for 60 min, refresh at 50 min)
    session_created_str = session_details.get('session_created_at')
    
    if session_created_str:
        try:
            session_created = datetime.fromisoformat(session_created_str)
            age_minutes = (datetime.now() - session_created).total_seconds() / 60
            
            logger.info(f"🕐 [{account['email']}] Session age: {age_minutes:.1f} minutes")
            
            # If session is older than 50 minutes, refresh it proactively
            if age_minutes > 50:
                logger.warning(f"⚠️ [{account['email']}] Session older than 50 minutes. Refreshing token...")
                
                if not client.refresh_session():
                    logger.error(f"❌ [{account['email']}] Token refresh failed. Re-login required from frontend.")
                    
                    # Automatically disable account to prevent repeated failures
                    config = account.get('configuration', {})
                    config['scrape_enabled'] = False
                    supabase.table('accounts').update({
                        'configuration': config
                    }).eq('id', account['id']).execute()
                    logger.error(f"🔴 [{account['email']}] Auto-disabled account (scrape_enabled=false). Please re-login from frontend.")
                    
                    return False
                
                # Update session in database
                new_session = client.get_session_dict()
                supabase.table('accounts').update({
                    'session_details': new_session
                }).eq('id', account['id']).execute()
                logger.info(f"✅ [{account['email']}] Session refreshed and updated.")
                return True
            else:
                logger.info(f"✅ [{account['email']}] Session is fresh (expires in ~{60 - age_minutes:.0f} minutes).")
                return True
                
        except Exception as e:
            logger.warning(f"⚠️ [{account['email']}] Could not parse session timestamp: {e}")
    
    # If no timestamp or parsing failed, try to refresh
    logger.warning(f"⚠️ [{account['email']}] No valid session timestamp. Attempting token refresh...")
    if client.refresh_session():
        new_session = client.get_session_dict()
        supabase.table('accounts').update({
            'session_details': new_session
        }).eq('id', account['id']).execute()
        logger.info(f"✅ [{account['email']}] Token refreshed and session updated.")
        return True
    
    logger.error(f"❌ [{account['email']}] Token refresh failed. Re-login required from frontend.")
    
    # Automatically disable account to prevent repeated failures
    config = account.get('configuration', {})
    config['scrape_enabled'] = False
    supabase.table('accounts').update({
        'configuration': config
    }).eq('id', account['id']).execute()
    logger.error(f"🔴 [{account['email']}] Auto-disabled account (scrape_enabled=false). Please re-login from frontend.")
    
    return False


# ===================================================
# MAIN SCRAPER FUNCTION
# ===================================================

def run_scraper_for_account(account: dict, supabase: Client):
    """
    Run scraper for a single Immowelt account from Supabase.
    
    - Uses existing session or refreshes if needed
    - Searches for new listings based on configuration
    - Compares with previous listings to find new ones (not by timestamp)
    - Keeps only last 50 listings
    - AUTO-CONTACTS new offers if contact form is configured
    - Updates 'last_updated_at' timestamp
    
    Returns: (success: bool, new_offers_count: int)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"🏃 Running scraper for: {account['email']}")
    logger.info(f"{'='*60}")
    
    # Initialize client (will automatically use ROTATING_PROXY from environment)
    client = ImmoweltClient()
    
    # Get configuration from account
    config = account.get('configuration', {})
    
    # Ensure valid session before scraping
    logger.info(f"🔐 [{account['email']}] Validating session...")
    if not ensure_valid_session(client, account, supabase):
        logger.error(f"❌ [{account['email']}] Could not establish valid session. Cannot fetch listings.")
        return False, 0
    
    # Search for listings
    logger.info(f"🔍 [{account['email']}] Fetching listings...")
    listings = client.search_listings(config)
    
    if not listings:
        logger.warning(f"⚠️ [{account['email']}] No listings found or request failed.")
        # Still update last_updated_at
        supabase.table('accounts').update({
            'last_updated_at': datetime.now().isoformat()
        }).eq('id', account['id']).execute()
        return True, 0
    
    logger.info(f"✅ [{account['email']}] Fetched {len(listings)} listings.")
    
    # Load existing listing_data
    existing_listing_data = account.get('listing_data', {}) or {}
    previous_offers = existing_listing_data.get('offers', [])
    
    # Check if this is the first run (no previous offers)
    is_first_run = len(previous_offers) == 0
    
    if is_first_run:
        logger.info(f"🎯 [{account['email']}] First run detected - initializing with {len(listings)} listings (no contact attempts)")
    
    # Get previous IDs
    previous_ids = {offer['id'] for offer in previous_offers}
    
    # Find new offers (not in previous list)
    new_offers = [listing for listing in listings if listing['id'] not in previous_ids]
    
    if not new_offers:
        logger.info(f"✅ [{account['email']}] No new listings found — everything is up to date.")
        # Still update last_updated_at
        supabase.table('accounts').update({
            'last_updated_at': datetime.now().isoformat()
        }).eq('id', account['id']).execute()
        return True, 0
    
    # Merge: new offers + previous offers, keep only latest 50
    all_offers = new_offers + previous_offers
    all_offers = all_offers[:50]
    
    # Update last_latest to current timestamp
    current_time = datetime.now().strftime("%d.%m.%Y, %H:%M:%S")
    
    # FULLY REPLACE listing_data with updated offers
    # Preserve contacted_ids history from existing data
    updated_listing_data = {
        "last_latest": current_time,
        "offers": all_offers,
        "contacted_ids": existing_listing_data.get('contacted_ids', [])
    }
    
    # Save to Supabase first
    try:
        supabase.table('accounts').update({
            'listing_data': updated_listing_data,
            'last_updated_at': datetime.now().isoformat()
        }).eq('id', account['id']).execute()
        
        logger.info(f"🆕 [{account['email']}] Added {len(new_offers)} new offers.")
        logger.info(f"📅 [{account['email']}] Updated last_latest → {current_time}")
        
    except Exception as e:
        logger.error(f"❌ [{account['email']}] Error saving to Supabase: {e}")
        return False, 0
    
    # ===================================================
    # AUTO-CONTACT NEW OFFERS
    # ===================================================
    
    # Skip contacting on first run (when initializing listings)
    if is_first_run:
        logger.info(f"🎯 [{account['email']}] First run complete - {len(new_offers)} listings saved. Contact will start on next run.")
        return True, len(new_offers)
    
    # Get contact form from configuration (root level)
    contact_form_config = config.get('contact_form')
    
    if not contact_form_config:
        logger.warning(f"⚠️ [{account['email']}] No contact form found in configuration.contact_form. Skipping auto-contact.")
        return True, len(new_offers)
    
    # Get message from the SEPARATE 'message' field in Supabase
    contact_message = account.get('message')
    if not contact_message or not contact_message.strip():
        logger.warning(f"⚠️ [{account['email']}] No message found in 'message' field. Skipping auto-contact.")
        return True, len(new_offers)
    
    # Build complete contact form with message
    contact_form = {**contact_form_config, 'message': contact_message}
    logger.info(f"📝 [{account['email']}] Using message from separate 'message' field: {contact_message[:50]}...")
    
    # Load contacted IDs history (last 1000) to prevent duplicates
    contacted_ids_history = existing_listing_data.get('contacted_ids', [])
    if not isinstance(contacted_ids_history, list):
        contacted_ids_history = []
    
    logger.info(f"📋 [{account['email']}] Loaded {len(contacted_ids_history)} previously contacted IDs from history")
    
    # Session was already validated above before searching
    logger.info(f"💬 [{account['email']}] Auto-contacting {len(new_offers)} new offers...")
    
    # Contact each offer
    contacted_count = 0
    failed_count = 0
    skipped_count = 0
    newly_contacted_ids = []
    
    for offer in new_offers:
        offer_id = offer.get('id')
        offer_title = offer.get('title', 'Unknown')
        offer_url = offer.get('url', '')
        
        # Check if already contacted (duplicate detection)
        if offer_id in contacted_ids_history:
            skipped_count += 1
            logger.info(f"⏭️  [{account['email']}] Skipping offer {offer_id} (already contacted): {offer_title[:40]}...")
            continue
        
        logger.info(f"📤 [{account['email']}] Contacting offer {offer_id}: {offer_title[:40]}...")
        logger.info(f"   🔗 URL: {offer_url}")
        
        result = client.contact_listing(str(offer_id), contact_form)
        if result:
            contacted_count += 1
            newly_contacted_ids.append(offer_id)
            logger.info(f"   ✅ [{account['email']}] Successfully contacted offer {offer_id}")
        else:
            failed_count += 1
            logger.error(f"   ❌ [{account['email']}] Failed to contact offer {offer_id}")
        
        # Random delay between contacts (2-3 seconds)
        time.sleep(random.uniform(2, 3))
    
    # Update contacted_ids history (keep last 1000)
    if newly_contacted_ids:
        # Merge new IDs with existing history
        updated_contacted_ids = newly_contacted_ids + contacted_ids_history
        # Keep only last 1000 IDs
        updated_contacted_ids = updated_contacted_ids[:1000]
        
        # Update listing_data with new contacted_ids history
        updated_listing_data['contacted_ids'] = updated_contacted_ids
        
        try:
            supabase.table('accounts').update({
                'listing_data': updated_listing_data
            }).eq('id', account['id']).execute()
            
            logger.info(f"💾 [{account['email']}] Updated contacted_ids history: {len(updated_contacted_ids)} IDs stored")
        except Exception as e:
            logger.error(f"❌ [{account['email']}] Error updating contacted_ids history: {e}")
    
    # Update the contacted_ads counter in configuration
    if contacted_count > 0:
        try:
            current_contacted = config.get('contacted_ads', 0)
            new_total = current_contacted + contacted_count
            
            config['contacted_ads'] = new_total
            
            supabase.table('accounts').update({
                'configuration': config
            }).eq('id', account['id']).execute()
            
            logger.info(f"📈 [{account['email']}] Updated contacted_ads: {current_contacted} → {new_total}")
        except Exception as e:
            logger.error(f"❌ [{account['email']}] Error updating contacted_ads counter: {e}")
    
    if skipped_count > 0:
        logger.info(f"📊 [{account['email']}] Contact Summary: ✅ {contacted_count} | ❌ {failed_count} | ⏭️  {skipped_count} skipped (duplicates)")
    else:
        logger.info(f"📊 [{account['email']}] Contact Summary: ✅ {contacted_count} | ❌ {failed_count}")
    
    logger.info(f"✅ [{account['email']}] Scraper completed successfully!")
    
    return True, len(new_offers)
