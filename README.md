# Immowelt Scraper Backend

Automated scraper for Immowelt real estate listings with session management, scheduled scraping, and auto-contact functionality.

## Features

- **🔐 Session Management**: Automatic token refresh and session validation
- **⏰ Scheduled Scraping**: Configurable intervals for checking new listings
- **📧 Auto-Contact**: Automatically contact new listings with customizable messages
- **🔄 Duplicate Prevention**: Tracks contacted listings to avoid duplicates
- **📊 Statistics Dashboard**: Monitor scraping performance via REST API
- **📝 Logging**: Rotating daily logs with 3-day retention
- **🌐 Proxy Support**: Per-account proxy configuration

## Architecture

```
imowelt_backend/
├── app.py                  # Flask backend with scheduling
├── immowelt_scraper.py     # Core scraper logic
├── logger_config.py        # Logging configuration
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (not in repo)
├── .env.example            # Environment variables template
└── logs/                   # Auto-generated log files
```

## Installation

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your Supabase credentials
```

3. **Run the backend**:
```bash
python app.py
```

The server will start on `http://0.0.0.0:5001`

## Configuration

### Supabase Database Schema

The scraper expects the following structure in your Supabase `accounts` table:

```sql
- id (UUID)
- website (TEXT) = 'immowelt'
- email (TEXT)
- password (TEXT)
- session_details (JSONB) - See session structure below
- configuration (JSONB) - See configuration structure below
- listing_data (JSONB) - See listing data structure below
- message (TEXT) - Contact message template
- last_updated_at (TIMESTAMPTZ)
```

### Session Details Structure

Session is created from frontend and stored in `session_details`:

```json
{
  "did": "s%3Av0%3A...",
  "auth0": "s%3A...",
  "did_compat": "s%3Av0%3A...",
  "auth0_compat": "s%3A...",
  "oauth.access.token": "eyJhbGci...",
  "oauth.access.expiration": "1764764992",
  "session_created_at": "2025-12-03T10:30:00"
}
```

### Configuration Structure

Stored in `configuration` field:

```json
{
  "scrape_enabled": true,
  "contacted_ads": 0,
  "proxy_port": "10000",
  "criteria": {
    "distributionTypes": ["Rent"],
    "estateTypes": ["House"],
    "projectTypes": ["New_Build", "Stock"],
    "numberOfRoomsMin": 1,
    "numberOfRoomsMax": 15,
    "priceMin": 20,
    "priceMax": 2000000,
    "spaceMin": 10,
    "classifiedBusiness": "Professional",
    "location": {
      "placeIds": ["AD02DE1"]
    },
    "contactForm": {
      "salutation": "mr",
      "firstName": "John",
      "lastName": "Doe",
      "email": "john@example.com",
      "phoneNumber": "",
      "householdType": "1",
      "workStatus": "6",
      "netMonthlyIncome": "5",
      "preferredMoveInDate": "4"
    }
  },
  "paging": {
    "page": 1,
    "size": 50,
    "order": "DateDesc"
  }
}
```

### Configuration Parameters

**Criteria Filters:**
- `distributionTypes`: ["Rent", "Buy"]
- `estateTypes`: ["House", "Apartment", "Studio", etc.]
- `projectTypes`: ["New_Build", "Stock"]
- `numberOfRoomsMin/Max`: 1-15
- `priceMin/Max`: Price range in EUR
- `spaceMin`: Minimum living space in m²
- `classifiedBusiness`: "Professional" or "Private"
- `location.placeIds`: Location IDs from Immowelt

**Contact Form:**
- `salutation`: "mr" or "ms"
- `firstName`, `lastName`, `email`: Contact details
- `householdType`: "1" = Single person, "2" = Couple, etc.
- `workStatus`: "1" = Employed, "6" = Student, etc.
- `netMonthlyIncome`: "1" = <1000€, "5" = 4000-5000€, etc.
- `preferredMoveInDate`: "1" = Immediately, "4" = >3 months, etc.

**System Settings:**
- `scrape_enabled`: Enable/disable scraping for this account
- `contacted_ads`: Counter for total contacted listings
- `proxy_port`: Port number for proxy (appended to PROXY_URL)

### Listing Data Structure

Automatically managed by scraper in `listing_data` field:

```json
{
  "last_latest": "03.12.2025, 10:30:00",
  "offers": [
    {
      "id": "2abc123",
      "url": "https://www.immowelt.de/expose/2abc123",
      "title": "Beautiful apartment",
      "published": "2025-12-03T10:30:00"
    }
  ],
  "contacted_ids": ["2abc123", "2def456"]
}
```

- **last_latest**: Timestamp of last scrape run
- **offers**: Last 50 listings (newest first)
- **contacted_ids**: Last 50 contacted listing IDs (for duplicate prevention)

## API Endpoints

### Health Check
```http
GET /
```
Returns backend status and version.

### Statistics
```http
GET /stats
```
Returns scraper statistics:
- Total runs
- Successful/failed runs
- Total new offers found
- Currently running scrapers
- Last 100 processed accounts

### List Accounts
```http
GET /accounts
```
Returns all Immowelt accounts with basic info.

### Ready Accounts
```http
GET /accounts/ready
```
Returns accounts ready to be scraped (enabled + past interval time).

### Trigger Scrape
```http
POST /scrape/trigger
```
Manually trigger scraping for all ready accounts.

### List Logs
```http
GET /logs
```
Returns list of available log files with metadata.

### Download Log
```http
GET /logs/download
GET /logs/download/<filename>
```
Download current or specific log file.

## How It Works

### 1. Session Management
- Sessions are created from frontend and stored in database
- Backend validates session age (auto-refreshes if >50 minutes old)
- Tokens are valid for 60 minutes
- Failed refresh automatically disables account

### 2. Scraping Process
- Background thread checks every 2 minutes for ready accounts
- Accounts are scraped every 5 minutes (configurable)
- Multiple accounts processed concurrently (max 10)

### 3. Listing Detection
- Fetches listings based on configuration filters
- Compares with previous `offers` list to find new IDs
- Saves only **new** listings (no timestamp comparison)
- Keeps last 50 offers in database

### 4. Auto-Contact
- Automatically contacts new listings if `contactForm` is configured
- Uses message from separate `message` field
- Tracks contacted IDs to prevent duplicates (last 50)
- Updates `contacted_ads` counter
- Random 1-2 second delay between contacts

### 5. Logging
- Daily rotating logs (midnight rotation)
- Keeps 3 days of history
- Logs to both file and console
- Downloadable via API

## Scraper Configuration

Adjust intervals in `app.py`:

```python
SCRAPER_INTERVAL = 5  # minutes - how often to scrape per account
QUEUE_CHECK_INTERVAL = 2  # minutes - how often to check for ready accounts
MAX_CONCURRENT_SCRAPERS = 10  # max concurrent scrapers
```

## Proxy Configuration

Per-account proxy support:

1. Set `PROXY_URL` in `.env` (without port):
```
PROXY_URL=http://username:password@proxy.domain.com:
```

2. Add `proxy_port` to account configuration:
```json
{
  "proxy_port": "10000"
}
```

Final proxy URL: `http://username:password@proxy.domain.com:10000`

## Troubleshooting

### Session Expired Errors
- Sessions expire after 60 minutes
- Backend auto-refreshes at 50 minutes
- If refresh fails, account is auto-disabled
- Re-login from frontend required

### No New Listings Found
- Check if `scrape_enabled` is `true`
- Verify configuration filters are correct
- Check logs for API errors

### Contact Failed
- Ensure `contactForm` is in `configuration.criteria`
- Verify `message` field is set
- Check if listing already contacted
- Review logs for API errors

### Account Auto-Disabled
- Token refresh failed
- Re-login from frontend
- Set `scrape_enabled` back to `true`

## License

MIT License - See LICENSE file for details
