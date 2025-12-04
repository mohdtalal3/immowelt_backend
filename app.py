import os
import time
import threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, send_file
from dotenv import load_dotenv
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor, as_completed
from immowelt_scraper import run_scraper_for_account
from logger_config import setup_logger, LOGS_DIR, LOG_FILE

# Setup logger
logger = setup_logger('app')

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Configuration
SCRAPER_INTERVAL = 2  # minutes - how often to scrape per account
QUEUE_CHECK_INTERVAL = 2  # minutes - how often to check for accounts ready to scrape
MAX_CONCURRENT_SCRAPERS = 10  # max number of accounts to scrape concurrently

app = Flask(__name__)

# Initialize global Supabase client (thread-safe, reusable)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Global state
scraper_stats = {
    'total_runs': 0,
    'successful_runs': 0,
    'failed_runs': 0,
    'total_new_offers': 0,
    'last_check': None,
    'currently_running': 0,
    'accounts_processed': []
}


def get_accounts_ready_to_scrape(supabase: Client):
    """
    Fetch accounts from Supabase that are:
    1. Website = 'immowelt'
    2. Scraping is enabled (scrape_enabled = true in configuration)
    3. Haven't been updated in the last SCRAPER_INTERVAL minutes
    
    Returns list of account dictionaries.
    """
    try:
        # Fetch all immowelt accounts
        response = supabase.table('accounts').select('*').eq('website', 'immowelt').execute()
        
        if not response.data:
            return []
        
        ready_accounts = []
        now = datetime.now()
        
        for account in response.data:
            # Check if scraping is enabled in configuration
            config = account.get('configuration', {})
            scrape_enabled = config.get('scrape_enabled', False)
            
            if not scrape_enabled:
                logger.info(f"⏸️ Account {account['email']} has scraping disabled")
                continue
            
            # Check if account has been updated recently
            last_updated_str = account.get('last_updated_at')
            
            if not last_updated_str:
                # Never updated - ready to scrape
                ready_accounts.append(account)
                continue
            
            try:
                last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
                # Remove timezone info for comparison
                if last_updated.tzinfo is not None:
                    last_updated = last_updated.replace(tzinfo=None)
                
                time_since_update = (now - last_updated).total_seconds() / 60  # minutes
                
                if time_since_update >= SCRAPER_INTERVAL:
                    ready_accounts.append(account)
                    logger.info(f"✅ Account {account['email']} ready (last updated {time_since_update:.1f} min ago)")
                else:
                    logger.info(f"⏳ Account {account['email']} not ready (last updated {time_since_update:.1f} min ago, needs {SCRAPER_INTERVAL - time_since_update:.1f} more min)")
            
            except Exception as e:
                logger.warning(f"⚠️ Error parsing timestamp for {account['email']}: {e}")
                # If can't parse, consider it ready
                ready_accounts.append(account)
        
        return ready_accounts
    
    except Exception as e:
        logger.error(f"❌ Error fetching accounts from Supabase: {e}")
        return []


def process_account(account: dict):
    """
    Process a single account - run scraper and update stats.
    Uses global supabase client (thread-safe).
    
    Returns: (account_email, success, new_offers_count)
    """
    try:
        success, new_offers_count = run_scraper_for_account(account, supabase)
        return (account['email'], success, new_offers_count)
    except Exception as e:
        logger.error(f"❌ Error processing account {account['email']}: {e}")
        return (account['email'], False, 0)


def scraper_queue_thread():
    """
    Background thread that checks for accounts ready to scrape every QUEUE_CHECK_INTERVAL minutes.
    Processes up to MAX_CONCURRENT_SCRAPERS accounts concurrently.
    Uses global supabase client (thread-safe).
    """
    logger.info(f"🚀 Scraper queue thread started!")
    logger.info(f"   - Checking every {QUEUE_CHECK_INTERVAL} minutes")
    logger.info(f"   - Scraping accounts every {SCRAPER_INTERVAL} minutes")
    logger.info(f"   - Max concurrent scrapers: {MAX_CONCURRENT_SCRAPERS}")
    
    while True:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 Checking for accounts ready to scrape...")
            logger.info(f"{'='*60}")
            
            scraper_stats['last_check'] = datetime.now().isoformat()
            
            ready_accounts = get_accounts_ready_to_scrape(supabase)
            
            if not ready_accounts:
                logger.info("✅ No accounts ready to scrape at this time.")
            else:
                logger.info(f"📋 Found {len(ready_accounts)} accounts ready to scrape")
                
                # Process accounts concurrently using ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCRAPERS) as executor:
                    # Submit all tasks
                    future_to_account = {
                        executor.submit(process_account, account): account 
                        for account in ready_accounts
                    }
                    
                    scraper_stats['currently_running'] = len(future_to_account)
                    
                    # Process completed tasks
                    for future in as_completed(future_to_account):
                        account = future_to_account[future]
                        try:
                            email, success, new_offers_count = future.result()
                            
                            scraper_stats['total_runs'] += 1
                            
                            if success:
                                scraper_stats['successful_runs'] += 1
                                scraper_stats['total_new_offers'] += new_offers_count
                                scraper_stats['accounts_processed'].append({
                                    'email': email,
                                    'timestamp': datetime.now().isoformat(),
                                    'new_offers': new_offers_count,
                                    'status': 'success'
                                })
                                logger.info(f"✅ Account {email} processed successfully ({new_offers_count} new offers)")
                            else:
                                scraper_stats['failed_runs'] += 1
                                scraper_stats['accounts_processed'].append({
                                    'email': email,
                                    'timestamp': datetime.now().isoformat(),
                                    'new_offers': 0,
                                    'status': 'failed'
                                })
                                logger.error(f"❌ Account {email} processing failed")
                        
                        except Exception as e:
                            scraper_stats['failed_runs'] += 1
                            logger.error(f"❌ Exception processing account {account['email']}: {e}")
                    
                    scraper_stats['currently_running'] = 0
                
                # Keep only last 100 processed accounts in memory
                if len(scraper_stats['accounts_processed']) > 100:
                    scraper_stats['accounts_processed'] = scraper_stats['accounts_processed'][-100:]
                
                logger.info(f"\n📊 Batch Summary:")
                logger.info(f"   Total runs: {scraper_stats['total_runs']}")
                logger.info(f"   Successful: {scraper_stats['successful_runs']}")
                logger.info(f"   Failed: {scraper_stats['failed_runs']}")
                logger.info(f"   Total new offers found: {scraper_stats['total_new_offers']}")
        
        except Exception as e:
            logger.error(f"❌ Error in scraper queue thread: {e}")
        
        # Wait for next check interval
        logger.info(f"\n⏳ Waiting {QUEUE_CHECK_INTERVAL} minutes until next check...")
        time.sleep(QUEUE_CHECK_INTERVAL * 60)


# ===================================================
# FLASK ROUTES
# ===================================================

@app.route('/')
def index():
    """Health check endpoint."""
    return jsonify({
        'status': 'running',
        'service': 'Immowelt Scraper Backend',
        'version': '1.0.0'
    })


@app.route('/stats')
def stats():
    """Get scraper statistics."""
    return jsonify({
        'stats': scraper_stats,
        'config': {
            'scraper_interval_minutes': SCRAPER_INTERVAL,
            'queue_check_interval_minutes': QUEUE_CHECK_INTERVAL,
            'max_concurrent_scrapers': MAX_CONCURRENT_SCRAPERS
        }
    })


@app.route('/accounts')
def accounts():
    """Get all immowelt accounts from Supabase."""
    try:
        response = supabase.table('accounts').select('id, email, website, last_updated_at, configuration').eq('website', 'immowelt').execute()
        
        return jsonify({
            'success': True,
            'count': len(response.data),
            'accounts': response.data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/accounts/ready')
def accounts_ready():
    """Get accounts that are ready to be scraped."""
    try:
        ready_accounts = get_accounts_ready_to_scrape(supabase)
        
        return jsonify({
            'success': True,
            'count': len(ready_accounts),
            'accounts': [
                {
                    'id': acc['id'],
                    'email': acc['email'],
                    'last_updated_at': acc.get('last_updated_at'),
                    'scrape_enabled': acc.get('configuration', {}).get('scrape_enabled', False)
                }
                for acc in ready_accounts
            ]
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/scrape/trigger', methods=['POST'])
def trigger_scrape():
    """Manually trigger a scrape check (useful for testing)."""
    try:
        ready_accounts = get_accounts_ready_to_scrape(supabase)
        
        if not ready_accounts:
            return jsonify({
                'success': True,
                'message': 'No accounts ready to scrape',
                'count': 0
            })
        
        # Process accounts in a separate thread to not block the request
        def async_scrape():
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCRAPERS) as executor:
                futures = [executor.submit(process_account, account) for account in ready_accounts]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error in async scrape: {e}")
        
        thread = threading.Thread(target=async_scrape)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Triggered scraping for {len(ready_accounts)} accounts',
            'count': len(ready_accounts)
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/logs')
def list_logs():
    """List all available log files."""
    try:
        if not os.path.exists(LOGS_DIR):
            return jsonify({
                'success': True,
                'logs': [],
                'message': 'No logs directory found yet'
            })
        
        log_files = []
        for filename in sorted(os.listdir(LOGS_DIR), reverse=True):
            if filename.startswith('scraper.log'):
                filepath = os.path.join(LOGS_DIR, filename)
                file_stats = os.stat(filepath)
                log_files.append({
                    'filename': filename,
                    'size_bytes': file_stats.st_size,
                    'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                    'modified': datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                    'download_url': f'/logs/download/{filename}'
                })
        
        return jsonify({
            'success': True,
            'count': len(log_files),
            'logs': log_files
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/logs/download')
@app.route('/logs/download/<filename>')
def download_log(filename=None):
    """Download a log file. Default: current log file."""
    try:
        if not os.path.exists(LOGS_DIR):
            return jsonify({
                'success': False,
                'error': 'No logs directory found'
            }), 404
        
        # If no filename provided, download the current log
        if filename is None:
            filename = 'scraper.log'
        
        # Security: Only allow downloading scraper.log files
        if not filename.startswith('scraper.log'):
            return jsonify({
                'success': False,
                'error': 'Invalid log file name'
            }), 400
        
        filepath = os.path.join(LOGS_DIR, filename)
        
        if not os.path.exists(filepath):
            return jsonify({
                'success': False,
                'error': f'Log file {filename} not found'
            }), 404
        
        return send_file(
            filepath,
            as_attachment=True,
            download_name=filename,
            mimetype='text/plain'
        )
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ===================================================
# MAIN
# ===================================================

if __name__ == '__main__':
    # Start background scraper thread
    scraper_thread = threading.Thread(target=scraper_queue_thread, daemon=True)
    scraper_thread.start()
    
    logger.info(f"\n{'='*60}")
    logger.info("🚀 IMMOWELT SCRAPER BACKEND STARTED")
    logger.info(f"{'='*60}")
    logger.info(f"🌐 Flask server starting on http://0.0.0.0:5001")
    logger.info(f"📋 Available endpoints:")
    logger.info(f"   GET  /                    - Health check")
    logger.info(f"   GET  /stats               - Scraper statistics")
    logger.info(f"   GET  /accounts            - List all accounts")
    logger.info(f"   GET  /accounts/ready      - List accounts ready to scrape")
    logger.info(f"   POST /scrape/trigger      - Manually trigger scrape")
    logger.info(f"   GET  /logs                - List all log files")
    logger.info(f"   GET  /logs/download       - Download current log file")
    logger.info(f"   GET  /logs/download/<file> - Download specific log file")
    logger.info(f"{'='*60}")
    logger.info(f"📥 Quick Download: http://localhost:5001/logs/download")
    logger.info(f"{'='*60}\n")
    
    # Start Flask app on port 5001 (different from immo24 which uses 5002/5000)
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)
