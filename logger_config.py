"""
Logging configuration for Immowelt Scraper Backend
- Rotates logs daily at midnight
- Keeps only 3 days of logs (automatically deletes older files)
- Logs to both file and console
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

# Create logs directory if it doesn't exist
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

# Log filename with date
LOG_FILE = os.path.join(LOGS_DIR, 'scraper.log')


def setup_logger(name: str = 'immowelt_scraper', level=logging.INFO):
    """
    Setup logger with file and console handlers.
    
    Args:
        name: Logger name
        level: Logging level (default: INFO)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Prevent duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    # Format for log messages
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler - rotates daily, keeps 3 days of logs
    file_handler = TimedRotatingFileHandler(
        LOG_FILE,
        when='midnight',       # Rotate at midnight
        interval=1,            # Every 1 day
        backupCount=3,         # Keep only 3 days of logs
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    file_handler.suffix = "%Y-%m-%d"  # Add date suffix to rotated files
    
    # Console handler - also show logs in terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# Create default logger instance
logger = setup_logger()
