"""
DataDome solver via CapSolver with per-account token caching.

Tokens are cached in datadome_tokens.json keyed by account email and reused
until a request gets blocked again (403), at which point a new token is solved.
"""
import json
import os
import threading
import time
from urllib.parse import urlparse

from curl_cffi import requests

from logger_config import setup_logger

logger = setup_logger('datadome_solver')

TOKENS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'datadome_tokens.json')


def capsolver_proxy(proxy_url: str) -> str:
    """Convert http://user:pass@host:port to CapSolver's host:port:user:pass format."""
    p = urlparse(proxy_url)
    auth = f":{p.username}:{p.password}" if p.username else ""
    return f"{p.hostname}:{p.port}{auth}"


def solve_datadome(captcha_url: str, user_agent: str, proxy_url: str) -> str:
    """Solve a DataDome challenge via CapSolver. Returns the datadome cookie value."""
    api_key = os.getenv('CAPSOLVER_API_KEY')
    if not api_key:
        raise RuntimeError("CAPSOLVER_API_KEY environment variable not set")

    task = {
        "type": "DatadomeSliderTask",
        "websiteURL": "https://www.immowelt.de",
        "captchaUrl": captcha_url,
        "userAgent": user_agent,
    }
    if proxy_url:
        task["proxy"] = capsolver_proxy(proxy_url)

    logger.info("🧩 Solving DataDome via CapSolver...")

    r = requests.post(
        "https://api.capsolver.com/createTask",
        json={"clientKey": api_key, "task": task},
        timeout=60,
    ).json()

    if r.get("errorId") != 0:
        raise RuntimeError(f"CapSolver createTask failed: {r}")

    task_id = r["taskId"]
    logger.info(f"➡ CapSolver task ID: {task_id}")

    for _ in range(60):
        time.sleep(2)
        r = requests.post(
            "https://api.capsolver.com/getTaskResult",
            json={"clientKey": api_key, "taskId": task_id},
            timeout=60,
        ).json()

        if r.get("status") == "ready":
            cookie = r["solution"]["cookie"]
            token = cookie.split(";")[0].split("=", 1)[1]
            logger.info("✅ DataDome solved")
            return token

        if r.get("errorId") != 0:
            raise RuntimeError(f"CapSolver getTaskResult failed: {r}")

    raise RuntimeError("CapSolver timed out")


def extract_captcha_url(response) -> str | None:
    """Extract the geo.captcha-delivery.com URL from a DataDome 403 response."""
    try:
        return response.json().get("url")
    except Exception:
        return None


class DatadomeTokenStore:
    """Per-account datadome token cache backed by a single JSON file. Thread-safe."""

    _lock = threading.Lock()

    def __init__(self, path: str = TOKENS_FILE):
        self.path = path

    def _read_all(self) -> dict:
        try:
            with open(self.path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def get(self, account_email: str) -> str | None:
        with self._lock:
            return self._read_all().get(account_email)

    def save(self, account_email: str, token: str):
        with self._lock:
            tokens = self._read_all()
            tokens[account_email] = token
            # Atomic write: write to temp file then replace, so concurrent
            # readers never see a partially-written file
            tmp_path = f"{self.path}.tmp"
            with open(tmp_path, "w") as f:
                json.dump(tokens, f, indent=2)
            os.replace(tmp_path, self.path)
        logger.info(f"💾 Datadome token saved for {account_email}")
