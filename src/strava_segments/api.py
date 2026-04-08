import time
import math
import logging
from datetime import datetime, timedelta, timezone

import httpx

from .config import STRAVA_ACCESS_TOKEN, STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN, RATE_LIMIT_BUFFER
from . import db

log = logging.getLogger(__name__)

BASE_URL = "https://www.strava.com/api/v3"


class StravaClient:
    def __init__(self, conn):
        self.conn = conn
        self.access_token = STRAVA_ACCESS_TOKEN
        self.client = httpx.Client(
            base_url=BASE_URL,
            headers={"Authorization": f"Bearer {self.access_token}"},
            timeout=30.0,
        )

    def close(self):
        self.client.close()

    def explore_segments(self, south: float, west: float, north: float, east: float) -> list[dict]:
        self._wait_if_needed()
        bounds = f"{south},{west},{north},{east}"
        resp = self.client.get("/segments/explore", params={"bounds": bounds, "activity_type": "riding"})
        self._log_request("/segments/explore", resp)
        if resp.status_code == 401:
            self._handle_401()
            return self.explore_segments(south, west, north, east)
        if resp.status_code == 429:
            self._handle_429(resp)
            return self.explore_segments(south, west, north, east)
        resp.raise_for_status()
        return resp.json().get("segments", [])

    def get_segment(self, strava_id: int) -> dict:
        self._wait_if_needed()
        resp = self.client.get(f"/segments/{strava_id}")
        self._log_request(f"/segments/{strava_id}", resp)
        if resp.status_code == 401:
            self._handle_401()
            return self.get_segment(strava_id)
        if resp.status_code == 429:
            self._handle_429(resp)
            return self.get_segment(strava_id)
        resp.raise_for_status()
        return resp.json()

    def _handle_401(self):
        if not all([STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN]):
            raise RuntimeError("Got 401 and no refresh token configured")
        log.info("Access token expired, refreshing...")
        resp = httpx.post("https://www.strava.com/oauth/token", data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "refresh_token": STRAVA_REFRESH_TOKEN,
            "grant_type": "refresh_token",
        })
        resp.raise_for_status()
        self.access_token = resp.json()["access_token"]
        self.client.headers["Authorization"] = f"Bearer {self.access_token}"
        log.info("Token refreshed")

    def _handle_429(self, resp: httpx.Response):
        usage = resp.headers.get("X-RateLimit-Usage", "?")
        limit = resp.headers.get("X-RateLimit-Limit", "?")
        wait = _seconds_to_next_15min_window()
        log.warning(f"Got 429, usage={usage}, limit={limit}, waiting {wait:.0f}s until next 15-min window")
        time.sleep(wait)

    def _log_request(self, endpoint: str, resp: httpx.Response):
        db.log_api_request(self.conn, endpoint, resp.status_code, dict(resp.headers))

    def _wait_if_needed(self):
        latest = db.get_latest_rate_limit(self.conn)
        if not latest:
            return

        usage_15 = latest.get("ratelimit_usage_15min") or 0
        limit_15 = latest.get("ratelimit_limit_15min") or 100
        usage_daily = latest.get("ratelimit_usage_daily") or 0
        limit_daily = latest.get("ratelimit_limit_daily") or 1000

        if usage_15 >= limit_15 - RATE_LIMIT_BUFFER:
            wait = _seconds_to_next_15min_window()
            log.warning(f"15-min rate limit approaching ({usage_15}/{limit_15}), waiting {wait}s")
            time.sleep(wait)
        elif usage_daily >= limit_daily - RATE_LIMIT_BUFFER:
            now = datetime.now(timezone.utc)
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            wait = ((midnight + timedelta(days=1)) - now).total_seconds()
            log.warning(f"Daily rate limit approaching ({usage_daily}/{limit_daily}), waiting {wait:.0f}s")
            time.sleep(wait)


def _seconds_to_next_15min_window() -> float:
    now = datetime.now(timezone.utc)
    current_minute = now.minute
    next_window_minute = (math.ceil((current_minute + 1) / 15) * 15) % 60
    if next_window_minute <= current_minute:
        target = now.replace(minute=next_window_minute, second=0, microsecond=0)
        target += timedelta(hours=1)
    else:
        target = now.replace(minute=next_window_minute, second=0, microsecond=0)
    return max((target - now).total_seconds(), 1.0)
