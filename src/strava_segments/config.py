import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

STRAVA_ACCESS_TOKEN = os.environ["STRAVA_ACCESS_TOKEN"]
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.environ.get("STRAVA_REFRESH_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

SCHEMA = "strava_segments"

DEFAULT_CENTER_LAT = 50.06
DEFAULT_CENTER_LNG = 19.94
DEFAULT_RADIUS_KM = 50
TILE_SIZE_KM = 1.0
TILE_OVERLAP_KM = 0.2

RATE_LIMIT_BUFFER = 5
