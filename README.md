# strava-segments

Strava segment crawler with surface type detection from OpenStreetMap.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

`.env`:
```
STRAVA_ACCESS_TOKEN=...
STRAVA_CLIENT_ID=...
STRAVA_CLIENT_SECRET=...
STRAVA_REFRESH_TOKEN=...
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres
```

## Commands

```bash
# Initialize database (create tables, run migrations)
strava-segments init-db

# Generate grid tiles (default: 50km radius around Kraków)
strava-segments generate-grid

# Crawl segments from Strava API
# --phase: explore (discover only), details (fetch details only), both (default)
# --origin-lat/--origin-lng: start from tiles closest to this point
strava-segments run
strava-segments run --origin-lat 50.026 --origin-lng 19.908

# Fetch surface type from OpenStreetMap (independent process)
strava-segments surface

# Reclassify surface types from stored OSM data (no API calls)
strava-segments reclassify

# Show crawl progress
strava-segments status
```

## Running in background

```bash
nohup strava-segments run --origin-lat 50.026 --origin-lng 19.908 > /tmp/strava-crawler.log 2>&1 &
nohup strava-segments surface > /tmp/strava-surface.log 2>&1 &

# Watch logs
tail -f /tmp/strava-crawler.log
tail -f /tmp/strava-surface.log
```

All processes can be safely interrupted with Ctrl+C (or kill) and resumed — they continue from where they left off.
