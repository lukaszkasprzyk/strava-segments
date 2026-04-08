# strava-segments

Crawler segmentów Strava z danymi o nawierzchni z OpenStreetMap.

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

## Komendy

```bash
# Inicjalizacja bazy (tworzenie tabel, migracje)
strava-segments init-db

# Generowanie siatki tile'ów (domyślnie 50km wokół Krakowa)
strava-segments generate-grid

# Crawling segmentów ze Strava API
# --phase: explore (tylko odkrywanie), details (tylko szczegóły), both (domyślnie)
# --origin-lat/--origin-lng: zacznij od najbliższych tile'ów do tego punktu
strava-segments run
strava-segments run --origin-lat 50.026 --origin-lng 19.908

# Pobieranie nawierzchni z OpenStreetMap (niezależny proces)
strava-segments surface

# Status crawla
strava-segments status
```

## Uruchamianie w tle

```bash
nohup strava-segments run --origin-lat 50.026 --origin-lng 19.908 > /tmp/strava-crawler.log 2>&1 &
nohup strava-segments surface > /tmp/strava-surface.log 2>&1 &

# Podgląd logów
tail -f /tmp/strava-crawler.log
tail -f /tmp/strava-surface.log
```

Wszystkie procesy można bezpiecznie przerwać Ctrl+C (lub kill) i wznowić — kontynuują od miejsca w którym skończyły.
