import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime, timezone

from .config import DATABASE_URL, SCHEMA

DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.tiles (
    id              SERIAL PRIMARY KEY,
    south_lat       DOUBLE PRECISION NOT NULL,
    west_lng        DOUBLE PRECISION NOT NULL,
    north_lat       DOUBLE PRECISION NOT NULL,
    east_lng        DOUBLE PRECISION NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    fetched_at      TIMESTAMPTZ,
    segments_found  INTEGER,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_tiles_status ON {SCHEMA}.tiles (status);

CREATE TABLE IF NOT EXISTS {SCHEMA}.segments (
    id                   SERIAL PRIMARY KEY,
    strava_id            BIGINT NOT NULL UNIQUE,
    name                 TEXT,
    activity_type        TEXT,
    distance             DOUBLE PRECISION,
    avg_grade            DOUBLE PRECISION,
    max_grade            DOUBLE PRECISION,
    elev_difference      DOUBLE PRECISION,
    climb_category       INTEGER,
    start_lat            DOUBLE PRECISION,
    start_lng            DOUBLE PRECISION,
    end_lat              DOUBLE PRECISION,
    end_lng              DOUBLE PRECISION,
    city                 TEXT,
    state                TEXT,
    country              TEXT,
    private              BOOLEAN,
    starred              BOOLEAN,
    effort_count         INTEGER,
    athlete_count        INTEGER,
    star_count           INTEGER,
    total_elevation_gain DOUBLE PRECISION,
    elevation_high       DOUBLE PRECISION,
    elevation_low        DOUBLE PRECISION,
    polyline             TEXT,
    fetched_at           TIMESTAMPTZ NOT NULL,
    detail_fetched_at    TIMESTAMPTZ,
    raw_explore          JSONB,
    raw_detail           JSONB
);

CREATE INDEX IF NOT EXISTS idx_segments_strava_id ON {SCHEMA}.segments (strava_id);
CREATE INDEX IF NOT EXISTS idx_segments_detail ON {SCHEMA}.segments (detail_fetched_at) WHERE detail_fetched_at IS NULL;

CREATE TABLE IF NOT EXISTS {SCHEMA}.api_requests_log (
    id                    SERIAL PRIMARY KEY,
    endpoint              TEXT NOT NULL,
    status_code           INTEGER,
    ratelimit_usage_15min INTEGER,
    ratelimit_usage_daily INTEGER,
    ratelimit_limit_15min INTEGER,
    ratelimit_limit_daily INTEGER,
    requested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_requests_log_at ON {SCHEMA}.api_requests_log (requested_at);

ALTER TABLE {SCHEMA}.segments ADD COLUMN IF NOT EXISTS surface_type TEXT;
ALTER TABLE {SCHEMA}.segments ADD COLUMN IF NOT EXISTS surface_fetched_at TIMESTAMPTZ;
ALTER TABLE {SCHEMA}.segments ADD COLUMN IF NOT EXISTS osm_ways JSONB;
CREATE INDEX IF NOT EXISTS idx_segments_surface ON {SCHEMA}.segments (surface_type) WHERE surface_type IS NULL AND polyline IS NOT NULL;
"""


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()


def insert_tiles(conn, tiles: list[tuple[float, float, float, float]]):
    with conn.cursor() as cur:
        args = ",".join(
            cur.mogrify("(%s,%s,%s,%s)", t).decode() for t in tiles
        )
        cur.execute(
            f"INSERT INTO {SCHEMA}.tiles (south_lat, west_lng, north_lat, east_lng) VALUES {args}"
        )
    conn.commit()


def get_pending_tiles(conn, limit: int = 100, origin_lat: float = None, origin_lng: float = None) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if origin_lat is not None and origin_lng is not None:
            cur.execute(
                f"""SELECT * FROM {SCHEMA}.tiles WHERE status = 'pending'
                    ORDER BY pow(south_lat + (north_lat - south_lat)/2 - %s, 2)
                           + pow(west_lng + (east_lng - west_lng)/2 - %s, 2)
                    LIMIT %s""",
                (origin_lat, origin_lng, limit),
            )
        else:
            cur.execute(
                f"SELECT * FROM {SCHEMA}.tiles WHERE status = 'pending' ORDER BY id LIMIT %s",
                (limit,),
            )
        return cur.fetchall()


def mark_tile_done(conn, tile_id: int, segments_found: int):
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.tiles SET status = 'done', fetched_at = NOW(), segments_found = %s WHERE id = %s",
            (segments_found, tile_id),
        )
    conn.commit()


def mark_tile_failed(conn, tile_id: int, error: str):
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.tiles SET status = 'failed', error_message = %s WHERE id = %s",
            (error, tile_id),
        )
    conn.commit()


def upsert_segment_from_explore(conn, strava_id: int, data: dict):
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.segments (
                strava_id, name, activity_type, distance, avg_grade, max_grade,
                elev_difference, climb_category, start_lat, start_lng, end_lat, end_lng,
                fetched_at, raw_explore
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (strava_id) DO UPDATE SET
                name = EXCLUDED.name,
                fetched_at = EXCLUDED.fetched_at,
                raw_explore = EXCLUDED.raw_explore
            """,
            (
                strava_id,
                data.get("name"),
                data.get("activity_type"),
                data.get("distance"),
                data.get("avg_grade"),
                data.get("max_grade"),
                data.get("elev_difference"),
                data.get("climb_category"),
                data.get("start_latlng", [None, None])[0],
                data.get("start_latlng", [None, None])[1],
                data.get("end_latlng", [None, None])[0],
                data.get("end_latlng", [None, None])[1],
                now,
                psycopg2.extras.Json(data),
            ),
        )


def update_segment_detail(conn, strava_id: int, data: dict):
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {SCHEMA}.segments SET
                city = %s,
                state = %s,
                country = %s,
                private = %s,
                starred = %s,
                effort_count = %s,
                athlete_count = %s,
                star_count = %s,
                total_elevation_gain = %s,
                elevation_high = %s,
                elevation_low = %s,
                polyline = %s,
                detail_fetched_at = %s,
                raw_detail = %s
            WHERE strava_id = %s
            """,
            (
                data.get("city"),
                data.get("state"),
                data.get("country"),
                data.get("private"),
                data.get("starred"),
                data.get("effort_count"),
                data.get("athlete_count"),
                data.get("star_count"),
                data.get("total_elevation_gain"),
                data.get("elevation_high"),
                data.get("elevation_low"),
                data.get("map", {}).get("polyline"),
                now,
                psycopg2.extras.Json(data),
                strava_id,
            ),
        )


def segment_needs_detail(conn, strava_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT detail_fetched_at FROM {SCHEMA}.segments WHERE strava_id = %s",
            (strava_id,),
        )
        row = cur.fetchone()
        return row is not None and row[0] is None


def get_segments_without_details(conn, limit: int = 100) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT strava_id FROM {SCHEMA}.segments WHERE detail_fetched_at IS NULL ORDER BY id LIMIT %s",
            (limit,),
        )
        return cur.fetchall()


def log_api_request(conn, endpoint: str, status_code: int, headers: dict):
    usage_15, usage_daily = _parse_ratelimit(headers.get("x-ratelimit-usage", "") or headers.get("X-RateLimit-Usage", ""))
    limit_15, limit_daily = _parse_ratelimit(headers.get("x-ratelimit-limit", "") or headers.get("X-RateLimit-Limit", ""))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.api_requests_log
                (endpoint, status_code, ratelimit_usage_15min, ratelimit_usage_daily,
                 ratelimit_limit_15min, ratelimit_limit_daily)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (endpoint, status_code, usage_15, usage_daily, limit_15, limit_daily),
        )
    conn.commit()


def get_latest_rate_limit(conn) -> dict | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT * FROM {SCHEMA}.api_requests_log ORDER BY id DESC LIMIT 1"
        )
        return cur.fetchone()


def get_status(conn) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE status = 'done') AS tiles_done,
                COUNT(*) FILTER (WHERE status = 'pending') AS tiles_pending,
                COUNT(*) FILTER (WHERE status = 'failed') AS tiles_failed,
                COUNT(*) AS tiles_total
            FROM {SCHEMA}.tiles
        """)
        tiles = cur.fetchone()

        cur.execute(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE detail_fetched_at IS NOT NULL) AS with_details
            FROM {SCHEMA}.segments
        """)
        segments = cur.fetchone()

        cur.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE polyline IS NOT NULL) AS with_polyline,
                COUNT(*) FILTER (WHERE surface_type IS NOT NULL) AS with_surface,
                COUNT(*) FILTER (WHERE polyline IS NOT NULL AND surface_type IS NULL) AS surface_pending
            FROM {SCHEMA}.segments
        """)
        surface = cur.fetchone()

        cur.execute(f"""
            SELECT
                COUNT(*) AS today_requests,
                MAX(ratelimit_usage_15min) AS usage_15min,
                MAX(ratelimit_limit_15min) AS limit_15min,
                MAX(ratelimit_usage_daily) AS usage_daily,
                MAX(ratelimit_limit_daily) AS limit_daily
            FROM {SCHEMA}.api_requests_log
            WHERE requested_at >= CURRENT_DATE
        """)
        api = cur.fetchone()

    return {"tiles": tiles, "segments": segments, "api": api, "surface": surface}


def get_segments_without_surface(conn, limit: int = 100) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""SELECT strava_id, polyline FROM {SCHEMA}.segments
                WHERE surface_type IS NULL AND polyline IS NOT NULL
                ORDER BY id LIMIT %s""",
            (limit,),
        )
        return cur.fetchall()


def update_segment_surface(conn, strava_id: int, surface_type: str | None, osm_ways: list[dict] | None = None):
    if not surface_type:
        return
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.segments SET surface_type = %s, surface_fetched_at = %s, osm_ways = %s WHERE strava_id = %s",
            (surface_type, now, psycopg2.extras.Json(osm_ways), strava_id),
        )
    conn.commit()


def recalculate_surface(conn, classify_fn):
    """Recalculate surface_type from stored osm_ways without hitting Overpass."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT strava_id, osm_ways FROM {SCHEMA}.segments WHERE osm_ways IS NOT NULL"
        )
        rows = cur.fetchall()
    count = 0
    for row in rows:
        new_type = classify_fn(row["osm_ways"])
        cur2 = conn.cursor()
        cur2.execute(
            f"UPDATE {SCHEMA}.segments SET surface_type = %s WHERE strava_id = %s",
            (new_type, row["strava_id"]),
        )
        count += 1
    conn.commit()
    return count


def _parse_ratelimit(value: str) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    parts = value.split(",")
    if len(parts) == 2:
        return int(parts[0]), int(parts[1])
    return None, None
