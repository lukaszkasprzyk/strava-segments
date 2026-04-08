import signal
import sys
import logging

import typer

from . import db
from .grid import generate_tiles
from .api import StravaClient
from .osm import fetch_osm_ways, classify_surface
from .config import DEFAULT_CENTER_LAT, DEFAULT_CENTER_LNG, DEFAULT_RADIUS_KM, TILE_SIZE_KM, TILE_OVERLAP_KM

app = typer.Typer()
log = logging.getLogger(__name__)

_shutdown = False


def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    log.info("Shutdown requested, finishing current request...")


signal.signal(signal.SIGINT, _handle_signal)


@app.command()
def init_db():
    """Create tables and indexes."""
    db.init_db()
    typer.echo("Database initialized.")


@app.command()
def generate_grid(
    lat: float = typer.Option(DEFAULT_CENTER_LAT, help="Center latitude"),
    lng: float = typer.Option(DEFAULT_CENTER_LNG, help="Center longitude"),
    radius: float = typer.Option(DEFAULT_RADIUS_KM, help="Radius in km"),
):
    """Generate grid tiles for the given area."""
    tiles = generate_tiles(lat, lng, radius, TILE_SIZE_KM, TILE_OVERLAP_KM)
    with db.get_conn() as conn:
        db.insert_tiles(conn, tiles)
    typer.echo(f"Generated {len(tiles)} tiles.")


@app.command()
def run(
    phase: str = typer.Option("both", help="Phase: explore, details, or both"),
    origin_lat: float = typer.Option(DEFAULT_CENTER_LAT, help="Start exploring from this latitude"),
    origin_lng: float = typer.Option(DEFAULT_CENTER_LNG, help="Start exploring from this longitude"),
):
    """Run the crawler. Can be interrupted with Ctrl+C and resumed."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with db.get_conn() as conn:
        client = StravaClient(conn)
        try:
            if phase in ("explore", "both"):
                _run_explore(conn, client, phase, origin_lat, origin_lng)
            if phase in ("details", "both") and not _shutdown:
                _run_details(conn, client)
        finally:
            client.close()

    if _shutdown:
        typer.echo("Stopped gracefully. Run again to resume.")


def _run_explore(conn, client: StravaClient, phase: str = "explore", origin_lat: float = None, origin_lng: float = None):
    global _shutdown
    while not _shutdown:
        tiles = db.get_pending_tiles(conn, limit=50, origin_lat=origin_lat, origin_lng=origin_lng)
        if not tiles:
            log.info("All tiles explored.")
            break

        for tile in tiles:
            if _shutdown:
                break

            tile_id = tile["id"]
            log.info(f"Exploring tile {tile_id} ({tile['south_lat']:.4f},{tile['west_lng']:.4f} -> {tile['north_lat']:.4f},{tile['east_lng']:.4f})")

            try:
                segments = client.explore_segments(
                    tile["south_lat"], tile["west_lng"], tile["north_lat"], tile["east_lng"]
                )
                new_segment_ids = []
                for seg in segments:
                    db.upsert_segment_from_explore(conn, seg["id"], seg)
                    new_segment_ids.append(seg["id"])
                conn.commit()
                db.mark_tile_done(conn, tile_id, len(segments))
                log.info(f"Tile {tile_id}: {len(segments)} segments")

                if phase == "both":
                    for strava_id in new_segment_ids:
                        if _shutdown:
                            break
                        if db.segment_needs_detail(conn, strava_id):
                            log.info(f"Fetching details for segment {strava_id}")
                            try:
                                data = client.get_segment(strava_id)
                                db.update_segment_detail(conn, strava_id, data)
                                conn.commit()
                            except Exception as e:
                                conn.rollback()
                                log.error(f"Segment {strava_id} detail failed: {e}")
            except Exception as e:
                conn.rollback()
                log.error(f"Tile {tile_id} failed: {e}")
                db.mark_tile_failed(conn, tile_id, str(e))


def _run_details(conn, client: StravaClient):
    global _shutdown
    while not _shutdown:
        segments = db.get_segments_without_details(conn, limit=50)
        if not segments:
            log.info("All segment details fetched.")
            break

        for seg in segments:
            if _shutdown:
                break

            strava_id = seg["strava_id"]
            log.info(f"Fetching details for segment {strava_id}")

            try:
                data = client.get_segment(strava_id)
                db.update_segment_detail(conn, strava_id, data)
                conn.commit()
            except Exception as e:
                conn.rollback()
                log.error(f"Segment {strava_id} detail failed: {e}")


@app.command()
def surface():
    """Fetch surface type from OpenStreetMap for segments with polyline data."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with db.get_conn() as conn:
        _run_surface(conn)

    if _shutdown:
        typer.echo("Stopped gracefully. Run again to resume.")


def _run_surface(conn):
    global _shutdown
    while not _shutdown:
        segments = db.get_segments_without_surface(conn, limit=50)
        if not segments:
            log.info("All segments have surface data.")
            break

        for seg in segments:
            if _shutdown:
                break

            strava_id = seg["strava_id"]
            polyline = seg["polyline"]
            log.info(f"Fetching surface for segment {strava_id}")

            try:
                ways = fetch_osm_ways(polyline)
                if ways is not None:
                    surface_type = classify_surface(ways)
                    db.update_segment_surface(conn, strava_id, surface_type, osm_ways=ways)
                    log.info(f"Segment {strava_id}: surface={surface_type}")
                else:
                    log.warning(f"Segment {strava_id}: no OSM data")
            except Exception as e:
                log.error(f"Segment {strava_id} surface fetch failed: {e}")


@app.command()
def reclassify():
    """Recalculate surface_type from stored OSM data (no API calls)."""
    with db.get_conn() as conn:
        count = db.recalculate_surface(conn, classify_surface)
    typer.echo(f"Reclassified {count} segments.")


@app.command()
def status():
    """Show crawl progress."""
    with db.get_conn() as conn:
        s = db.get_status(conn)

    t = s["tiles"]
    seg = s["segments"]
    api = s["api"]
    sf = s["surface"]

    typer.echo(f"Tiles:    {t['tiles_done']}/{t['tiles_total']} done, {t['tiles_pending']} pending, {t['tiles_failed']} failed")
    typer.echo(f"Segments: {seg['total']} total, {seg['with_details']} with details")
    typer.echo(f"Surface:  {sf['with_surface']}/{sf['with_polyline']} fetched, {sf['surface_pending']} pending")

    usage_15 = api.get("usage_15min") or "?"
    limit_15 = api.get("limit_15min") or "?"
    usage_d = api.get("usage_daily") or "?"
    limit_d = api.get("limit_daily") or "?"
    typer.echo(f"API today: {api['today_requests']} requests, 15min: {usage_15}/{limit_15}, daily: {usage_d}/{limit_d}")
