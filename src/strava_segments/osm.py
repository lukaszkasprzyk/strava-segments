import json
import logging
import time
import urllib.request
import urllib.parse
from collections import Counter

log = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
SAMPLE_RADIUS_M = 15
REQUEST_DELAY_S = 3.0
MAX_RETRIES = 3

# Ignore footways/paths/steps — they pollute results when near the actual road
_IGNORED_HIGHWAYS = {"footway", "path", "steps", "pedestrian", "corridor"}

# Surfaces suitable for road cycling
ROAD_SURFACES = {"asphalt", "concrete", "paved"}


def fetch_osm_ways(encoded_polyline: str, max_samples: int = 5) -> list[dict] | None:
    points = _decode_polyline(encoded_polyline)
    if not points:
        return None

    samples = _pick_samples(points, max_samples)

    try:
        return _query_overpass_batch(samples)
    except Exception as e:
        log.warning(f"Overpass batch query failed: {e}")
        return None


def classify_surface(ways: list[dict]) -> str | None:
    surface_counts: Counter = Counter()
    for way in ways:
        tags = way.get("tags", {})
        if tags.get("highway") in _IGNORED_HIGHWAYS:
            continue
        surface = tags.get("surface")
        if surface:
            surface_counts[surface] += 1

    if not surface_counts:
        return None

    total = sum(surface_counts.values())
    road_count = sum(v for s, v in surface_counts.items() if s in ROAD_SURFACES)
    paving_count = surface_counts.get("paving_stones", 0)

    all_road = all(s in ROAD_SURFACES for s in surface_counts)
    if all_road:
        return surface_counts.most_common(1)[0][0]

    if road_count > 0 and paving_count > 0 and paving_count / total < 0.3:
        non_road_non_paving = total - road_count - paving_count
        if non_road_non_paving == 0:
            return "mostly_asphalt"

    any_road = any(s in ROAD_SURFACES for s in surface_counts)
    if any_road:
        return "mixed"

    return surface_counts.most_common(1)[0][0]


def _query_overpass_batch(points: list[tuple[float, float]]) -> list[dict]:
    unions = "\n".join(
        f"way(around:{SAMPLE_RADIUS_M},{lat},{lng})[\"highway\"][\"surface\"];"
        for lat, lng in points
    )
    query = f"""
    [out:json][timeout:25];
    ({unions});
    out tags;
    """
    data = urllib.parse.urlencode({"data": query}).encode()
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(OVERPASS_URL, data=data)
            resp = urllib.request.urlopen(req, timeout=30)
            return json.loads(resp.read()).get("elements", [])
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = REQUEST_DELAY_S * (attempt + 1)
            log.debug(f"Overpass retry {attempt + 1}, waiting {wait:.0f}s")
            time.sleep(wait)


def _pick_samples(points: list[tuple[float, float]], max_samples: int) -> list[tuple[float, float]]:
    n = len(points)
    if n <= max_samples:
        return points
    step = (n - 1) / (max_samples - 1)
    return [points[round(i * step)] for i in range(max_samples)]


def _decode_polyline(encoded: str) -> list[tuple[float, float]]:
    points = []
    index = lat = lng = 0
    while index < len(encoded):
        for _ in range(2):
            shift = result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if result & 1 else result >> 1
            if _ == 0:
                lat += delta
            else:
                lng += delta
        points.append((lat / 1e5, lng / 1e5))
    return points
