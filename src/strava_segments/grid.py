import math


def generate_tiles(
    center_lat: float,
    center_lng: float,
    radius_km: float,
    tile_size_km: float = 1.0,
    overlap_km: float = 0.2,
) -> list[tuple[float, float, float, float]]:
    """Generate grid tiles with overlap covering a circular area.

    Returns list of (south_lat, west_lng, north_lat, east_lng) tuples.
    """
    km_per_deg_lat = 111.32
    km_per_deg_lng = 111.32 * math.cos(math.radians(center_lat))

    bbox_south = center_lat - radius_km / km_per_deg_lat
    bbox_north = center_lat + radius_km / km_per_deg_lat
    bbox_west = center_lng - radius_km / km_per_deg_lng
    bbox_east = center_lng + radius_km / km_per_deg_lng

    step_lat = tile_size_km / km_per_deg_lat
    step_lng = tile_size_km / km_per_deg_lng
    overlap_lat = overlap_km / km_per_deg_lat
    overlap_lng = overlap_km / km_per_deg_lng

    tiles = []
    lat = bbox_south
    while lat < bbox_north:
        lng = bbox_west
        while lng < bbox_east:
            south = lat - overlap_lat
            north = lat + step_lat + overlap_lat
            west = lng - overlap_lng
            east = lng + step_lng + overlap_lng

            tile_center_lat = (south + north) / 2
            tile_center_lng = (west + east) / 2
            dist = _haversine(center_lat, center_lng, tile_center_lat, tile_center_lng)
            if dist <= radius_km + tile_size_km:
                tiles.append((south, west, north, east))

            lng += step_lng
        lat += step_lat

    return tiles


def _haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
