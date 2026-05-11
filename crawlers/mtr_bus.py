"""MTR Bus crawler — all data comes from official CSV files.

Output schema matches the app's mtr_stops table:

routes[]:
  route_number, route_name_chi, route_name_eng,
  orig_chi, orig_eng, dest_chi, dest_eng,
  fares: { adult_octopus, child_octopus, elderly_octopus,
           joyou_octopus, disability_octopus, student_octopus,
           adult_single, child_single, elderly_single,
           joyou_single, disability_single, student_single }

stops[]:
  station_id, latitude, longitude,
  station_name_chi, station_name_eng,
  route_number, direction (U/D), sequence
"""
import csv
import io
import logging
from .utils import fetch_json

logger = logging.getLogger(__name__)

BASE = "https://opendata.mtr.com.hk/data"


async def crawl(client) -> dict:
    logger.info("MTR Bus: fetching CSVs...")

    async def fetch_csv(name: str) -> list[dict]:
        url = f"{BASE}/{name}"
        r = await client.get(url)
        r.raise_for_status()
        text = r.text
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)

    import asyncio as _asyncio
    routes_raw, stops_raw, fares_raw = await _asyncio.gather(
        fetch_csv("mtr_bus_routes.csv"),
        fetch_csv("mtr_bus_stops.csv"),
        fetch_csv("mtr_bus_fares.csv"),
    )

    # Build fares lookup: route_number -> fare dict
    # CSV columns: ROUTE_ID,FARE_OCTO_ADULT,FARE_OCTO_CHILD,...
    fares_map: dict[str, dict] = {}
    for row in fares_raw:
        rn = row.get("ROUTE_ID", "").strip()
        if not rn:
            continue
        fares_map[rn] = {
            "adult_octopus":      row.get("FARE_OCTO_ADULT", "-").strip(),
            "child_octopus":      row.get("FARE_OCTO_CHILD", "-").strip(),
            "elderly_octopus":    row.get("FARE_OCTO_ELDERLY", "-").strip(),
            "joyou_octopus":      row.get("FARE_OCTO_JOYU", "-").strip(),
            "disability_octopus": row.get("FARE_OCTO_PWD", "-").strip(),
            "student_octopus":    row.get("FARE_OCTO_STUDENT", "-").strip(),
            "adult_single":       row.get("FARE_SINGLE_ADULT", "-").strip(),
            "child_single":       row.get("FARE_SINGLE_CHILD", "-").strip(),
            "elderly_single":     row.get("FARE_SINGLE_ELDERLY", "-").strip(),
            "joyou_single":       row.get("FARE_SINGLE_JOYU", "-").strip(),
            "disability_single":  row.get("FARE_SINGLE_PWD", "-").strip(),
            "student_single":     row.get("FARE_SINGLE_STUDENT", "-").strip(),
        }

    # CSV columns: ROUTE_ID,ROUTE_NAME_CHI,ROUTE_NAME_ENG,IS_CIRCULAR,LINE_UP,LINE_DOWN,REFERENCE_ID
    # One row per route variant (e.g. 506 has 2 rows for different variants)
    # Group by REFERENCE_ID (base route number) and take distinct route numbers
    seen_routes: set[str] = set()
    routes: list[dict] = []
    for row in routes_raw:
        ref_id = row.get("REFERENCE_ID", "").strip()
        rn     = row.get("ROUTE_ID", "").strip()
        if not ref_id or ref_id in seen_routes:
            continue
        seen_routes.add(ref_id)
        routes.append({
            "route_number":   ref_id,
            "route_name_chi": row.get("ROUTE_NAME_CHI", "").strip(),
            "route_name_eng": row.get("ROUTE_NAME_ENG", "").strip(),
            "fares":          fares_map.get(ref_id, {}),
        })

    # CSV columns: ROUTE_ID,DIRECTION,STATION_SEQNO,STATION_ID,STATION_LATITUDE,
    #              STATION_LONGITUDE,STATION_NAME_CHI,STATION_NAME_ENG,REFERENCE_ID
    # Direction: O=outbound, I=inbound
    all_stops: list[dict] = []
    for row in stops_raw:
        rn  = row.get("REFERENCE_ID", "").strip() or row.get("ROUTE_ID", "").strip()
        sid = row.get("STATION_ID", "").strip()
        if not rn or not sid:
            continue
        direction = row.get("DIRECTION", "O").strip()  # O / I

        lat_str = row.get("STATION_LATITUDE", "").strip()
        lng_str = row.get("STATION_LONGITUDE", "").strip()
        seq_str = row.get("STATION_SEQNO", "0").strip()

        stop = {
            "station_id":       sid,
            "latitude":         float(lat_str) if lat_str else None,
            "longitude":        float(lng_str) if lng_str else None,
            "station_name_chi": row.get("STATION_NAME_CHI", "").strip(),
            "station_name_eng": row.get("STATION_NAME_ENG", "").strip(),
            "route_number":     rn,
            "direction":        direction,
            "sequence":         int(seq_str) if seq_str.isdigit() else 0,
        }
        all_stops.append(stop)

    all_stops.sort(key=lambda s: (s["route_number"], s["direction"], s["sequence"]))

    logger.info(f"MTR Bus: {len(routes)} routes, {len(all_stops)} stops")
    return {"routes": routes, "stops": all_stops}
