"""KMB / LWB crawler.

Output schema matches the app's KMBRouteData / kmb_stops / kmb_route_stops models:

routes[]:
  route, bound (O/I), service_type, orig_en/tc/sc, dest_en/tc/sc

stops{ stop_id -> stop }:
  stop_id, name_en, name_tc, name_sc, lat, long

route_stops{ "route|bound|service_type" -> [stop_id, ...] }

fares and freq are patched in later by the GTFS crawler.
"""
import logging
from .utils import fetch_json, tc_to_sc

logger = logging.getLogger(__name__)

BASE = "https://data.etabus.gov.hk/v1/transport/kmb"


async def crawl(client) -> dict:
    logger.info("KMB: fetching routes...")
    rdata = await fetch_json(client, f"{BASE}/route/")
    raw_routes = rdata.get("data", [])
    logger.info(f"KMB: {len(raw_routes)} route-direction records")

    routes = []
    for r in raw_routes:
        orig_tc = r.get("orig_tc", "")
        dest_tc = r.get("dest_tc", "")
        routes.append({
            "route":       r["route"],
            "bound":       r["bound"],       # "O" or "I"
            "service_type": r["service_type"],
            "orig_en":     r.get("orig_en", ""),
            "orig_tc":     orig_tc,
            "orig_sc":     tc_to_sc(orig_tc),
            "dest_en":     r.get("dest_en", ""),
            "dest_tc":     dest_tc,
            "dest_sc":     tc_to_sc(dest_tc),
        })

    logger.info("KMB: fetching all stops...")
    sdata = await fetch_json(client, f"{BASE}/stop")
    raw_stops = sdata.get("data", [])
    stops: dict[str, dict] = {}
    for s in raw_stops:
        name_tc = s.get("name_tc", "")
        stops[s["stop"]] = {
            "stop_id":  s["stop"],
            "name_en":  s.get("name_en", ""),
            "name_tc":  name_tc,
            "name_sc":  tc_to_sc(name_tc),
            "lat":      str(s.get("lat", "")),
            "long":     str(s.get("long", "")),
        }
    logger.info(f"KMB: {len(stops)} stops")

    logger.info("KMB: fetching all route-stops...")
    rsdata = await fetch_json(client, f"{BASE}/route-stop/")
    raw_rs = rsdata.get("data", [])

    route_stops: dict[str, list] = {}
    # Build sorted seq map first
    seq_map: dict[str, dict[int, str]] = {}
    for rs in raw_rs:
        key = f"{rs['route']}|{rs['bound']}|{rs['service_type']}"
        seq_map.setdefault(key, {})[int(rs["seq"])] = rs["stop"]

    for key, seqs in seq_map.items():
        route_stops[key] = [seqs[i] for i in sorted(seqs)]

    logger.info(f"KMB: {len(route_stops)} route-direction-servicetype combinations")

    return {
        "routes":      routes,
        "stops":       stops,
        "route_stops": route_stops,
    }
