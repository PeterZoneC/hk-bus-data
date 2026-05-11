"""NLB (New Lantao Bus) crawler.

Output schema matches the app's NBLBusListItem / NBLBusStopsItem models:

routes[]:
  routeId, routeNo, routeName_c, routeName_s, routeName_e,
  overnightRoute (0/1), specialRoute (0/1)

route_stops{ routeId -> stops[] }:
  stopId, stopName_c, stopName_s, stopName_e,
  stopLocation_c, stopLocation_s, stopLocation_e,
  latitude, longitude, fare, fareHoliday,
  someDepartureObserveOnly (0/1), sequence
"""
import asyncio
import logging
from .utils import fetch_json, tc_to_sc

logger = logging.getLogger(__name__)

BASE = "https://rt.data.gov.hk/v2/transport/nlb"


async def crawl(client) -> dict:
    logger.info("NLB: fetching route list...")
    data = await fetch_json(client, f"{BASE}/route.php?action=list")
    raw_routes = data.get("routes", [])
    logger.info(f"NLB: {len(raw_routes)} routes found")

    routes = []
    for r in raw_routes:
        name_c = r.get("routeName_c", "")
        name_e = r.get("routeName_e", "")
        routes.append({
            "routeId":       str(r["routeId"]),
            "routeNo":       r.get("routeNo", ""),
            "routeName_c":   name_c,
            "routeName_s":   tc_to_sc(name_c),
            "routeName_e":   name_e,
            "overnightRoute": int(r.get("overnightRoute", 0)),
            "specialRoute":   int(r.get("specialRoute", 0)),
        })

    route_stops: dict[str, list] = {}

    async def fetch_stops(route: dict):
        rid = route["routeId"]
        try:
            sdata = await fetch_json(client, f"{BASE}/stop.php?action=list&routeId={rid}")
            stops_raw = sdata.get("stops", [])
            stops = []
            for seq, s in enumerate(stops_raw, start=1):
                name_c = s.get("stopName_c", "")
                name_e = s.get("stopName_e", "")
                loc_c  = s.get("stopLocation_c", "")
                loc_e  = s.get("stopLocation_e", "")
                stops.append({
                    "stopId":               str(s["stopId"]),
                    "stopName_c":           name_c,
                    "stopName_s":           tc_to_sc(name_c),
                    "stopName_e":           name_e,
                    "stopLocation_c":       loc_c,
                    "stopLocation_s":       tc_to_sc(loc_c),
                    "stopLocation_e":       loc_e,
                    "latitude":             str(s.get("latitude", "")),
                    "longitude":            str(s.get("longitude", "")),
                    "fare":                 s.get("fare") or None,
                    "fareHoliday":          s.get("fareHoliday") or None,
                    "someDepartureObserveOnly": int(s.get("someDepartureObserveOnly", 0)),
                    "sequence":             seq,
                })
            route_stops[rid] = stops
        except Exception as e:
            logger.warning(f"NLB: failed to fetch stops for route {rid}: {e}")
            route_stops[rid] = []

    # Limit concurrency to 10 to be polite
    sem = asyncio.Semaphore(10)

    async def fetch_with_sem(route):
        async with sem:
            await fetch_stops(route)

    await asyncio.gather(*[fetch_with_sem(r) for r in routes])
    logger.info(f"NLB: done. {len(routes)} routes, {sum(len(v) for v in route_stops.values())} stops total")

    return {"routes": routes, "route_stops": route_stops}
