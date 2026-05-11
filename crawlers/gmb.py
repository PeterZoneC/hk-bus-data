"""GMB (Green Minibus) crawler.

Output schema:

routes[]:
  route_id (internal int), route_seq (1=outbound/2=inbound),
  region (HKI/KLN/NT), route_code,
  description_tc, description_sc, description_en,
  orig_tc, orig_sc, orig_en,
  dest_tc, dest_sc, dest_en,
  headways[]: { weekdays[7], public_holiday, start_time, end_time,
                frequency (min), frequency_upper (min or null) }
  stops[stop_id, ...]      -- ordered

stops{ stop_id -> { lat, lng } }

fares patched in later by GTFS crawler (GMB fares come from GTFS).
"""
import asyncio
import logging
from .utils import fetch_json, tc_to_sc

logger = logging.getLogger(__name__)

BASE = "https://data.etagmb.gov.hk"
REGIONS = ["HKI", "KLN", "NT"]

# GMB sits behind CloudFront and requires Referer to avoid 403
GMB_HEADERS = {"Referer": "https://data.etagmb.gov.hk/"}


async def crawl(client) -> dict:
    routes: list[dict] = []
    stops: dict[str, dict] = {}

    for region in REGIONS:
        logger.info(f"GMB: fetching route list for {region}...")
        data = await fetch_json(client, f"{BASE}/route/{region}", headers=GMB_HEADERS)
        # API returns {"data": {"routes": ["1","2",...]}} (list of strings)
        route_codes = data.get("data", {}).get("routes", [])
        logger.info(f"GMB {region}: {len(route_codes)} routes")

        sem = asyncio.Semaphore(8)

        async def fetch_route(region=region, code=None):
            async with sem:
                try:
                    rdata = await fetch_json(
                        client, f"{BASE}/route/{region}/{code}",
                        headers=GMB_HEADERS
                    )
                    # data is a list of service-type objects
                    data_list = rdata.get("data", [])
                    if not data_list:
                        return
                    route_data = data_list[0]   # use first service type (normal departure)
                    directions = route_data.get("directions", [])

                    for direction in directions:
                        route_seq  = direction.get("route_seq")
                        route_id   = direction.get("route_id")
                        desc_tc    = route_data.get("description_tc", "")
                        desc_en    = route_data.get("description_en", "")
                        orig_tc    = direction.get("orig_tc", "")
                        dest_tc    = direction.get("dest_tc", "")
                        orig_en    = direction.get("orig_en", "")
                        dest_en    = direction.get("dest_en", "")

                        # Parse headways
                        headways = []
                        for hw in direction.get("headways", []):
                            headways.append({
                                "weekdays":        hw.get("weekdays", [True]*7),
                                "public_holiday":  hw.get("public_holiday", False),
                                "start_time":      hw.get("start_time", ""),
                                "end_time":        hw.get("end_time", ""),
                                "frequency":       hw.get("frequency"),
                                "frequency_upper": hw.get("frequency_upper"),
                            })

                        routes.append({
                            "route_id":       route_id,
                            "route_seq":      route_seq,
                            "region":         region,
                            "route_code":     code,
                            "description_tc": desc_tc,
                            "description_sc": tc_to_sc(desc_tc),
                            "description_en": desc_en,
                            "orig_tc":        orig_tc,
                            "orig_sc":        tc_to_sc(orig_tc),
                            "orig_en":        orig_en,
                            "dest_tc":        dest_tc,
                            "dest_sc":        tc_to_sc(dest_tc),
                            "dest_en":        dest_en,
                            "headways":       headways,
                            "stops":          [],   # filled below
                            "fares":          [],   # filled by GTFS
                        })

                        # Fetch stop sequence for this direction
                        try:
                            sdata = await fetch_json(
                                client,
                                f"{BASE}/route-stop/{route_id}/{route_seq}",
                                headers=GMB_HEADERS
                            )
                            stop_list = sdata.get("data", {}).get("route_stops", [])
                            stop_ids = []
                            for s in stop_list:
                                sid = str(s.get("stop_id", ""))
                                stop_ids.append(sid)
                                if sid not in stops:
                                    # Fetch stop coords
                                    try:
                                        stop_detail = await fetch_json(
                                            client, f"{BASE}/stop/{sid}",
                                            headers=GMB_HEADERS
                                        )
                                        sd = stop_detail.get("data", {})
                                        coords = sd.get("coordinates", {}).get("wgs84", {})
                                        stops[sid] = {
                                            "lat": str(coords.get("latitude", "")),
                                            "lng": str(coords.get("longitude", "")),
                                        }
                                    except Exception:
                                        stops[sid] = {"lat": "", "lng": ""}
                            routes[-1]["stops"] = stop_ids
                        except Exception as e:
                            logger.warning(
                                f"GMB: failed to fetch stops for route {route_id} seq {route_seq}: {e}"
                            )
                except Exception as e:
                    logger.warning(f"GMB: failed to fetch route {region}/{code}: {e}")

        await asyncio.gather(*[fetch_route(region, code) for code in route_codes])

    logger.info(f"GMB: done. {len(routes)} route-directions, {len(stops)} unique stops")
    return {"routes": routes, "stops": stops}
