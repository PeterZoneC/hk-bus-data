"""Main entry point.

Crawls all operators, patches GTFS fares/freq, and outputs:
  bus_data.json       — full data (for debugging)
  bus_data.min.json   — minified (for App download)
  bus_data.md5        — MD5 of the minified file

JSON top-level structure:
{
  "version":      "YYYYMMDDHHMMSS",
  "generated_at": "ISO8601",
  "nlb":   { routes, route_stops },
  "kmb":   { routes, stops, route_stops },   ← fares/freq from GTFS
  "gmb":   { routes, stops },                ← fares/freq from GTFS
  "mtr_bus": { routes, stops }
}
"""
import asyncio
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone, timedelta

import httpx

from crawlers import nlb, kmb, gmb, mtr_bus, gtfs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("main")

HKT = timezone(timedelta(hours=8))
TIMEOUT = httpx.Timeout(60.0, pool=None)


def patch_kmb_fares_freq(kmb_data: dict, gtfs_data: dict):
    """Attach GTFS fares and frequencies to each KMB route record."""
    fares_kmb = gtfs_data["fares"].get("KMB", {})
    fares_lwb = gtfs_data["fares"].get("LWB", {})
    freq_kmb  = gtfs_data["freq"].get("KMB", {})
    freq_lwb  = gtfs_data["freq"].get("LWB", {})

    # Build a route→gtfs_route_id mapping from route_stops keys
    # KMB route_stops key: "route|bound|service_type"
    # GTFS route_id: numeric string (we match by stop sequence)
    # Simplification: for now, embed fares/freq by gtfs route_id in a lookup
    # The app will need to cross-reference by its own gtfs_id mapping.
    # We store them keyed by "route|bound|service_type" where gtfs_id is known.

    # We output fares/freq as separate lookup tables keyed by GTFS route_id
    # so the app can join on its existing gtfs_id column.
    kmb_data["gtfs_fares"] = {**fares_kmb, **fares_lwb}
    kmb_data["gtfs_freq"]  = {**freq_kmb,  **freq_lwb}


def patch_gmb_fares(gmb_data: dict, gtfs_data: dict):
    """Attach GTFS fares to each GMB route record by matching route_id."""
    fares_gmb = gtfs_data["fares"].get("GMB", {})
    freq_gmb  = gtfs_data["freq"].get("GMB", {})

    for route in gmb_data["routes"]:
        rid = str(route.get("route_id", ""))
        # Fares: try bound "1" (outbound) and "2" (inbound)
        bound_str = str(route.get("route_seq", "1"))
        route["fares"] = fares_gmb.get(rid, {}).get(bound_str, [])
        route["freq"]  = freq_gmb.get(rid, {}).get(bound_str, {})


async def run():
    now = datetime.now(HKT)
    version = now.strftime("%Y%m%d%H%M%S")
    generated_at = now.isoformat()

    logger.info("=" * 60)
    logger.info(f"Starting crawl — version {version}")
    logger.info("=" * 60)

    async def safe(coro, name: str, fallback: dict):
        try:
            return await coro
        except Exception as e:
            logger.error(f"{name} crawler failed: {e}")
            return fallback

    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
        logger.info("Launching all crawlers in parallel...")
        nlb_data, kmb_data, gmb_data, mtr_data, gtfs_data = await asyncio.gather(
            safe(nlb.crawl(client),     "NLB",     {"routes": [], "route_stops": {}}),
            safe(kmb.crawl(client),     "KMB",     {"routes": [], "stops": {}, "route_stops": {}}),
            safe(gmb.crawl(client),     "GMB",     {"routes": [], "stops": {}}),
            safe(mtr_bus.crawl(client), "MTR Bus", {"routes": [], "stops": []}),
            safe(gtfs.crawl(client),    "GTFS",    {"fares": {}, "freq": {}, "service_days": {}}),
        )

    logger.info("All crawlers done. Patching GTFS data...")
    patch_kmb_fares_freq(kmb_data, gtfs_data)
    patch_gmb_fares(gmb_data, gtfs_data)

    output = {
        "version":      version,
        "generated_at": generated_at,
        "nlb":          nlb_data,
        "kmb":          kmb_data,
        "gmb":          gmb_data,
        "mtr_bus":      mtr_data,
    }

    logger.info("Writing bus_data.json (pretty)...")
    with open("bus_data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    logger.info("Writing bus_data.min.json (minified)...")
    minified = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
    with open("bus_data.min.json", "w", encoding="utf-8") as f:
        f.write(minified)

    md5 = hashlib.md5(minified.encode("utf-8")).hexdigest()
    with open("bus_data.md5", "w") as f:
        f.write(md5)

    size_kb = len(minified.encode("utf-8")) / 1024
    logger.info(f"Done! bus_data.min.json = {size_kb:.1f} KB, MD5 = {md5}")

    # Summary stats
    logger.info("=" * 60)
    logger.info(f"  NLB routes:      {len(nlb_data['routes'])}")
    logger.info(f"  KMB routes:      {len(kmb_data['routes'])}")
    logger.info(f"  KMB stops:       {len(kmb_data['stops'])}")
    logger.info(f"  GMB routes:      {len(gmb_data['routes'])}")
    logger.info(f"  MTR Bus routes:  {len(mtr_data['routes'])}")
    logger.info(f"  MTR Bus stops:   {len(mtr_data['stops'])}")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(run())
