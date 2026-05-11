"""GTFS parser — extracts fares and timetable frequencies.

Downloads the government GTFS ZIP and extracts:
  - fare_attributes.txt  → per-stop cumulative fares for KMB/LWB/GMB
  - fare_rules.txt       → maps fare_id to route + origin/dest stops
  - frequencies.txt      → KMB/LWB/GMB timetable frequencies
  - trips.txt            → maps trip_id → route_id + direction + calendar
  - calendar.txt         → service days (Mon-Sun + holiday)

Returns:
  fares:  { agency -> { gtfs_route_id -> { bound -> [fare_per_boarding_stop] } } }
  freq:   { agency -> { gtfs_route_id -> { bound -> { calendar ->
              { start_time -> { end_time, headway_secs } } } } } }
  service_days: { calendar_id -> [sun, mon, tue, wed, thu, fri, sat] }

The "fares" list follows hkbus logic:
  For each boarding stop index `on`, store the MAXIMUM fare seen
  (i.e. the fare to the furthest destination reachable from that stop).
  Result is an array indexed by stop sequence (0-based), length = stops-1.
"""
import asyncio
import csv
import io
import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

GTFS_URL = "https://static.data.gov.hk/td/pt-headway-tc/gtfs.zip"
CACHE_PATH = Path("gtfs.zip")

TARGET_AGENCIES = {"KMB", "LWB", "GMB", "CTB"}


async def crawl(client) -> dict:
    if not CACHE_PATH.exists():
        logger.info("GTFS: downloading ZIP (~13MB)...")
        r = await client.get(GTFS_URL)
        r.raise_for_status()
        CACHE_PATH.write_bytes(r.content)
        logger.info("GTFS: download complete")
    else:
        logger.info("GTFS: using cached gtfs.zip")

    with zipfile.ZipFile(CACHE_PATH) as zf:
        def read(name: str) -> list[dict]:
            with zf.open(name) as f:
                return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))

        logger.info("GTFS: parsing fare_attributes...")
        fare_attrs = read("fare_attributes.txt")
        logger.info("GTFS: parsing fare_rules...")
        fare_rules = read("fare_rules.txt")
        logger.info("GTFS: parsing frequencies...")
        frequencies = read("frequencies.txt")
        logger.info("GTFS: parsing trips...")
        trips = read("trips.txt")
        logger.info("GTFS: parsing calendar...")
        calendar = read("calendar.txt")

    # ── service days ────────────────────────────────────────────────────────
    service_days: dict[str, list[int]] = {}
    for row in calendar:
        sid = row["service_id"].strip()
        # Store as [sun, mon, tue, wed, thu, fri, sat]
        service_days[sid] = [
            int(row.get("sunday", 0)),
            int(row.get("monday", 0)),
            int(row.get("tuesday", 0)),
            int(row.get("wednesday", 0)),
            int(row.get("thursday", 0)),
            int(row.get("friday", 0)),
            int(row.get("saturday", 0)),
        ]

    # ── fare_attributes: fare_id → (price, agency_id) ───────────────────────
    fare_price: dict[str, tuple[str, str]] = {}
    for row in fare_attrs:
        fid     = row["fare_id"].strip()
        price   = row["price"].strip()
        agency  = row.get("agency_id", "").strip()
        if agency in TARGET_AGENCIES:
            fare_price[fid] = (price, agency)

    # ── fare_rules: map fare_id → route_id ──────────────────────────────────
    # fare_id format: {route_id}-{bound}-{service_type}-{on_stop_seq}
    # We rebuild: agency -> route_id -> bound -> { on_seq -> (price, max_off_seq) }
    # Then flatten to sorted list.
    fare_map: dict[str, dict[str, dict[str, dict[int, tuple[str, int]]]]] = {}

    for row in fare_rules:
        fid      = row["fare_id"].strip()
        if fid not in fare_price:
            continue
        price, agency = fare_price[fid]

        parts = fid.split("-")
        if len(parts) < 4:
            continue
        route_id, bound, _stype, on_str = parts[0], parts[1], parts[2], parts[3]
        try:
            on = int(on_str)
        except ValueError:
            continue

        origin_id = row.get("origin_id", "").strip()
        dest_id   = row.get("destination_id", "").strip()
        try:
            off = int(dest_id) if dest_id.isdigit() else 0
        except ValueError:
            off = 0

        fare_map.setdefault(agency, {}).setdefault(route_id, {}).setdefault(bound, {})
        existing = fare_map[agency][route_id][bound].get(on)
        if existing is None or off > existing[1]:
            fare_map[agency][route_id][bound][on] = (price, off)

    # Flatten to sorted list per route-bound
    fares: dict[str, dict[str, dict[str, list[str]]]] = {}
    for agency, routes in fare_map.items():
        fares[agency] = {}
        for rid, bounds in routes.items():
            fares[agency][rid] = {}
            for bound, seq_map in bounds.items():
                sorted_seqs = sorted(seq_map)
                fares[agency][rid][bound] = [
                    "0" if seq_map[s][0] == "0.0000" else seq_map[s][0]
                    for s in sorted_seqs
                ]

    # ── frequencies + trips → timetable ─────────────────────────────────────
    # trip_id format: {route_id}-{bound}-{calendar}-{start_time_hhmm}
    # frequencies: trip_id → end_time, headway_secs
    freq_map: dict[str, dict[str, dict[str, dict[str, dict]]]] = {}
    # agency -> route_id -> bound -> calendar -> { start_hhmm -> {end, secs} }

    # Build trip lookup
    trip_agency: dict[str, str] = {}
    for row in trips:
        tid = row["trip_id"].strip()
        # agency from route_id prefix: look up in fares dict later, or parse trip_id
        parts = tid.split("-")
        if len(parts) >= 3:
            trip_agency[tid] = row.get("service_id", "").strip()

    for row in frequencies:
        tid   = row["trip_id"].strip()
        start = row["start_time"].strip()[:5].replace(":", "")   # hhmm
        end   = row["end_time"].strip()[:5]
        secs  = int(row["headway_secs"].strip())

        parts = tid.split("-")
        if len(parts) < 4:
            continue
        route_id, bound, calendar, _ = parts[0], parts[1], parts[2], parts[3]

        # Determine agency from fares lookup
        agency = None
        for ag in TARGET_AGENCIES:
            if fares.get(ag, {}).get(route_id):
                agency = ag
                break
        if agency is None:
            # Try to infer from route_id pattern
            continue

        (freq_map
            .setdefault(agency, {})
            .setdefault(route_id, {})
            .setdefault(bound, {})
            .setdefault(calendar, {})
        )[start] = {"end_time": end, "headway_secs": secs}

    logger.info(
        f"GTFS: parsed fares for "
        f"{sum(len(v) for v in fares.values())} route-agencies, "
        f"freq for {sum(len(v) for v in freq_map.values())} route-agencies"
    )
    return {
        "fares":        fares,
        "freq":         freq_map,
        "service_days": service_days,
    }
