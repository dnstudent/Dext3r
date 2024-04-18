from pathlib import Path
from typing import Any, Optional
from datetime import timedelta, date

import polars as pl


def tot_elements(stations: list[dict[str, Any]]):
    if stations:
        start_date: date = min(s["from"] for s in stations)
        end_date: date = max(s["to"] for s in stations)
        aggregation_period = min(s["agg_period"] for s in stations)
        return (
            int((end_date - start_date + timedelta(days=1)).total_seconds())
            * len(stations)
            // aggregation_period
        )
    else:
        return 0


def station_name_in(station, stack: list):
    for s in stack:
        if station["name"].lower().strip() == s["name"].lower().strip():
            return True
    return False


def find_biggest(new_slice, sorted_queue, max_size) -> Optional[int]:
    if len(new_slice) == 0 and len(sorted_queue) > 0:
        for i, s in enumerate(sorted_queue):
            if s["count"] <= max_size:
                return i
    part = new_slice[-1]["timeline_section"]
    agg_period = new_slice[-1]["agg_period"]
    var = new_slice[-1]["v"]
    # size = sum(s["count"] for s in new_slice)
    for i, s in enumerate(sorted_queue):
        if (
            s["timeline_section"] == part
            and s["agg_period"] == agg_period
            and s["v"] == var
            # It is essential to avoid requesting stations with the same name, otw the parsing will be impossible later
            and (not station_name_in(s, new_slice))
            and (
                # tot_elements must be computed repeatedly because the queue size changes depending on the relations between elements
                tot_elements(new_slice + [s])
                <= max_size
            )
        ):
            return i
    return None


def pop_biggest_slice(sorted_queue, max_size):
    stations = []
    station_to_add = find_biggest(stations, sorted_queue, max_size)
    while (station_to_add is not None) and (len(sorted_queue) > 0):
        stations.append(sorted_queue.pop(station_to_add))
        station_to_add = find_biggest(stations, sorted_queue, max_size)
    return stations
