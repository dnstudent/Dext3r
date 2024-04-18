from pathlib import Path
from typing import Any, Optional
from datetime import timedelta, date

# from .read import read_invalid_tasks, read_existing_requests

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


# def make_stacks(
#     metas,
#     existing_requests_path: Path = base / "data" / "requests.json",
#     invalid_tasks_path: Path = base / "data" / "invalid_tasks.txt",
#     start_from=datetime(1989, 1, 1),
# ):
#     invalid_tasks = read_invalid_tasks(invalid_tasks_path)
#     sent_requests = read_existing_requests(existing_requests_path).join(
#         invalid_tasks, on="task", how="anti"
#     )
#     parts = (
#         metas.select("id", "name", "agg_code", "begin", "end", "v")
#         .filter(pl.col("end").ge(start_from))
#         .with_columns(
#             pl.max_horizontal(pl.col("begin"), pl.lit(start_from)).alias("begin")
#         )
#     )
#     part1 = parts.with_columns(
#         pl.min_horizontal(pl.col("end"), pl.lit(datetime(2005, 1, 1))).alias("end"),
#         pl.lit(1).alias("part"),
#     )
#     part2 = parts.with_columns(
#         pl.max_horizontal(pl.col("begin"), pl.lit(datetime(2005, 1, 1))).alias("begin"),
#         pl.lit(2).alias("part"),
#     )
#     parts = (
#         (
#             pl.concat([part1, part2], how="vertical")
#             .filter(pl.col("begin") < pl.col("end"))
#             .with_columns(
#                 (pl.col("end") - pl.col("begin")).dt.total_days().alias("count")
#             )
#         )
#         .join(sent_requests, on=["id", "agg_code", "part"], how="anti")
#         .sort("count", "begin")
#     )

#     stack_1 = parts.filter(pl.col("part").eq(1)).to_dicts()
#     stack_2 = parts.filter(pl.col("part").eq(2)).to_dicts()
#     return stack_1, stack_2


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
