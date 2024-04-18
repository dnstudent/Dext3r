from pathlib import Path
import json
from datetime import datetime
from typing import Any

import polars as pl


def register_request_task(previous_requests: list[dict[str, Any]], station, task_id):
    previous_requests.append(station | {"task_id": task_id})


def register_task_payload(task_id, payload, path):
    if Path(path).exists():
        with open(path, "rt") as f:
            data = json.load(f)
    else:
        data = {}
    data[task_id] = payload
    data[task_id]["request_time"] = datetime.now().strftime(r"%Y-%m-%dT%H:%M:%SZ")
    with open(path, "wt") as f:
        json.dump(data, f)


def register_task_content(task_id, stations, path):
    if not Path(path).exists():
        print(f"Requests file does not exist at {path}")
        data = []
    else:
        data = (
            pl.read_csv(path)
            .with_columns(pl.col("from").str.to_date(), pl.col("to").str.to_date())
            .to_dicts()
        )
    for station in stations:
        register_request_task(data, station, task_id)
    pl.from_dicts(data).write_csv(path)


def register_task(
    task_id: str,
    stations: list[dict[str, Any]],
    payload: dict[str, Any],
    paths: str | Path,
):
    register_task_content(task_id, stations, paths[0])
    register_task_payload(task_id, payload, paths[1])
