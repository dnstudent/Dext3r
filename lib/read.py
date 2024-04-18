from pathlib import Path
import polars as pl
import re
import json
from datetime import datetime

from .requests import dformat

base = Path(__file__).parent.parent


def read_invalid_tasks(path=base / "data" / "completed_tasks.txt"):
    path = Path(path)
    if not path.exists():
        return pl.DataFrame(schema={"task": pl.Utf8()})
    uids = re.findall(
        r"Errore nella richiesta #([\d\-a-z]+)", path.read_text(), flags=re.MULTILINE
    )
    inv1 = pl.DataFrame({"task": uids}, schema={"task": pl.Utf8()})
    manual = [
        pl.read_csv(file, columns=["task"], schema={"task": pl.Utf8()})
        for file in (path.parent / "invalid").glob("*.txt")
    ]
    return pl.concat([inv1] + manual)


def read_existing_requests(path=base / "data" / "requests.json"):
    schema = {
        "id": pl.Utf8(),
        "agg_code": pl.Int32(),
        "part": pl.Int32(),
        "task": pl.Utf8(),
    }
    if path.exists():
        with open(path, "r") as f:
            d = json.load(f)
        parts = []
        for sid, p1 in d.items():
            for agg_code, p2 in p1.items():
                for part, task in p2.items():
                    if task:
                        parts.append(
                            {
                                "id": sid,
                                "agg_code": int(agg_code),
                                "part": int(part),
                                "task": task,
                            }
                        )
        return pl.from_dicts(parts, schema=schema)
    return pl.DataFrame(schema=schema)


def read_payloads(path=base / "data" / "payloads.json"):
    path = Path(path)
    with open(path, "r") as f:
        return json.load(f)


def _next_table(lines, start):
    while lines[start] == "":
        start += 1
    # start now points to the first line of the table
    counter = 0
    while start + counter < len(lines) and lines[start + counter] != "":
        counter += 1
    # counter now contains the number of lines of the table, including the header
    return start, counter


def table_specs(lines):
    tabs = []
    tabs.append(_next_table(lines, 1))
    while tabs[-1][0] + tabs[-1][1] + 1 < len(lines):
        tabs.append(_next_table(lines, tabs[-1][0] + tabs[-1][1] + 1))
    return tabs


def check_dext3r_meta(dext3r_meta, task):
    if dext3r_meta["name"].is_duplicated().any():
        print(
            f"Warning: duplicated station names in task {task} meta. They will be dropped."
        )
        return False
    return True


def check_task_period(task, table, payloads):
    if task not in payloads:
        print(f"Could not compare payload times for task {task}.")
        return True
    payload_begin = datetime.strptime(payloads[task]["begin_datetime"], dformat).date()
    payload_end = datetime.strptime(payloads[task]["end_datetime"], dformat).date()
    try:
        if (
            table["start"].min().date() > payload_begin
            or table["stop"].max().date() < payload_end
        ):
            return False
    except Exception as e:
        print(f"Could not compare payload times for task {task}.")
        print(e)
    return True


def check_task_result(task, meta, payload):
    pass


def read_dext3r_meta(path, spec):
    return pl.read_csv(
        path,
        skip_rows=spec[0],
        n_rows=spec[1] - 1,
        schema={
            "Nome della stazione": pl.Utf8(),
            "Rete di misura": pl.Utf8(),
            "Comune": pl.Utf8(),
            "Provincia": pl.Utf8(),
            "Regione": pl.Utf8(),
            "Nazione": pl.Utf8(),
            "Altezza (Metri sul livello del mare)": pl.Float64(),
            "Longitudine (Gradi Centesimali)": pl.Float64(),
            "Latitudine (Gradi Centesimali)": pl.Float64(),
            "Bacino": pl.Utf8(),
        },
    ).rename(
        {
            "Nome della stazione": "name",
            "Rete di misura": "network",
            "Altezza (Metri sul livello del mare)": "elevation",
            "Longitudine (Gradi Centesimali)": "lon",
            "Latitudine (Gradi Centesimali)": "lat",
        }
    )


def join_station_id(
    task_metadata: pl.DataFrame,
    stations_metadata: pl.DataFrame,
    payloads: dict,
    task: str,
):
    payload = payloads.get(task, None)
    if task_metadata.select("name").is_duplicated().any():
        print(
            f"Warning: duplicated station names in task {task} metadata. Could not join those."
        )
        task_metadata = task_metadata.filter(pl.col("name").is_unique())

    if payload:
        # return task_metadata.join(
        #     stations_metadata.filter(pl.col("id").is_in(payload["station"])).select(
        #         "id", "network", "name"
        #     ),
        #     on=["network", "name"],
        #     how="left",
        # )
        return stations_metadata.filter(pl.col("id").is_in(payload["station"])).join(
            task_metadata, on=["network", "name"], how="semi"
        )
    # -> a table of metadata unique in the names
    same_anag = stations_metadata.join(
        task_metadata, on=["network", "name"], how="semi"
    )
    if not same_anag.select(["network", "name"]).is_duplicated().any():
        return same_anag

    unique_matches = same_anag.filter(pl.struct("network", "name").is_unique())
    dubious_matches = same_anag.join(unique_matches, on="id", how="anti")

    stricter_meta = (
        dubious_matches.with_columns(
            pl.col("height").cast(pl.Int32()).alias("int_elevation"),
        )
        .join(
            task_metadata.with_columns(
                (pl.col("lon") * 10e5).floor().cast(pl.Int64()).alias("int_lon"),
                (pl.col("lat") * 10e5).floor().cast(pl.Int64()).alias("int_lat"),
                pl.col("elevation").cast(pl.Int32()).alias("int_elevation"),
            ),
            left_on=["name", "network", "lon", "lat", "int_elevation"],
            right_on=["name", "network", "int_lon", "int_lat", "int_elevation"],
            how="semi",
        )
        .drop("int_elevation")
    )
    return pl.concat([unique_matches, stricter_meta], how="vertical")


def read_dext3r_data(path, lines, spec, task):
    name = lines[spec[0]]
    variable = re.findall(r"((?:minima)|(?:massima))", lines[spec[0] + 1])[0]
    variable = "T_MIN" if variable == "minima" else "T_MAX"
    return (
        pl.read_csv(
            path,
            new_columns=["start", "stop", "value"],
            skip_rows=spec[0] + 1,
            n_rows=spec[1] - 2,
            truncate_ragged_lines=True,
            schema={"start": pl.Utf8(), "stop": pl.Utf8(), "value": pl.Utf8()},
        )
        .cast({"value": pl.Float64()})
        .with_columns(
            pl.lit(variable).alias("variable"),
            pl.lit(name).alias("name"),
            pl.col("start").str.strptime(pl.Datetime(), r"%Y-%m-%d %H:%M:%S%:z"),
            pl.col("stop").str.strptime(pl.Datetime(), r"%Y-%m-%d %H:%M:%S%:z"),
            pl.lit(task).alias("task"),
        )
    )


def read_dext3r_tables(path, payloads, stations_metadata):
    path = Path(path)
    with open(path, "r") as f:
        data = f.read()
    lines = data.splitlines()
    specs = table_specs(lines)

    task = path.stem[7:]

    specs.pop(-1)

    # Reading metadata
    meta_spec = specs.pop(-1)
    meta = read_dext3r_meta(path, meta_spec)
    check_dext3r_meta(meta, task)
    meta = join_station_id(meta, stations_metadata, payloads, task).with_columns(
        pl.lit(task).alias("task")
    )

    # Reading data
    data_tables = []
    for table_spec in specs:
        ddata = read_dext3r_data(path, lines, table_spec, task)
        if not check_task_period(task, ddata, payloads):
            print(f"Warning: mismatch in {task} data period.")
            continue
        data_tables.append(ddata)
    if data_tables:
        data_tables = (
            pl.concat(data_tables, how="vertical")
            .join(meta.select("id", "name"), on="name", how="inner")
            .select("start", "stop", "value", "variable", "id", "task")
        )
    else:
        data_tables = pl.DataFrame(
            schema={
                "start": pl.Datetime(time_unit="us", time_zone="UTC"),
                "stop": pl.Datetime(time_unit="us", time_zone="UTC"),
                "value": pl.Float64(),
                "variable": pl.Utf8(),
                "id": pl.Utf8(),
                "task": pl.Utf8(),
            }
        )

    return meta, data_tables


def assoc_station_id(dext3r_table, requests, global_meta):
    pass
