from typing import Optional

import polars as pl
from pathlib import Path
import requests

base = Path(__file__).parent.parent


def download_available_stations(base: Path):
    """
    Downloads the available station information and saves it to station_infos.json.

    Args:
        base (Path): The base path to the data directory.

    Returns:
        None
    """
    station_infos = requests.get("https://simc.arpae.it/meteozen/rt_data/stations").text
    (base / "station_infos.json").write_text(station_infos)


def download_available_series(base: Path):
    """
    Requests the available Dext3r series and saves it to to series_infos.json.

    Args:
        base (Path): The base path to the data directory.

    Returns:
        None
    """
    series_infos = requests.get(
        "https://simc.arpae.it/meteozen/rt_data/archivesummary"
    ).text
    (base / "series_infos.json").write_text(series_infos)


def list_available_series(base: Path, force_request=False) -> pl.DataFrame:
    """
    Retrieves the available series information.

    Args:
        base (Path): The base path to the data directory.
        force_request (bool, optional): If True, forces the download of the available series information even if it already exists. Defaults to False.

    Returns:
        polars.DataFrame: A DataFrame containing the available series information.
    """
    data_path = base / "series_infos.json"
    if (not data_path.exists()) or force_request:
        download_available_series(base)
    series_info = (
        pl.read_json(data_path)
        .rename({"variable": "v"})
        .with_columns(
            pl.col("v")
            .str.extract_groups(
                r"(?P<agg_code>\d),0,(?P<agg_period>\d+)/(?P<L1>[^,]+),(?P<L2>[^,]+),(?P<L3>[^,]+),(?P<L4>[^,]+)/(?P<variable>.*)"
            )
            .alias("codes")
        )
        .unnest("codes")
        .cast(
            {
                "agg_period": pl.Int32(),
                "agg_code": pl.Int32(),
                "begin": pl.Datetime(),
                "end": pl.Datetime(),
            }
        )
        .with_columns(
            pl.col("begin").dt.date().alias("begin"),
            pl.col("end").dt.date().alias("end"),
        )
    )
    return series_info


def list_available_stations(base: Path, force_request=False) -> pl.DataFrame:
    data_path = Path(base / "station_infos.json")
    if (not data_path.exists()) or force_request:
        download_available_stations(base)
    return pl.read_json(data_path)


def load_series_meta(variable, path, all, force_request=False):
    stations = list_available_stations()
    stat_infos = list_available_series(force_request)
    part = (
        stat_infos.filter(
            pl.col("variable").eq(variable), pl.col("agg_code").is_in([2, 3])
        )
        .sort("agg_period", descending=True)
        .group_by("station", "agg_code")
        .first()
    )
    return stations.join(part, left_on="id", right_on="station", how="inner")


def list_available_meta(base: Path, force_request=False):
    return list_available_stations(base, force_request).join(
        list_available_series(base, force_request),
        left_on="id",
        right_on="station",
        how="inner",
    )
