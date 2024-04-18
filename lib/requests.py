from datetime import datetime, timedelta
import requests
import polars as pl

dformat = r"%Y-%m-%dT%H:%M:%SZ"
tdict = {"T_MAX": "2", "T_MIN": "3"}


def slice_payload(slice, email):
    start_date = min(s["from"] for s in slice)
    end_date = max(s["to"] for s in slice) + timedelta(days=1)
    start_datetime = datetime(
        start_date.year, start_date.month, start_date.day
    ) - timedelta(hours=1)
    end_datetime = datetime(end_date.year, end_date.month, end_date.day)
    variables = list(set(s["v"] for s in slice))
    if len(variables) == 1:
        variables = variables[0]
    stations = [s["id"] for s in slice]
    return {
        "email": email,
        "begin_datetime": start_datetime.strftime(dformat),
        "end_datetime": end_datetime.strftime(dformat),
        "variable": variables,
        "station": stations,
        "fmt": "csv",
    }


def request_slice(slice, email):
    payload = slice_payload(slice, email)
    return (
        requests.get("https://simc.arpae.it/meteozen/debra/api/data", payload),
        payload,
    )
