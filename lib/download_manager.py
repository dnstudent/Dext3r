import re
from time import sleep
from datetime import timedelta, date
from pathlib import Path
from typing import Any

from tqdm.notebook import tqdm
import polars as pl

from .metas import list_available_meta
from .stack import pop_biggest_slice
from .requests import request_slice
from .write import register_task


class Dext3rDownloader:
    def __init__(self, workspace_path, from_date: date, max_days=15 * 365):
        self.workspace_path = Path(workspace_path)
        if not self.workspace_path.exists():
            self.workspace_path.mkdir(parents=True)
        self.sent_requests_path = self.workspace_path / "requests.csv"
        self.invalid_tasks_path = self.workspace_path / "invalid_tasks.txt"
        self.payloads_path = self.workspace_path / "payloads.json"
        self.from_date = from_date
        self.max_days = max_days

    def init_request_queue(
        self,
        variable: str | list[str],
        aggregation_code: str | list[str],
        aggregation_span: int,
        to_date: date,
        force_meta_request=False,
    ) -> pl.DataFrame:
        """
        Initializes the queue for downloading data.

        Args:
            variable (str | list[str]): The variable(s) to download.
            aggregation_code (str | list[str]): The aggregation(s) to apply.
            aggregation_span (int): The aggregation span in seconds.
            to_date (date): The end date of the measures.
            force_meta_request (bool, optional): Whether to force a metadata request or use the cached version. Defaults to False.

        Returns:
            pl.DataFrame: The queue elements as rows of a DataFrame.

        Raises:
            None
        """
        # Ensure that the inputs are lists
        if type(variable) != list:
            variable = [variable]
        if type(aggregation_code) != list:
            aggregation_code = [aggregation_code]

        variable = pl.from_dict({"variable": variable})
        aggregation_code = pl.from_dict(
            {"agg_code": aggregation_code}, schema={"agg_code": pl.Int32()}
        )

        # Computing the time sections
        cut_dates = pl.date_range(
            self.from_date,
            to_date,
            timedelta(days=self.max_days),
            eager=True,
        ).to_list() + [to_date]
        time_parts = (
            pl.from_dict({"from": cut_dates[:-1], "to": cut_dates[1:]})
            .filter(pl.col("from") < pl.col("to"))
            .with_row_index("timeline_section")
            .cast({"timeline_section": pl.Int32()})
        )
        series_filter = variable.join(aggregation_code, how="cross").with_columns(
            pl.lit(aggregation_span, pl.Int32()).alias("agg_period")
        )
        return (
            list_available_meta(self.workspace_path, force_meta_request)
            .filter(pl.col("end").ge(self.from_date), pl.col("begin").le(to_date))
            .join(series_filter, on=["variable", "agg_code", "agg_period"], how="semi")
            .join(time_parts, how="cross")
            .filter(pl.col("begin") <= pl.col("to"), pl.col("end") >= pl.col("from"))
            .with_columns(
                pl.max_horizontal(pl.col("begin"), pl.col("from")).alias("from"),
                pl.min_horizontal(pl.col("end"), pl.col("to")).alias("to"),
            )
            .with_columns(
                (
                    (
                        pl.col("to") - pl.col("from") + timedelta(days=1)
                    ).dt.total_seconds()
                    // pl.col("agg_period")
                ).alias("count")
            )
            .select(
                [
                    "name",
                    "id",
                    "v",
                    "from",
                    "to",
                    "count",
                    "timeline_section",
                    "agg_period",
                ]
            )
        )

    def read_sent_requests(self) -> pl.DataFrame:
        schema = {
            "task_id": pl.Utf8(),
            "name": pl.Utf8(),
            "id": pl.Utf8(),
            "v": pl.Utf8(),
            "from": pl.Date(),
            "to": pl.Date(),
            "count": pl.Int32(),
            "timeline_section": pl.Int32(),
            "agg_period": pl.Int32(),
        }
        if not self.sent_requests_path.exists():
            return pl.DataFrame(schema=schema)
        return pl.read_csv(self.sent_requests_path, dtypes=schema)

    def read_invalid_tasks(self):
        if not self.invalid_tasks_path.exists():
            tasks = []
        else:
            content = self.invalid_tasks_path.read_text()
            task_regex = re.compile(
                r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"
            )
            tasks = task_regex.findall(content)
        return pl.from_dict({"task_id": tasks}, schema={"task_id": pl.Utf8()})

    def make_request_queue(
        self,
        variable: str | list[str],
        aggregation: str | list[str],
        aggregation_period: int,
        to_date: date,
        resume: bool,
        force_meta_request: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Creates a sorted request queue based on the given parameters. The queue is sorted by the number of elements in each request, keeping the
        heaviest elements at the beginning.

        Args:
            variable (str | list[str]): The variable(s) to include in the request queue.
            aggregation (str | list[str]): The aggregation(s) to include in the request queue.
            aggregation_period (int): The aggregation period to include in the request queue.
            to_date (date): The end date of the request queue.
            resume (bool): Whether to resume from previous requests or force the request of parts that were already processed.
            force_meta_request (bool, optional): Whether to force an updated version of metadata from Dext3r. Defaults to False.

        Returns:
            list[dict[str, Any]]: A list of dictionaries representing the sorted request queue.
        """
        queue = self.init_request_queue(
            variable,
            aggregation,
            aggregation_period,
            to_date,
            force_meta_request,
        )
        if resume:
            # Remove the requests that were already sent, not counting the tasks flagged as invalid
            to_remove = self.read_sent_requests().join(
                self.read_invalid_tasks(), on="task_id", how="anti"
            )
            queue = queue.join(
                to_remove, on=["id", "v", "timeline_section"], how="anti"
            )
        return queue.sort("count", "timeline_section", descending=True).to_dicts()

    def download(
        self,
        email: str | list[str],
        variable: str | list[str],
        aggregation_code: str | list[str],
        aggregation_span: int,
        to_date: date,
        pause: int = 120,
        max_lines: int = 18000,
        resume: bool = True,
        max_tries: int = 5,
    ):
        """
        Downloads data from the Dext3r service.

        Args:
            email (str | list[str]): The email address(es) to use for the download request.
            variable (str | list[str]): The variable(s) to download. Check https://arpa-simc.github.io/dballe/general_ref/btable.html for the list of available variables.
            aggregation_code (str | list[str]): The aggregation code(s) to download. Check https://arpa-simc.github.io/dballe/general_ref/tranges.html for the list of available aggregation codes.
            aggregation_span (int): The aggregation span for the data in seconds. Valid aggregations are 86400 (daily), 3600 (hourly) and 900 (15 minutes).
            to_date (date): The end date for the data.
            pause (int, optional): The pause time between requests in seconds. Defaults to 120. It will be dynamically adjusted based on the number of retries.
            max_lines (int, optional): The maximum number of lines to download per request. Defaults to 18000. The Dext3r service imposes a limit of 25000, but computing the effective request size is not an exact task. The higher the value, the higher the risk of exceeding the limit and the longer the response time.
            resume (bool, optional): Whether to resume a previous download. Defaults to True.
            max_tries (int, optional): The maximum number of retry attempts for failed requests. Defaults to 5.
        """
        # Building the request queue
        queue: list[dict[str, Any]] = self.make_request_queue(
            variable,
            aggregation_code,
            aggregation_span,
            to_date,
            resume,
        )
        if type(email) == str:
            email = [email]
        with tqdm(total=len(queue)) as pbar:
            task = None
            dyn_pause = pause
            while len(queue) > 0:
                sent = False
                c = 0
                email_index = 0
                # Tacking a group of requests suitable to be sent together
                queue_slice = pop_biggest_slice(queue, max_lines)
                while not sent and c < max_tries:
                    try:
                        response, payload = request_slice(
                            queue_slice, email[email_index]
                        )
                        if response.status_code == 200:
                            task = response.json()["task"]
                            # Keeping track of the requests and the payloads
                            register_task(
                                task,
                                queue_slice,
                                payload,
                                [self.sent_requests_path, self.payloads_path],
                            )
                            sent = True
                            pbar.set_postfix_str(f"OK: {task}")
                            email_index = (email_index + 1) % len(email)
                            if c == 0:
                                dyn_pause -= 20
                        else:
                            content = response.json()
                            if "detail" in content:
                                detail = content["detail"]
                            elif "details" in content:
                                detail = content["details"]
                            else:
                                detail = None
                            dyn_pause = min(dyn_pause + 10, 210)
                            pbar.set_postfix_str(
                                f"Error: {response.status_code} ({detail}) ({c+1}/{max_tries}). Last successful task: {task}"
                            )
                            if response.status_code == 403:
                                c = max_tries
                    except Exception as e:
                        names = [s["name"] for s in queue_slice]
                        ids = [s["id"] for s in queue_slice]
                        print(
                            f"Exception '{e}' while processing {{names: {names}, ids: {ids}}}"
                        )
                    c += 1
                    sleep(dyn_pause)
                pbar.update(len(queue_slice))
