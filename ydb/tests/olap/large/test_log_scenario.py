import datetime
import os
import pytest

import random

import logging
import yatest.common

from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.olap.common.thread_helper import TestThread
from ydb.tests.olap.common.ydb_client import YdbClient

from enum import Enum


logger = logging.getLogger(__name__)


class YdbWorkloadLog:
    class PKMode(Enum):
        TIMESTAMP_DEVIATION = (1,)
        UNIFORM = 2

    def __init__(
        self,
        endpoint: str,
        database: str,
        table_name: str,
        timestamp_deviation: int | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
    ):
        self.path: str = yatest.common.binary_path(os.environ["YDB_CLI_BINARY"])
        self.endpoint: str = endpoint
        self.database: str = database
        self.begin_command: list[str] = [
            self.path,
            "-e",
            self.endpoint,
            "-d",
            self.database,
            "workload",
            "log",
            "--path",
            table_name,
        ]
        if timestamp_deviation is not None:
            if date_from is not None:
                raise TypeError("timestamp_deviation and uniform PK mode should be applied mutual exclusively")
            if date_to is not None:
                raise TypeError("timestamp_deviation and uniform PK mode should be applied mutual exclusively")
            self.timestamp_deviation = timestamp_deviation
            self.pk_mode = YdbWorkloadLog.PKMode.TIMESTAMP_DEVIATION
        else:
            if date_from is not None:
                if date_to is None:
                    raise TypeError("missing date_to")
                self.date_from = date_from
                self.date_to = date_to
                self.pk_mode = YdbWorkloadLog.PKMode.UNIFORM
            else:
                raise TypeError("missing date_from")

    def _call(self, command: list[str], wait=False, timeout=None):
        logging.info(f'YdbWorkloadLog execute {' '.join(command)} with wait = {wait}')
        yatest.common.execute(command=command, wait=wait, timeout=timeout)

    def create_table(self, table_name: str):
        command = self.begin_command + ["init", "--path", table_name, "--store", "column", "--ttl", "1000"]
        self._call(command=command, wait=True)

    def _insert_rows(self, operation_name: str, seconds: int, threads: int, rows: int, wait: bool):
        logging.info(f'YdbWorkloadLog {operation_name}')
        command = self.begin_command + [
            "run",
            str(operation_name),
            "--seconds",
            str(seconds),
            "--threads",
            str(threads),
            "--rows",
            str(rows),
        ]
        if self.pk_mode == YdbWorkloadLog.PKMode.TIMESTAMP_DEVIATION:
            command = command + ["--timestamp_deviation", str(self.timestamp_deviation)]
        else:
            command = command + ["--date-from", str(self.date_from), "--date-to", str(self.date_to)]
        self._call(command=command, wait=wait, timeout=max(2 * seconds, 60))

    # seconds - Seconds to run workload
    # threads - Number of parallel threads in workload
    # rows - Number of rows to upsert
    def bulk_upsert(self, seconds: int, threads: int, rows: int, wait: bool = False):
        self._insert_rows(operation_name="bulk_upsert", seconds=seconds, threads=threads, rows=rows, wait=wait)

    def upsert(self, seconds: int, threads: int, rows: int, wait: bool = False):
        self._insert_rows(operation_name="upsert", seconds=seconds, threads=threads, rows=rows, wait=wait)

    def insert(self, seconds: int, threads: int, rows: int, wait: bool = False):
        self._insert_rows(operation_name="insert", seconds=seconds, threads=threads, rows=rows, wait=wait)

    def __del__(self):
        command: list[str] = self.begin_command + ["clean"]
        try:
            yatest.common.execute(command=command, wait=True)
        except Exception:
            pass


class TestLogScenario(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()
        pass

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_immediate_writing_on_bulk_upsert": True},
            column_shard_config={"alter_object_enabled": True},
            additional_log_configs={"TX_COLUMNSHARD_SCAN": LogLevels.TRACE},
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(endpoint=f"grpc://{node.host}:{node.port}", database=f"/{config.domain_name}")
        cls.ydb_client.wait_connection(timeout=60)

    def get_row_count(self) -> int:
        return self.ydb_client.query(f"select count(*) as Rows from `{self.table_name}`")[0].rows[0]["Rows"]

    def aggregation_query(self, duration: datetime.timedelta):
        deadline: datetime = datetime.datetime.now() + duration
        while datetime.datetime.now() < deadline:
            hours: int = random.randint(1, 10)
            self.ydb_client.query(f"SELECT COUNT(*) FROM `{self.table_name}` ")
            self.ydb_client.query(
                f"SELECT * FROM `{self.table_name}` WHERE timestamp < CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours})"
            )
            self.ydb_client.query(
                f"SELECT COUNT(*) FROM `{self.table_name}` WHERE timestamp < CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours})"
            )
            self.ydb_client.query(
                f"SELECT COUNT(*) FROM `{self.table_name}` WHERE "
                + f"(timestamp >= CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours + 1})) AND "
                + f"(timestamp <= CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours}))"
            )

    @pytest.mark.parametrize('timestamp_deviation', [3 * 60, 365 * 24 * 60 * 2])  # [3 hours, 2 years]
    def test_log_deviation(self, timestamp_deviation: int):
        """As per https://github.com/ydb-platform/ydb/issues/13530"""

        wait_time: int = int(get_external_param("wait_seconds", "3"))
        self.table_name: str = "log"

        ydb_workload: YdbWorkloadLog = YdbWorkloadLog(
            endpoint=self.ydb_client.endpoint,
            database=self.ydb_client.database,
            table_name=self.table_name,
            timestamp_deviation=timestamp_deviation,
        )
        ydb_workload.create_table(self.table_name)

        # deduplication for simple reader will be added in #15043
        self.ydb_client.query(f"ALTER OBJECT `/{self.ydb_client.database}/{self.table_name}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`) ")
        ydb_workload.bulk_upsert(seconds=wait_time, threads=10, rows=500, wait=True)
        logging.info(f"Count rows after insert {self.get_row_count()} before wait")

        assert self.get_row_count() != 0

        prev_count: int = self.get_row_count()

        threads: list[TestThread] = []
        threads.append(TestThread(target=ydb_workload.bulk_upsert, args=[wait_time, 10, 500, True]))
        threads.append(TestThread(target=ydb_workload.insert, args=[wait_time, 10, 500, True]))
        threads.append(TestThread(target=ydb_workload.upsert, args=[wait_time, 10, 500, True]))

        for _ in range(10):
            threads.append(TestThread(target=self.aggregation_query, args=[datetime.timedelta(seconds=int(wait_time))]))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        current_count: int = self.get_row_count()
        logging.info(f'check insert: {current_count} {prev_count}')
        assert current_count != prev_count

    def test_log_uniform(self):
        """As per https://github.com/ydb-platform/ydb/issues/13531"""

        wait_time: int = int(get_external_param("wait_seconds", "30"))
        self.table_name: str = "log"

        ydb_workload: YdbWorkloadLog = YdbWorkloadLog(
            endpoint=self.ydb_client.endpoint,
            database=self.ydb_client.database,
            table_name=self.table_name,
            date_from=0,
            date_to=2678400000,
        )  # Monday, 16 November 2054, 0:00:00
        ydb_workload.create_table(self.table_name)
        ydb_workload.bulk_upsert(seconds=wait_time, threads=10, rows=1, wait=True)
        logging.info(f"Count rows after insert {self.get_row_count()} before wait")

        assert self.get_row_count() != 0

        prev_count: int = self.get_row_count()

        threads: list[TestThread] = []
        threads.append(TestThread(target=ydb_workload.bulk_upsert, args=[wait_time, 10, 1, True]))
        threads.append(TestThread(target=ydb_workload.insert, args=[wait_time, 10, 1, True]))
        threads.append(TestThread(target=ydb_workload.upsert, args=[wait_time, 10, 1, True]))

        for _ in range(10):
            threads.append(TestThread(target=self.aggregation_query, args=[datetime.timedelta(seconds=int(wait_time))]))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        current_count: int = self.get_row_count()
        logging.info(f'check insert: {current_count} {prev_count}')
        assert current_count != prev_count
