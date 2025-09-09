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


class YdbWorkloadOverload:
    def __init__(
        self,
        endpoint: str,
        database: str,
        table_name: str,
        stderr: str = None,
        stdout: str = None,
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
        self.stderr = stderr
        self.stdout = stdout

    def _call(self, command: list[str], wait=False, timeout=None):
        logging.info(f'YdbWorkloadOverload execute {' '.join(command)} with wait = {wait}')
        yatest.common.execute(command=command, wait=wait, timeout=timeout, stderr=self.stderr, stdout=self.stdout)

    def create_table(self, table_name: str):
        command = self.begin_command + ["init", "--path", table_name, "--store", "column", "--ttl", "1000"]
        self._call(command=command, wait=True)

    def _insert_rows(self, operation_name: str, seconds: int, threads: int, rows: int, wait: bool):
        logging.info(f'YdbWorkloadOverload {operation_name}')
        command = self.begin_command + [
            "run",
            str(operation_name),
            "--seconds",
            str(seconds),
            "--threads",
            str(threads),
            "--rows",
            str(rows),
            "--quiet"
        ]
        self._call(command=command, wait=wait, timeout=2*seconds)

    # seconds - Seconds to run workload
    # threads - Number of parallel threads in workload
    # rows - Number of rows to upsert
    def bulk_upsert(self, seconds: int, threads: int, rows: int, wait: bool = False):
        self._insert_rows(operation_name="bulk_upsert", seconds=seconds, threads=threads, rows=rows, wait=wait)

    def __del__(self):
        command: list[str] = self.begin_command + ["clean"]
        try:
            yatest.common.execute(command=command, wait=True)
        except Exception:
            pass


class TestLogScenario(object):
    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls, writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_immediate_writing_on_bulk_upsert": True,
                                 "enable_cs_overloads_subscription_retries": True},
                                #  "enable_topic_compactification_by_key": True},
            column_shard_config={"alter_object_enabled": True,
                                 "writing_in_flight_requests_count_limit": writing_in_flight_requests_count_limit,
                                 "writing_in_flight_request_bytes_limit": writing_in_flight_request_bytes_limit},
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
            self.ydb_client.query(f"SELECT COUNT(*) FROM `{self.table_name}` ")

    @pytest.mark.parametrize('writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit', [(1, 10000), (2, 10000), (1000, 1), (1000, 2), (1, 1), (2, 2)])
    def test_overloads(self, writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit):
        self._setup_ydb(writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit)
        wait_time: int = 60
        self.table_name: str = "log"

        output_path = yatest.common.test_output_path()
        output_stdout = open(os.path.join(output_path, "command_stdout.log"), "w")

        ydb_workload: YdbWorkloadOverload = YdbWorkloadOverload(
            endpoint=self.ydb_client.endpoint,
            database=self.ydb_client.database,
            table_name=self.table_name,
            stdout=output_stdout
        )
        ydb_workload.create_table(self.table_name)

        ydb_workload.bulk_upsert(seconds=wait_time, threads=1, rows=10, wait=True)

        output_stdout.close()
        keys = None
        values = None
        with open(os.path.join(output_path, "command_stdout.log"), "r") as file:
            for line in file:
                if line.startswith("Total"):
                    keys = line.split()
                elif keys is not None:
                    values = line.split()
                    break

        assert keys is not None and values is not None

        stats = dict(zip(keys, values))

        logging.info(f"Stats: {stats}")

        assert stats["Errors"] == "0"

        logging.info(f"Count rows after insert {self.get_row_count()}")
        assert self.get_row_count() != 0
