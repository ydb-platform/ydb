import sys
import ipdb

import datetime
import os
import random

import logging
import time
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import ScenarioTestHelper
from ydb.tests.olap.scenario.conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import TestContext
import yatest.common

from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.thread_helper import TestThread
from ydb.tests.olap.helpers.ydb_client import YdbClient

from enum import Enum


logger = logging.getLogger(__name__)


class YdbWorkloadLog:
    def __init__(self, endpoint: str, database: str, table_name: str):
        self.path: str = yatest.common.binary_path(os.environ["YDB_CLI_BINARY"])
        self.endpoint: str = endpoint
        self.database: str = database
        self.begin_command: list[str] = [self.path, "-e", self.endpoint, "-d", self.database, "workload", "log", "--path", table_name]

    def _call(self, command: list[str], wait=False):
        logging.info(f'YdbWorkloadLog execute {' '.join(command)} with wait = {wait}')
        yatest.common.execute(command=command, wait=wait)

    def create_table(self, table_name: str):
        logging.info('YdbWorkloadLog init table')
        command = self.begin_command + ["init", "--path", table_name, "--store", "column"]
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
            "--timestamp_deviation",
            "180"
        ]
        self._call(command=command, wait=wait)

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


class TestLogScenario(BaseTestSet):
    class InsertMode(Enum):
        BULK_UPSERT = 1
        INSERT = 2
        UPSERT = 3

    @classmethod
    def setup_class(cls):
        super().setup_class()
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        cls.ydb_client = YdbClient(endpoint=cls._ydb_instance.endpoint(), database=f"/{cls._ydb_instance.database()}")
        cls.ydb_client.wait_connection()

    @classmethod
    def _get_cluster_config(cls):
        return KikimrConfigGenerator(
            extra_feature_flags={
                "enable_immediate_writing_on_bulk_upsert": True
            },
        )

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        super().teardown_class()

    # @classmethod
    # def teardown_class(cls):
    #     cls.ydb_client.stop()
    #     cls.cluster.stop()

    # @classmethod
    # def _setup_ydb(cls):
    #     ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
    #     logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
    #     config = KikimrConfigGenerator(
    #         extra_feature_flags={
    #             "enable_immediate_writing_on_bulk_upsert": True
    #         },
    #     )
    #     cls.cluster = KiKiMR(config)
    #     cls.cluster.start()
    #     node = cls.cluster.nodes[1]
    #     cls.ydb_client = YdbClient(endpoint=f"grpc://{node.host}:{node.port}", database=f"/{config.domain_name}")
    #     cls.ydb_client.wait_connection()

    def get_row_count(self) -> int:
        return self.ydb_client.query(f"select count(*) as Rows from `{self.table_name}`")[0].rows[0]["Rows"]

    def aggregation_query(self, duration: datetime.timedelta):
        deadline: datetime = datetime.datetime.now() + duration
        while datetime.datetime.now() < deadline:
            hours: int = random.randint(1, 10)
            self.ydb_client.query(f"SELECT COUNT(*) FROM `{self.table_name}` ")
            self.ydb_client.query(f"SELECT * FROM `{self.table_name}` WHERE timestamp < CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours})")
            self.ydb_client.query(f"SELECT COUNT(*) FROM `{self.table_name}` WHERE timestamp < CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours})")
            self.ydb_client.query(f"SELECT COUNT(*) FROM `{self.table_name}` WHERE "
                                  f"(timestamp >= CurrentUtcTimestamp() - DataTime::IntervalFromHours({hours}) - 1) AND "
                                  f"(timestamp <= CurrentUtcTimestamp() - DataTime::IntervalFromHours({hours}))")

    def check_insert(self, duration: int):
        prev_count: int = self.get_row_count()
        time.sleep(duration)
        current_count: int = self.get_row_count()
        logging.info(f'check insert: {current_count} {prev_count}')
        assert current_count != prev_count

    def scenario_test(self, ctx: TestContext):
        """As per https://github.com/ydb-platform/ydb/issues/13530"""

        wait_time: int = int(get_external_param("wait_minutes", "3")) * 60

        self.table_name: str = "log"
        logging.error(f"_ydb.instance.database: {self._ydb_instance.database()}")
        ydb_workload: YdbWorkloadLog = YdbWorkloadLog(endpoint=self.ydb_client.endpoint, database=self.ydb_client.database, table_name=self.table_name)
        
        # logging.error(f"_ydb.instance.database: {self._ydb_instance.database()}")
        logging.error(f"YdbCluster endpoint: {self.ydb_client.endpoint}, database: {self.ydb_client.database}")

        # yatest.common.execute(command="echo " + f"'YdbCluster endpoint: {self.ydb_client.endpoint}, database: {self.ydb_client.database}'" + ">> /home/emgariko/project/ydb_fork/ydb/ydb/tests/olap/scenario/logs.txt")

        print(f"YdbCluster endpoint: {self.ydb_client.endpoint}, database: {self.ydb_client.database}", file=sys.stderr)
        # assert false
        # ydb_workload: YdbWorkloadLog = YdbWorkloadLog(endpoint=self._ydb_instance.endpoint(), database=f"/{self._ydb_instance.database()}", table_name=self.table_name)
        ydb_workload.create_table(self.table_name)
        # TODO: using insert here will somehow make self.get_row_count() equal 0
        ydb_workload.bulk_upsert(seconds=60, threads=10, rows=1000, wait=True)
        # assert False
        
        # time.sleep(10000000)
        logging.info(f"Count rows after insert {self.get_row_count()} before wait")
        # ipdb.set_trace()
        assert self.get_row_count() != 0

        threads: list[TestThread] = []
        threads.append(TestThread(target=ydb_workload.bulk_upsert, args=[wait_time, 10, 1000, True]))
        threads.append(TestThread(target=ydb_workload.insert, args=[wait_time, 10, 1000, True]))
        threads.append(TestThread(target=ydb_workload.upsert, args=[wait_time, 10, 1000, True]))

        for _ in range(10):
            threads.append(TestThread(target=self.aggregation_query, args=[datetime.timedelta(seconds=int(wait_time))]))
        threads.append(TestThread(target=self.check_insert, args=[wait_time + 10]))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
