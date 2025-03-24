import datetime
import os
import random

import logging
import time
from ydb.tests.library.common.types import Erasure
import yatest.common

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.thread_helper import TestThread
from ydb.tests.olap.helpers.ydb_client import YdbClient

from enum import Enum

logger = logging.getLogger(__name__)

class TestRestartNodes(object):
    @classmethod
    def setup_class(cls):
        # nodes_count = 8 if cls.erasure == Erasure.BLOCK_4_2 else 9
        nodes_count = 8
        configurator = KikimrConfigGenerator(None,
                                             nodes=nodes_count,
                                             use_in_memory_pdisks=False,
                                             additional_log_configs={'CMS': LogLevels.DEBUG},
                                             )
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(endpoint=f"grpc://{node.host}:{node.port}", database=f"/{configurator.domain_name}")
        cls.ydb_client.wait_connection()
        time.sleep(120)
        

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()


    def get_row_count(self) -> int:
        return self.ydb_client.query(f"select count(*) as Rows from `{self.table_name}`")[0].rows[0]["Rows"]

    def aggregation_query(self, duration: datetime.timedelta):
        deadline: datetime = datetime.datetime.now() + duration
        while datetime.datetime.now() < deadline:
            hours: int = random.randint(1, 10)
            self.ydb_client.query(f"SELECT COUNT(*) FROM `{self.table_name}` ")
            self.ydb_client.query(f"SELECT * FROM `{self.table_name}` WHERE timestamp < CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours})")
            # TODO: this queries somehow make db fallen. Investigate
            # self.ydb_client.query(f"SELECT COUNT(*) FROM `{self.table_name}` WHERE timestamp < CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours})")
            # self.ydb_client.query(f"SELECT COUNT(*) FROM `{self.table_name}` WHERE " + 
                                #   f"(timestamp >= CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours + 1})) AND " + 
                                #   f"(timestamp <= CurrentUtcTimestamp() - DateTime::IntervalFromHours({hours}))")

    def check_insert(self, duration: int):
        prev_count: int = self.get_row_count()
        time.sleep(duration)
        current_count: int = self.get_row_count()
        logging.info(f'check insert: {current_count} {prev_count}')
        assert current_count != prev_count

    def create_table(self, thread_id: int):
        deadline: datetime = datetime.datetime.now() + datetime.timedelta(minutes=3)
        while datetime.datetime.now() < deadline:
            tableName = f"table_{thread_id}_{datetime.timestamp()}"
            self.ydb_client.query(f"""
                CREATE TABLE `{tableName}` (
                    `Key` Uint64 NOT NULL,
                    `Value1` String,
                    PRIMARY KEY (`Key`)
                )
                TABLESTORE `tableStoreName`;
            """)
            self.ydb_client.query(f"""
                ALTER TABLE `{tableName}`
                ADD COLUMN `Value2` String;
            """)

    def test(self):
        self.ydb_client.query("""
            --!syntax_v1
            CREATE TABLESTORE `TableStore` (
                Key Uint64 NOT NULL,
                Value1 String,
                PRIMARY KEY (Key)
            )
            WITH (
                STORE = COLUMN
            );
        """)

        threads: list[TestThread] = []
        # threads.append(TestThread(target=ydb_workload.bulk_upsert, args=[wait_time, 10, 1000, True]))
        for i in range(10):
            threads.append(TestThread(target=self.create_table, args=[i]))

        for thread in threads:
            thread.start()

        time.sleep(10)

        for node in self.cluster.nodes:
            node.stop()
            # киляются с killom.
            # тут уточнить, как это делать.

        for node in self.cluster.nodes:
            node.start()

        for thread in threads:
            thread.join()

        