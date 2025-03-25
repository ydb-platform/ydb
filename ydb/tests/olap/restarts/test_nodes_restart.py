import datetime
import os
import random

import logging
import sys
import time
from ydb.tests.library.common.types import Erasure
import yatest.common
import random

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
                                             extra_feature_flags=[
                                                'enable_column_store'
                                             ]
                                            )
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(endpoint=f"grpc://{node.host}:{node.port}", database=f"/{configurator.domain_name}")
        cls.ydb_client.wait_connection()
        time.sleep(10)
        # TODO: increase sleep value
        

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    def alter_table(self, thread_id: int):
        print(f"In progress: starting altering the table for thread#{thread_id}\n", file=sys.stderr)
        deadline: datetime = datetime.datetime.now() + datetime.timedelta(minutes=2)
        while datetime.datetime.now() < deadline:
            print("In progress: sending alter query\n", file=sys.stderr)
            self.ydb_client.query(f"""
                ALTER TABLE `my_table` SET(TTL = Interval("PT48H") ON timestamp)
                """)
            print("In progress: executed alter query\n", file=sys.stderr)

        print(f"In progress: finished altering for thread#{thread_id}\n", file=sys.stderr)

    def kill_nodes(self):
        print(f"In progress: starting killing nodes\n", file=sys.stderr)
        deadline: datetime = datetime.datetime.now() + datetime.timedelta(minutes=2)
        while datetime.datetime.now() < deadline:
            nodes = list(self.cluster.nodes.items())
            random.shuffle(nodes)
            for key, node in nodes:
                print(f"In progress: killing {key}-node\n", file=sys.stderr)
                node.kill()
                print(f"In progress: killed {key}-node, starting it\n", file=sys.stderr)
                node.start()
                print(f"In progress: started {key}-node\n", file=sys.stderr)
        print(f"In progress: finished killing nodes\n", file=sys.stderr)
                

    def test(self):
        # self.ydb_client.query("""
        #     --!syntax_v1
            # CREATE TABLESTORE `TableStore` (
            #     Key Uint64 NOT NULL,
            #     Value1 String,
            #     PRIMARY KEY (Key)
            # )
            # WITH (
            #     STORE = COLUMN,
            #     AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            # );
        # """)

        # Ошибка должна быть как в paste.
        # Нужно думать про это как про то что у пользователя не работает alter.
        # Сценарий пользователя: летят alter-ы, рестартуют ноды, затем ничего не работает.
        # 1) создать одну таблицу.
        # 2) альтеры в одну таблицу и одновременно случайно убиваем произвольные ноды в течение 2х минут . из нескольких потоков.
        # 3) поднимаю все ноды. 
        # 4) начинаю гонять 1 альтер.
        # 5) в течение 2х минут альтер должен начать успешно проходить. крутить в цикле.

        print("Init: creating a table\n", file=sys.stderr)
        self.ydb_client.query(f"""
                CREATE TABLE `my_table` (
                    timestamp Timestamp NOT NULL, 
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (timestamp)
                ) WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5
                );
            """)
        print("Init: sucessfully created the table\n", file=sys.stderr)

        threads: list[TestThread] = []
        for i in range(10):
            threads.append(TestThread(target=self.alter_table, args=[i]))
        threads.append(TestThread(target=self.kill_nodes))

        print("In progress: starting 11 threads\n", file=sys.stderr)
        for thread in threads:
            thread.start()
        print("In progress: started 11 threads, now joining them\n", file=sys.stderr)
        for thread in threads:
            thread.join()
        print("In progress: joined 11 threads\n", file=sys.stderr)

        print("In progress: starting nodes again\n", file=sys.stderr)
        # TODO: investigate, maybe here should be no repeating start() calls
        for node in self.cluster.nodes.values():
            node.start()
        print("In progress: stared nodes again\n", file=sys.stderr)

        print("In progress: starting altering table\n", file=sys.stderr)
        deadline: datetime = datetime.datetime.now() + datetime.timedelta(minutes=2)
        while datetime.datetime.now() < deadline:
            print("In progress: sending alter query\n", file=sys.stderr)
            self.ydb_client.query(f"""
                ALTER TABLE `my_table` SET(TTL = Interval("PT49H") ON timestamp)
                """)
            print("In progress: executed alter query\n", file=sys.stderr)
            
        print("Finished: finished altering table\n", file=sys.stderr)
