import datetime
import os
import random

import logging
import shutil
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
from pathlib import Path

# logging.basicConfig(
#     stream=sys.stderr,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S',
#     level=logging.INFO
# )

LOG_DIR = '/home/emgariko/project/logs/LostPlanStep/test_output_'

def copy_test_results(run_id):
    source_dir = '/home/emgariko/project/ydb_fork/ydb/ydb/tests/olap/restarts/test-results'
    dest_dir = f'/home/emgariko/project/logs/LostPlanStep/test_results_/test_results_{run_id}'
    
    try:
        Path(dest_dir).parent.mkdir(parents=True, exist_ok=True)
        
        if os.path.exists(source_dir):
            shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
            return True
    except Exception as e:
        print(f"Failed to copy test results: {e}")
        return False

def get_next_log_id(log_dir=LOG_DIR):
    Path(log_dir).mkdir(exist_ok=True)

    existing_logs = [f for f in os.listdir(log_dir) if f.startswith("out_")]
    
    if not existing_logs:
        return 1
    
    ids = [int(f.split('_')[1].split('.')[0]) for f in existing_logs]
    return max(ids) + 1

def setup_logger(name=__name__, log_dir=LOG_DIR):
    logger = logging.getLogger(name)
    # handler = logging.StreamHandler(sys.stderr)
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # handler.setFormatter(formatter)
    # logger.addHandler(handler)

    # logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    next_id = get_next_log_id(log_dir)
    log_file = f"{log_dir}/out_{next_id}.log"
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    logger.handlers.clear()
    logger.addHandler(file_handler)

    if copy_test_results(next_id):
        logger.info(f"Test results copied to test_results_{next_id}")
    else:
        logger.warning("Failed to copy test results")
    
    return logger

logger = setup_logger()

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
        logger.info(f"In progress: starting altering the table for thread#{thread_id}")
        deadline: datetime = datetime.datetime.now() + datetime.timedelta(seconds=30)
        while datetime.datetime.now() < deadline:
            ttl = random.randint(0, 1000)
            logger.info("In progress: sending alter query")
            try:
                self.ydb_client.query(f"""
                    ALTER TABLE `my_table` SET(TTL = Interval("PT{ttl}H") ON timestamp)
                    """)
            except Exception as x:
                logger.error(f"In progress: Caught an exception during query executing: {x}")
            except:
                logger.error(f"In progress: Caught an unknown exception during node killing executing")
            logger.info("In progress: executed alter query")

        logger.info(f"In progress: finished altering for thread#{thread_id}")

    def kill_nodes(self):
        logger.info(f"In progress: starting killing nodes")
        deadline: datetime = datetime.datetime.now() + datetime.timedelta(seconds=90)
        while datetime.datetime.now() < deadline:
            nodes = list(self.cluster.nodes.items())
            random.shuffle(nodes)
            nodes = nodes[0: len(nodes) // 2]
            for key, node in nodes:
                try:
                    logger.info(f"In progress: killing {key}-node")
                    node.kill()
                    time.sleep(random.randint(1, 10))
                    logger.info(f"In progress: killed {key}-node, starting it")
                    node.start()
                    time.sleep(random.randint(1, 10))
                    logger.info(f"In progress: started {key}-node")
                except Exception as x:
                    logger.error(f"In progress: Caught an exception during node killing executing: {x}")
                except:
                    logger.error(f"In progress: Caught an unknown exception during node killing executing")
        logger.info(f"In progress: finished killing nodes")
                

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

        logger.info("Init: creating a table")
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
        logger.info("Init: sucessfully created the table")

        threads: list[TestThread] = []
        for i in range(10):
            threads.append(TestThread(target=self.alter_table, args=[i]))
        threads.append(TestThread(target=self.kill_nodes))

        logger.info("In progress: starting 11 threads")
        for thread in threads:
            thread.start()
        logger.info("In progress: started 11 threads, now joining last")
        # for thread in threads:
        #     thread.join()
        threads[-1].join()
        logger.info("In progress: joined killing thread")

        logger.info("In progress: starting nodes again")
        # TODO: investigate, maybe here should be no repeating start() calls
        for node in self.cluster.nodes.values():
            node.start()
        logger.info("In progress: stared nodes again")

        logger.info("In progress: starting altering table")
        deadline: datetime = datetime.datetime.now() + datetime.timedelta(seconds=30)

        # проверить что ни одного успешного прогона не было.
        # Поставить на ночь крутится.
        # Дальше надо воспроизвести на store.
        # В тесте поискать как создавать таблицу в ColumnStore.
        
        all_failed = True

        while datetime.datetime.now() < deadline:
            hasException = False
            ttl = random.randint(1, 1000)
            logger.info("In progress: sending alter query")
            try:
                self.ydb_client.query(f"""
                    ALTER TABLE `my_table` SET(TTL = Interval("PT{ttl}H") ON timestamp)
                    """)
            except Exception as x:
                logger.error(f"In progress: Caught an exception during query executing: {x}")
                hasException = True
            except:
                logger.error(f"In progress: Caught an unknown exception during node killing executing")
                hasException = True
            finally:
                if not hasException:
                    all_failed = False
            logger.info("In progress: executed alter query")
            
        if all_failed:
            logger.error("Finished: All alter queries in last section failed")

        logger.info("Finished: finished altering table")
