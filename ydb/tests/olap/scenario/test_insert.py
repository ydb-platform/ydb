import time
from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
)
from ydb.tests.olap.common.thread_helper import TestThread, TestThreads
from ydb import PrimitiveType, issues
from typing import List, Dict, Any
from ydb.tests.olap.lib.utils import get_external_param

import random
import logging
logger = logging.getLogger(__name__)


class TestInsert(BaseTestSet):
    schema_cnt = (
        ScenarioTestHelper.Schema()
        .with_column(name="key", type=PrimitiveType.Int32, not_null=True)
        .with_column(name="c", type=PrimitiveType.Int64)
        .with_key_columns("key")
    )

    schema_log = (
        ScenarioTestHelper.Schema()
        .with_column(name="key", type=PrimitiveType.Int32, not_null=True)
        .with_key_columns("key")
    )

    def _loop_upsert(self, ctx: TestContext, data: list, table: str):
        min_time = 10.0 / len(data)
        max_time = min_time * 10

        sth = ScenarioTestHelper(ctx)
        table_name = "log" + table
        for batch in data:
            sth.bulk_upsert_data(table_name, self.schema_log, batch)
            logger.info("Upsert")
            time.sleep(random.uniform(min_time, max_time))

    def _loop_insert(self, ctx: TestContext, rows_count: int, table: str):
        min_time = 1.0 / rows_count
        max_time = min_time * 10

        sth = ScenarioTestHelper(ctx)
        log: str = sth.get_full_path("log" + table)
        cnt: str = sth.get_full_path("cnt" + table)
        ignore_error: Set[str] = {
            "Transaction locks invalidated"
        }
        for iteration in range(rows_count):
            for attempt in range(10):
                logger.info(f'Inserting data to {cnt}, iteration {iteration}, attempt {attempt}...')
                result = sth.execute_query(
                    yql=f'$cnt = SELECT CAST(COUNT(*) AS INT64) from `{log}`; INSERT INTO `{cnt}` (key, c) values({iteration}, $cnt)',
                    retries=0, fail_on_error=False, return_error=True, ignore_error=ignore_error,
                )
                if result == []:  # query succeeded
                    logger.info(f'Inserting data to {cnt}, iteration {iteration}, attempt {attempt}... succeeded')
                    break
                elif isinstance(result, issues.Error):
                    if not any(e in str(result) for e in ignore_error):
                        raise Exception(f'unexpected error: {result}')
                    logger.info(f'Inserting data to {cnt}, iteration {iteration}, attempt {attempt}... got acceptable error: "{result}", trying again')
                else:
                    logger.info(f'Inserting data to {cnt}, iteration {iteration}, attempt {attempt}... completed with unexpected result: {type(result)} {result}')
                    raise Exception(f'unexpected result: {type(result)} {result}')
                sleep_time = random.uniform(min_time, max_time)
                logger.info(f'Going to sleep for {sleep_time} seconds')
                time.sleep(sleep_time)

    def scenario_read_data_during_bulk_upsert(self, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        cnt_table_name: str = "cnt"
        log_table_name: str = "log"
        batches_count = int(get_external_param("batches_count", "10"))
        rows_count = int(get_external_param("rows_count", "1000"))
        inserts_count = int(get_external_param("inserts_count", str(self.def_inserts_count)))
        tables_count = int(get_external_param("tables_count", "1"))
        for table in range(tables_count):
            sth.execute_scheme_query(
                CreateTable(cnt_table_name + str(table)).with_schema(self.schema_cnt)
            )
        for table in range(tables_count):
            sth.execute_scheme_query(
                CreateTable(log_table_name + str(table)).with_schema(self.schema_log)
            )
        data: List = []
        for i in range(batches_count):
            batch: List[Dict[str, Any]] = []
            for j in range(rows_count):
                batch.append({"key": j + rows_count * i})
            data.append(batch)

        thread1: TestThreads = TestThreads()
        thread2: TestThreads = TestThreads()
        for table in range(tables_count):
            thread1.append(TestThread(target=self._loop_upsert, args=[ctx, data, str(table)]))
        for table in range(tables_count):
            thread2.append(TestThread(target=self._loop_insert, args=[ctx, inserts_count, str(table)]))
        thread1.start_all()
        thread2.start_all()

        thread2.join_all()
        thread1.join_all()

        for table in range(tables_count):
            cnt_table_name0 = cnt_table_name + str(table)
            rows: int = sth.get_table_rows_count(cnt_table_name0)
            assert rows == inserts_count
            scan_result = sth.execute_scan_query(
                f"SELECT key, c FROM `{sth.get_full_path(cnt_table_name0)}` ORDER BY key"
            )
            for i in range(rows):
                if scan_result.result_set.rows[i]["key"] != i:
                    assert False, f"{i} ?= {scan_result.result_set.rows[i]['key']}"

            log_table_name0 = log_table_name + str(table)
            rows: int = sth.get_table_rows_count(log_table_name0)
            assert rows == rows_count * batches_count
            scan_result = sth.execute_scan_query(
                f"SELECT key FROM `{sth.get_full_path(log_table_name0)}` ORDER BY key"
            )
            for i in range(rows):
                if scan_result.result_set.rows[i]["key"] != i:
                    assert False, f"{i} ?= {scan_result.result_set.rows[i]['key']}"
