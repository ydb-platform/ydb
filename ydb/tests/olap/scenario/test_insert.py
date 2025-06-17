import time
from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
)
from ydb.tests.olap.common.thread_helper import TestThread, TestThreads
from ydb import PrimitiveType
from typing import List, Dict, Any
from ydb.tests.olap.lib.utils import get_external_param

import random
import logging
import threading
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

    def _loop_insert(self, ctx: TestContext, rows_count: int, table: str, err_event: threading.Event):
        min_time = 1.0 / rows_count
        max_time = min_time * 10

        sth = ScenarioTestHelper(ctx)
        log: str = sth.get_full_path("log" + table)
        cnt: str = sth.get_full_path("cnt" + table)
        ignore_error: tuple[str] = (
            "Conflict with existing key",
            "Transaction locks invalidated"
        )
        for i in range(rows_count):
            if err_event.is_set():
                break

            logger.info("Insert")
            conflicting_keys_position = 10
            transaction_locks_position = 10
            for c in range(10):
                try:
                    result = sth.execute_query(
                        yql=f'$cnt = SELECT CAST(COUNT(*) AS INT64) from `{log}`; INSERT INTO `{cnt}` (key, c) values({i}, $cnt)',
                        retries=0, fail_on_error=False, return_error=True, ignore_error=ignore_error,
                    )

                    if isinstance(result, tuple):
                        error = str(result[1])
                        if "Conflict with existing key" in error:
                            conflicting_keys_position = min(conflicting_keys_position, c)
                            err_event.set()
                        elif "Transaction locks invalidated" in error:
                            transaction_locks_position = min(transaction_locks_position, c)
                            err_event.set()
                        else:
                            if c >= 9:
                                raise Exception(f'Query failed {error}')
                            else:
                                time.sleep(1)
                                continue
                        if conflicting_keys_position < transaction_locks_position:
                            raise Exception(f'Insert failed table {table}: {error}')
                    else:
                        if result == 1:
                            if c >= 9:
                                raise Exception('Insert failed table {}'.format(table))
                            else:
                                time.sleep(1)
                                continue
                    break
                except Exception:
                    if c >= 9:
                        raise
                time.sleep(random.uniform(min_time, max_time))

    def scenario_read_data_during_bulk_upsert(self, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        cnt_table_name: str = "cnt"
        log_table_name: str = "log"
        batches_count = int(get_external_param("batches_count", "10"))
        rows_count = int(get_external_param("rows_count", "1000"))
        inserts_count = int(get_external_param("inserts_count", str(self.def_inserts_count)))
        tables_count = int(get_external_param("tables_count", "1"))
        err_event = threading.Event()
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
            thread2.append(TestThread(target=self._loop_insert, args=[ctx, inserts_count, str(table), err_event]))
        thread1.start_all()
        thread2.start_all()

        thread2.join_all()
        thread1.join_all()

        if err_event.is_set():
            logger.info("Acceptable error was detected")
            return

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
