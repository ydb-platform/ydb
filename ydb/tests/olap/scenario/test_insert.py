import time
from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
)
from helpers.thread_helper import TestThread
from ydb import PrimitiveType
from typing import List, Dict, Any
from ydb.tests.olap.lib.utils import get_external_param, external_param_is_true


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
        sth = ScenarioTestHelper(ctx)
        table_name = "log" + table
        for batch in data:
            sth.bulk_upsert_data(table_name, self.schema_log, batch)

    def _loop_insert(self, ctx: TestContext, rows_count: int, table: str, ignore_read_errors: bool):
        sth = ScenarioTestHelper(ctx)
        log: str = sth.get_full_path("log" + table)
        cnt: str = sth.get_full_path("cnt" + table)
        for i in range(rows_count):
            for c in range(10):
                try:
                    sth.execute_query(
                        yql=f'$cnt = SELECT CAST(COUNT(*) AS INT64) from `{log}`; INSERT INTO `{cnt}` (key, c) values({i}, $cnt)', retries=20, fail_on_error=False
                    )
                    break
                except Exception:
                    if ignore_read_errors:
                        pass
                    else:
                        raise
                time.sleep(1)

    def scenario_read_data_during_bulk_upsert(self, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        cnt_table_name: str = "cnt"
        log_table_name: str = "log"
        batches_count = int(get_external_param("batches_count", "10"))
        rows_count = int(get_external_param("rows_count", "1000"))
        inserts_count = int(get_external_param("inserts_count", "200"))
        tables_count = int(get_external_param("tables_count", "1"))
        ignore_read_errors = external_param_is_true("ignore_read_errors")
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

        thread1 = []
        thread2 = []
        for table in range(tables_count):
            thread1.append(TestThread(target=self._loop_upsert, args=[ctx, data, str(table)]))
        for table in range(tables_count):
            thread2.append(TestThread(target=self._loop_insert, args=[ctx, inserts_count, str(table), ignore_read_errors]))

        for thread in thread1:
            thread.start()

        for thread in thread2:
            thread.start()

        for thread in thread2:
            thread.join()

        for thread in thread1:
            thread.join()

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
