from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
)

from ydb import PrimitiveType
from typing import List, Dict, Any
from ydb.tests.olap.lib.utils import get_external_param
import threading


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

    def _loop_upsert(self, ctx: TestContext, data: list):
        sth = ScenarioTestHelper(ctx)
        for batch in data:
            sth.bulk_upsert_data("log", self.schema_log, batch)

    def _loop_insert(self, ctx: TestContext, rows_count: int):
        sth = ScenarioTestHelper(ctx)
        log: str = sth.get_full_path("log")
        cnt: str = sth.get_full_path("cnt")
        for i in range(rows_count):
            sth.execute_query(
                f'$cnt = SELECT CAST(COUNT(*) AS INT64) from `{log}`; INSERT INTO `{cnt}` (key, c) values({i}, $cnt)'
            )

    def scenario_read_data_during_bulk_upsert(self, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        cnt_table_name: str = "cnt"
        log_table_name: str = "log"
        batches_count = int(get_external_param("batches_count", "10"))
        rows_count = int(get_external_param("rows_count", "1000"))
        inserts_count = int(get_external_param("inserts_count", "200"))
        sth.execute_scheme_query(
            CreateTable(cnt_table_name).with_schema(self.schema_cnt)
        )
        sth.execute_scheme_query(
            CreateTable(log_table_name).with_schema(self.schema_log)
        )
        data: List = []
        for i in range(batches_count):
            batch: List[Dict[str, Any]] = []
            for j in range(rows_count):
                batch.append({"key": j + rows_count * i})
            data.append(batch)

        thread1 = threading.Thread(target=self._loop_upsert, args=[ctx, data])
        thread2 = threading.Thread(target=self._loop_insert, args=[ctx, inserts_count])

        thread1.start()
        thread2.start()

        thread2.join()
        thread1.join()

        rows: int = sth.get_table_rows_count(cnt_table_name)
        assert rows == inserts_count
        scan_result = sth.execute_scan_query(
            f"SELECT key, c FROM `{sth.get_full_path(cnt_table_name)}` ORDER BY key"
        )
        for i in range(rows):
            if scan_result.result_set.rows[i]["key"] != i:
                assert False, f"{i} ?= {scan_result.result_set.rows[i]['key']}"

        rows: int = sth.get_table_rows_count(log_table_name)
        assert rows == rows_count * batches_count
        scan_result = sth.execute_scan_query(
            f"SELECT key FROM `{sth.get_full_path(log_table_name)}` ORDER BY key"
        )
        for i in range(rows):
            if scan_result.result_set.rows[i]["key"] != i:
                assert False, f"{i} ?= {scan_result.result_set.rows[i]['key']}"
