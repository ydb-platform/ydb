from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    DropTable,
)

from ydb import PrimitiveType
from typing import List, Dict, Any
import threading

class TestInsert(BaseTestSet):
    schema_cnt = (
        ScenarioTestHelper.Schema()
        .with_column(name='key', type=PrimitiveType.Int32, not_null=True)
        .with_column(name='c', type=PrimitiveType.Int64)
        .with_key_columns('key')
    )

    schema_log = (
        ScenarioTestHelper.Schema()
        .with_column(name='key', type=PrimitiveType.Int32, not_null=True)
        .with_key_columns('key')
    )

    def _loop_upsert(self, ctx: TestContext, data: list):
        sth = ScenarioTestHelper(ctx)
        for batch in data:
            sth.bulk_upsert_data("log", self.schema_log, batch)

    def _loop_insert(self, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        for i in range(100):
            sth.execute_query(f"$cnt = SELECT CAST(COUNT(*) AS INT64) from `{sth.get_full_path("log")}`; INSERT INTO `{sth.get_full_path("cnt") }` (key, c) values({i}, $cnt)")

    def scenario_read_data_during_bulk_upsert(self, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        cnt_table_name: str = "cnt"
        log_table_name: str = "log"
        sth.execute_scheme_query(CreateTable(cnt_table_name).with_schema(self.schema_cnt))
        sth.execute_scheme_query(CreateTable(log_table_name).with_schema(self.schema_log))
        
        data: list = []
        for i in range(100):
            batch: List[Dict[str, Any]] = []
            for j in range(i, 1000):
                batch.append({'key' : j})
            data.append(batch)

        thread1 = threading.Thread(target=self._loop_upsert, args=[ctx, data])
        thread2 = threading.Thread(target=self._loop_insert, args=[ctx])

        thread1.start()
        thread2.start()

        thread2.join()
        thread1.join()

        sth.execute_scheme_query(DropTable(cnt_table_name))
        sth.execute_scheme_query(DropTable(log_table_name))
