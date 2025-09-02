from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    CreateTableStore,
    DropTable,
)
from ydb import PrimitiveType
from ydb.tests.olap.common.thread_helper import TestThread, TestThreads
import allure


class TestSchemeLoad(BaseTestSet):
    schema1 = (
        ScenarioTestHelper.Schema()
        .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
        .with_column(name='level', type=PrimitiveType.Uint32)
        .with_key_columns('id')
    )

    def _create_tables(self, prefix: str, count: int, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        for i in range(count):
            sth.execute_scheme_query(CreateTable(f'store/{prefix}_{i}').with_schema(self.schema1))

    def _drop_tables(self, prefix: str, count: int, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        for i in range(count):
            sth.execute_scheme_query(DropTable(f'store/{prefix}_{i}'))

    def scenario_create_and_drop_tables(self, ctx: TestContext):
        tables_count = 100
        threads_count = 20

        ScenarioTestHelper(ctx).execute_scheme_query(CreateTableStore('store').with_schema(self.schema1))
        with allure.step('Create tables'):
            threads: TestThreads = TestThreads()
            for t in range(threads_count):
                index: int = threads.append(
                    TestThread(target=self._create_tables, args=[str(t), int(tables_count / threads_count), ctx])
                )
                threads.start_thread(index)
            threads.join_all()

        with allure.step('Drop tables'):
            threads: TestThreads = TestThreads()
            for t in range(threads_count):
                index: int = threads.append(
                    TestThread(target=self._drop_tables, args=[str(t), int(tables_count / threads_count), ctx])
                )
                threads.start_thread(index)
            threads.join_all()
