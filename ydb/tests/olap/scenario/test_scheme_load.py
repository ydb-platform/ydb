from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    CreateTableStore,
    AlterTable,
    AlterTableStore,
    DropTable,
)
from ydb import PrimitiveType
from ydb.tests.olap.common.thread_helper import TestThread, TestThreads
import allure
import uuid
import random


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
            sth.execute_scheme_query(DropTable(f'store/{prefix}_{i}'), retries=5)

    def scenario_add_and_drop_columns(self, ctx: TestContext):
        ops_count = 100
        max_columns_count = 100

        store_name = 'testStore'
        table_name = 'testStore/testTable'

        sth = ScenarioTestHelper(ctx)
        added_columns = set()

        def generate_column_name() -> str:
            return str(uuid.uuid1())

        def add_column():
            new_column = generate_column_name()
            while new_column in added_columns:
                new_column = generate_column_name()

            sth.execute_scheme_query(
                AlterTableStore(store_name).add_column(sth.Column(new_column, PrimitiveType.Uint32))
            )
            added_columns.add(new_column)

        def drop_column():
            if not added_columns:
                return

            column_to_drop = random.choice(tuple(added_columns))

            sth.execute_scheme_query(
                AlterTableStore(store_name).drop_column(sth.Column(column_to_drop))
            )
            added_columns.remove(column_to_drop)

        sth.execute_scheme_query(CreateTableStore(store_name).with_schema(self.schema1))
        sth.execute_scheme_query(CreateTable(table_name).with_schema(self.schema1))

        for _ in range(ops_count):
            p = len(added_columns) / max_columns_count

            if (random.random() < p):
                drop_column()
            else:
                add_column()

        # TODO check final columns count
        # assert <get schema and count columns> == len(added_columns) + 2

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
