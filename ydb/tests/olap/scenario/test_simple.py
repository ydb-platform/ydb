from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    CreateTableStore,
    AlterTable,
    AlterTableStore,
)
from ydb import PrimitiveType, StatusCode
import ydb.tests.olap.scenario.helpers.data_generators as dg


class TestSimple(BaseTestSet):
    schema1 = (
        ScenarioTestHelper.Schema()
        .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
        .with_column(name='level', type=PrimitiveType.Uint32)
        .with_key_columns('id')
    )

    schema2 = (
        ScenarioTestHelper.Schema()
        .with_column(name='id', type=PrimitiveType.Int32, not_null=True)
        .with_column(name='not_level', type=PrimitiveType.Uint32)
        .with_key_columns('id')
    )

    def _test_table(self, ctx: TestContext, table_name: str):
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(CreateTable(table_name).with_schema(self.schema1))
        sth.bulk_upsert_data(
            table_name,
            self.schema1,
            [
                {'id': 1, 'level': 3},
                {'id': 2, 'level': None},
            ],
            comment="with ok scheme",
        )
        assert sth.get_table_rows_count(table_name) == 2

        sth.bulk_upsert_data(
            table_name,
            self.schema2,
            [
                {'id': 3, 'not_level': 3},
            ],
            StatusCode.SCHEME_ERROR,
            comment='with wrong scheme',
        )

        sth.bulk_upsert(table_name, dg.DataGeneratorPerColumn(self.schema1, 100), comment="100 sequetial ids")
        sth.bulk_upsert(
            table_name,
            dg.DataGeneratorPerColumn(self.schema1, 100, dg.ColumnValueGeneratorRandom()),
            comment="100 random rows",
        )

    def scenario_table(self, ctx: TestContext):
        self._test_table(ctx, 'testTable')

    def scenario_tablestores(self, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(CreateTableStore('testStore').with_schema(self.schema1))
        self._test_table(ctx, 'testStore/testTable')

    def scenario_alter_table(self, ctx: TestContext):
        table_name = 'testTable'
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(CreateTable(table_name).with_schema(self.schema1))
        sth.bulk_upsert(
            table_name,
            dg.DataGeneratorPerColumn(self.schema1, 10).with_column('level', dg.ColumnValueGeneratorRandom()),
            comment="old schema",
        )
        assert sth.get_table_rows_count(table_name) == 10
        sth.execute_scheme_query(
            AlterTable(table_name).add_column(sth.Column('not_level', PrimitiveType.Uint32)).drop_column('level')
        )
        assert sth.get_table_rows_count(table_name) == 10
        sth.bulk_upsert(
            table_name,
            dg.DataGeneratorPerColumn(self.schema2, 10, dg.ColumnValueGeneratorDefault(init_value=10)).with_column(
                'not_level', dg.ColumnValueGeneratorRandom()
            ),
            comment="new schema",
        )
        assert sth.get_table_rows_count(table_name) == 20

    def scenario_alter_tablestore(self, ctx: TestContext):
        table_name = 'testStore/testTable'
        sth = ScenarioTestHelper(ctx)
        sth.execute_scheme_query(CreateTableStore('testStore').with_schema(self.schema1))
        sth.execute_scheme_query(CreateTable(table_name).with_schema(self.schema1))
        sth.bulk_upsert(
            table_name,
            dg.DataGeneratorPerColumn(self.schema1, 10).with_column('level', dg.ColumnValueGeneratorRandom()),
            comment="old schema",
        )
        assert sth.get_table_rows_count(table_name) == 10
        sth.execute_scheme_query(
            AlterTableStore('testStore').add_column(sth.Column('not_level', PrimitiveType.Uint32)).drop_column('level')
        )
        #        sth.execute_scheme_query(
        #            AlterTableStore('testStoreAlter').drop_column('level')
        #        )
        assert sth.get_table_rows_count(table_name) == 10
        sth.bulk_upsert(
            table_name,
            dg.DataGeneratorPerColumn(self.schema2, 10, dg.ColumnValueGeneratorDefault(init_value=10)).with_column(
                'not_level', dg.ColumnValueGeneratorRandom()
            ),
            comment="new schema",
        )
        assert sth.get_table_rows_count(table_name) == 20
