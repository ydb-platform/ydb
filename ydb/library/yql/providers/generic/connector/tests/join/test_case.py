import itertools
from dataclasses import dataclass
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.public.api.protos.ydb_value_pb2 import Type

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
import ydb.library.yql.providers.generic.connector.tests.utils.types.clickhouse as clickhouse
import ydb.library.yql.providers.generic.connector.tests.utils.types.postgresql as postgresql
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.utils.data_source_kind import data_source_kind_alias
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
)
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings


@dataclass
class Table:
    name: str
    schema: Schema
    data_in: Sequence

    @property
    def select_what(self) -> SelectWhat:
        return self.schema.select_every_column()


@dataclass
class DataSource:
    database: Database
    table: Table
    kind: EDataSourceKind.ValueType = EDataSourceKind.DATA_SOURCE_KIND_UNSPECIFIED

    @property
    def alias(self) -> str:
        return data_source_kind_alias(self.kind) + "_" + self.table.name

    @property
    def yql_qualified_name(self) -> str:
        return f'{self.table.name}'

    def yql_aliased_name(self, cluster_name: str) -> str:
        return f'{cluster_name}.{self.yql_qualified_name} AS {self.alias}'

    def __hash__(self) -> int:
        return hash(self.kind)


@dataclass
class TestCase:
    name: str
    data_sources: Sequence[DataSource]
    data_out: Sequence[Sequence]

    @property
    def yql_select_what(self) -> str:
        return ', '.join((ds.table.select_what.names_with_prefix(ds.alias) for ds in self.data_sources))

    @property
    def generic_settings(self) -> GenericSettings:
        clickhouse_clusters = set()
        postgresql_clusters = set()

        for data_source in self.data_sources:
            match data_source.kind:
                case EDataSourceKind.CLICKHOUSE:
                    clickhouse_clusters.add(
                        GenericSettings.ClickHouseCluster(database=data_source.database.name, protocol=EProtocol.NATIVE)
                    )

                case EDataSourceKind.POSTGRESQL:
                    postgresql_clusters.add(
                        GenericSettings.PostgreSQLCluster(database=data_source.database.name, schema=None)
                    )
                case _:
                    raise Exception(f'invalid data source: {data_source.kind}')
        return GenericSettings(
            clickhouse_clusters=clickhouse_clusters,
            postgresql_clusters=postgresql_clusters,
            date_time_format=EDateTimeFormat.YQL_FORMAT,
        )

    def make_sql(self, settings: Settings) -> str:
        dss = self.data_sources
        ds_head, ds_tail = dss[0], dss[1:]

        yql_script_parts = [
            '\nSELECT',
            self.yql_select_what,
            f'FROM {ds_head.yql_aliased_name(settings.get_cluster_name(ds_head.kind))} ',
            '\n'.join(
                f'JOIN {ds.yql_aliased_name(settings.get_cluster_name(ds.kind))} '
                + f'ON {ds_head.alias}.id = {ds.alias}.id'
                for ds in ds_tail
            ),
            f'ORDER BY {ds_head.alias}_id',
        ]
        yql_script = "\n".join(yql_script_parts)
        return yql_script


class InnerJoinTestCase(TestCase):
    def make_sql(self, settings: Settings) -> str:
        ch_cluster = settings.get_cluster_name(self.data_sources[0].kind)
        pg_cluster = settings.get_cluster_name(self.data_sources[1].kind)
        ch_table = self.data_sources[0].yql_qualified_name
        pg_table = self.data_sources[1].yql_qualified_name
        return (
            f'SELECT pg.* FROM {ch_cluster}.{ch_table} AS ch INNER JOIN {pg_cluster}.{pg_table} AS pg '
            'ON ch.id = pg.id WHERE ch.id < 2'
        )


class Factory:
    _id_column: Column = Column(
        name='id',
        ydb_type=Type.INT32,
        data_source_type=DataSourceType(ch=clickhouse.Int32(), pg=postgresql.Serial()),
    )

    _data_columns: Sequence[Column] = [
        Column(
            name='col1',
            ydb_type=Type.STRING,
            data_source_type=DataSourceType(ch=clickhouse.String(), pg=postgresql.Text()),
        ),
        Column(
            name='col2',
            ydb_type=Type.INT32,
            data_source_type=DataSourceType(ch=clickhouse.Int32(), pg=postgresql.Int4()),
        ),
    ]

    def __make_simple_test_cases(self) -> Sequence[TestCase]:
        tables: Sequence[TestCase] = [
            Table(
                name='example_1',
                schema=Schema(
                    columns=ColumnList(self._id_column, self._data_columns[0], self._data_columns[1]),
                ),
                data_in=[
                    [1, 'example_1_a', 10],
                    [2, 'example_1_b', 20],
                    [3, 'example_1_c', 30],
                    [4, 'example_1_d', 40],
                    [5, 'example_1_e', 50],
                ],
            ),
            Table(
                name='example_2',
                schema=Schema(
                    columns=ColumnList(self._id_column, self._data_columns[1], self._data_columns[0]),
                ),
                data_in=[
                    [1, 2, 'example_2_a'],
                    [2, 4, 'example_2_b'],
                    [3, 8, 'example_2_c'],
                    [4, 16, 'example_2_d'],
                    [5, 32, 'example_2_e'],
                ],
            ),
        ]

        data_out = list(map(lambda x: list(itertools.chain(*x)), zip(*(t.data_in for t in tables))))

        data_sources: Sequence[EDataSourceKind] = (
            EDataSourceKind.CLICKHOUSE,
            EDataSourceKind.POSTGRESQL,
        )

        # For each test case we create a unique set of datasources;
        # tables described above will be mapped to every particular set of data sources
        # in order to model federative requests, for example:
        #
        # TestCase(table_1 -> CH, table_2 -> PG)
        # TestCase(table_1 -> PG, table_2 -> CH)
        # TestCase(table_1 -> PG, table_2 -> PG)
        # TestCase(table_1 -> CH, table_2 -> CH)
        data_source_combinations: Sequence[Sequence[DataSource]] = set(
            itertools.chain(*map(itertools.permutations, itertools.combinations_with_replacement(data_sources, 2)))
        )

        test_cases = []

        for dsc in data_source_combinations:
            assert len(dsc) == len(tables)

            # generate test case name
            test_case_name = 'join_' + "_".join(data_source_kind_alias(data_source_kind) for data_source_kind in dsc)

            # inject tables into data sources
            test_case_data_sources = []
            for i, data_source_kind in enumerate(dsc):
                ds = DataSource(
                    kind=data_source_kind,
                    database=Database(name=test_case_name, kind=data_source_kind),
                    table=tables[i],
                )
                test_case_data_sources.append(ds)

            test_case = TestCase(
                name=test_case_name,
                data_sources=test_case_data_sources,
                data_out=data_out,
            )

            test_cases.append(test_case)

        return test_cases

    def __make_inner_join_test_case(self) -> Sequence[TestCase]:
        ch_table = Table(
            name='test_1',
            schema=Schema(
                columns=ColumnList(self._id_column),
            ),
            data_in=[
                [1],
                [2],
                [3],
                [4],
                [5],
            ],
        )
        pg_table = Table(
            name='test_1',
            schema=Schema(
                columns=ColumnList(self._id_column, self._data_columns[0], self._data_columns[1]),
            ),
            data_in=[
                [1, 'test_1_a', 10],
                [2, 'test_1_b', 20],
                [3, 'test_1_c', 30],
                [8, 'test_1_d', 80],
                [9, 'test_1_e', 90],
            ],
        )

        test_case_name = 'inner_join'

        test_case_data_sources = [
            DataSource(
                kind=EDataSourceKind.CLICKHOUSE,
                database=Database(kind=EDataSourceKind.CLICKHOUSE, name=test_case_name),
                table=ch_table,
            ),
            DataSource(
                kind=EDataSourceKind.POSTGRESQL,
                database=Database(kind=EDataSourceKind.POSTGRESQL, name=test_case_name),
                table=pg_table,
            ),
        ]
        data_out = [
            ['test_1_a', 10, 1],
        ]
        return [
            InnerJoinTestCase(
                name=test_case_name,
                data_sources=test_case_data_sources,
                data_out=data_out,
            )
        ]

    def make_test_cases(self) -> Sequence[TestCase]:
        return self.__make_simple_test_cases() + self.__make_inner_join_test_case()
