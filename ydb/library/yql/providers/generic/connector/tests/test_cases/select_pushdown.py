import itertools
from typing import Sequence, TypeAlias

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.test_cases.select_positive as select_positive
import ydb.library.yql.providers.generic.connector.tests.utils.clickhouse as clickhouse
import ydb.library.yql.providers.generic.connector.tests.utils.postgresql as postgresql
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
    SelectWhere,
)

TestCase: TypeAlias = select_positive.TestCase


class Factory:
    _name = 'pushdown'

    def _make_tests_clickhouse(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_int32',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(ch=clickhouse.Int32()),
                ),
                Column(
                    name='col_string',
                    ydb_type=Type.UTF8,
                    data_source_type=DataSourceType(ch=clickhouse.String()),
                ),
            ),
        )

        data_in = [
            [
                1,
                'one',
            ],
            [
                2,
                'two',
            ],
            [
                3,
                'three',
            ],
        ]

        data_out = [
            ['one'],
        ]

        data_source_kind = EDataSourceKind.CLICKHOUSE

        return [
            TestCase(
                name=f'{self._name}_{data_source_kind}',
                data_in=data_in,
                data_out_=data_out,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_string')),
                select_where=SelectWhere('col_int32 = 1'),
                data_source_kind=data_source_kind,
                schema=schema,
                database=Database.make_for_data_source_kind(data_source_kind),
            )
        ]

    def _make_tests_postgresql(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_int32',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(pg=postgresql.Int4()),
                ),
                Column(
                    name='col_int64',
                    ydb_type=Type.INT64,
                    data_source_type=DataSourceType(pg=postgresql.Int8()),
                ),
                Column(
                    name='col_string',
                    ydb_type=Type.UTF8,
                    data_source_type=DataSourceType(pg=postgresql.Text()),
                ),
                Column(
                    name='col_float',
                    ydb_type=Type.FLOAT,
                    data_source_type=DataSourceType(pg=postgresql.Float4()),
                ),
            ),
        )

        data_in = [
            [1, 2, 'one', 1.1],
            [2, 2, 'two', 1.23456789],
            [3, 5, 'three', 0.00000012],
        ]

        data_out_1 = [
            ['one'],
        ]

        data_out_2 = [
            ['two'],
        ]

        data_source_kind = EDataSourceKind.POSTGRESQL

        return [
            TestCase(
                name=f'{self._name}_{data_source_kind}',
                data_in=data_in,
                data_out_=data_out_1,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_string')),
                select_where=SelectWhere('col_int32 = 1'),
                data_source_kind=data_source_kind,
                schema=schema,
                database=Database.make_for_data_source_kind(data_source_kind),
            ),
            TestCase(
                name=f'{self._name}_{data_source_kind}',
                data_in=data_in,
                data_out_=data_out_2,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_string')),
                select_where=SelectWhere('col_int32 = col_int64'),
                data_source_kind=data_source_kind,
                schema=schema,
                database=Database.make_for_data_source_kind(data_source_kind),
            ),
        ]

    def make_test_cases(self) -> Sequence[TestCase]:
        return list(
            itertools.chain(
                self._make_tests_clickhouse(),
                self._make_tests_postgresql(),
            )
        )
