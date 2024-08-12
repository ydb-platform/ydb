import datetime
import itertools
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.utils.types.postgresql as postgresql
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
    SelectWhere,
    makeOptionalYdbTypeFromTypeID,
)

from ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common import TestCase


class Factory:
    def _primitive_types(self) -> Sequence[TestCase]:
        """
        Every data source has its own type system;
        we test datasource-specific types in the following test cases.
        """
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_01_bool',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.BOOL),
                    data_source_type=DataSourceType(pg=postgresql.Bool()),
                ),
                Column(
                    name='col_02_smallint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(pg=postgresql.SmallInt()),
                ),
                Column(
                    name='col_03_int2',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(pg=postgresql.Int2()),
                ),
                Column(
                    name='col_04_smallserial',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(pg=postgresql.SmallSerial()),
                ),
                Column(
                    name='col_05_serial2',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(pg=postgresql.Serial2()),
                ),
                Column(
                    name='col_06_integer',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(pg=postgresql.Integer()),
                ),
                Column(
                    name='col_07_int',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(pg=postgresql.Int()),
                ),
                Column(
                    name='col_08_int4',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(pg=postgresql.Int4()),
                ),
                Column(
                    name='col_09_serial',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(pg=postgresql.Serial()),
                ),
                Column(
                    name='col_10_serial4',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(pg=postgresql.Serial4()),
                ),
                Column(
                    name='col_11_bigint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(pg=postgresql.BigInt()),
                ),
                Column(
                    name='col_12_int8',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(pg=postgresql.Int8()),
                ),
                Column(
                    name='col_13_bigserial',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(pg=postgresql.BigSerial()),
                ),
                Column(
                    name='col_14_serial8',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(pg=postgresql.Serial8()),
                ),
                Column(
                    name='col_15_real',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.FLOAT),
                    data_source_type=DataSourceType(pg=postgresql.Real()),
                ),
                Column(
                    name='col_16_float4',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.FLOAT),
                    data_source_type=DataSourceType(pg=postgresql.Float4()),
                ),
                Column(
                    name='col_17_double_precision',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(pg=postgresql.DoublePrecision()),
                ),
                Column(
                    name='col_18_float8',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(pg=postgresql.Float8()),
                ),
                # TODO: check unicode strings
                Column(
                    name='col_19_bytea',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(pg=postgresql.Bytea()),
                ),
                Column(
                    name='col_20_character',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(pg=postgresql.Character()),
                ),
                Column(
                    name='col_21_character_varying',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(pg=postgresql.CharacterVarying()),
                ),
                Column(
                    name='col_22_text',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(pg=postgresql.Text()),
                ),
                Column(  # TODO: maybe refactor: in fq-connector-go col_23_timestamp, col_24_date
                    name='col_23_date',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(pg=postgresql.Date()),
                ),
                Column(
                    name='col_24_timestamp',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(pg=postgresql.TimestampWithoutTimeZone()),
                ),
                # TODO: YQ-2297
                # Column(
                #     name='col_25_time',
                #     ydb_type=?,
                #     data_source_type=DataSourceType(pg=postgresql.time),
                # ),
                # maybe col_26_time?
                Column(
                    name='col_27_json',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.JSON),
                    data_source_type=DataSourceType(pg=postgresql.Json()),
                ),
            )
        )

        tc = TestCase(
            name_='primitive_types',
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=[
                [
                    False,
                    2,
                    3,
                    1,
                    1,
                    6,
                    7,
                    8,
                    1,
                    1,
                    11,
                    12,
                    1,
                    1,
                    15.15,
                    16.16,
                    17.17,
                    18.18,
                    'az',
                    'az   ',
                    'az   ',
                    'az',
                    datetime.date(2023, 8, 9),
                    datetime.datetime(2023, 8, 9, 13, 19, 11),
                    '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}',
                    # TODO: support time in YQ-2297
                ],
                [
                    True,
                    -2,
                    -3,
                    2,
                    2,
                    -6,
                    -7,
                    -8,
                    1,
                    1,
                    -11,
                    -12,
                    2,
                    2,
                    -15.15,
                    -16.16,
                    -17.17,
                    -18.18,
                    'buki',
                    'buki ',
                    'buki ',
                    'buki',
                    datetime.date(1988, 11, 20),
                    datetime.datetime(1988, 11, 20, 12, 00, 00),
                    '{ "TODO" : "unicode" }',
                    # TODO: support time in YQ-2297
                ],
                [
                    None,
                    None,
                    None,
                    3,
                    3,
                    None,
                    None,
                    None,
                    3,
                    3,
                    None,
                    None,
                    3,
                    3,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            ],
            data_out_=None,
            data_source_kind=EDataSourceKind.POSTGRESQL,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [tc]

    def _upper_case_column(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT COL1 from a pg table.
        https://st.yandex-team.ru/YQ-2264
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='"COL1"',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(pg=postgresql.Integer()),
                ),
            )
        )

        test_case_name = 'upper_case_column'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat(SelectWhat.Item(name='COL1')),
            select_where=None,
            data_in=[
                [
                    3,
                ]
            ],
            data_out_=[
                [
                    3,
                ],
            ],
            data_source_kind=EDataSourceKind.POSTGRESQL,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
        )

        return [tc]

    def _constant(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT 42 from a pg table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col',
                    ydb_type=Type.INT64,
                    data_source_type=DataSourceType(pg=postgresql.Serial8()),
                ),
            )
        )

        test_case_name = 'constant'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat(SelectWhat.Item(name='42', kind='expr')),
            select_where=None,
            data_in=[
                [
                    1,
                ],
                [
                    2,
                ],
                [
                    3,
                ],
            ],
            data_out_=[
                [
                    42,
                ],
                [
                    42,
                ],
                [
                    42,
                ],
            ],
            data_source_kind=EDataSourceKind.POSTGRESQL,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
        )

        return [tc]

    def _count(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT COUNT(*) from a pg table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col',
                    ydb_type=Type.UTF8,
                    data_source_type=DataSourceType(pg=postgresql.Text()),
                ),
            )
        )

        test_case_name = 'count'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat(SelectWhat.Item(name='COUNT(*)', kind='expr')),
            select_where=None,
            data_in=[
                [
                    'first',
                ],
                [
                    'second',
                ],
                [
                    'third',
                ],
            ],
            data_out_=[
                [
                    3,
                ],
            ],
            data_source_kind=EDataSourceKind.POSTGRESQL,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
        )

        return [tc]

    def _pushdown(self) -> TestCase:
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

        test_case_name = 'pushdown'

        return [
            TestCase(
                name_=test_case_name,
                data_in=data_in,
                data_out_=data_out_1,
                protocol=EProtocol.NATIVE,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_string')),
                select_where=SelectWhere('col_int32 = 1'),
                data_source_kind=data_source_kind,
                schema=schema,
            ),
            TestCase(
                name_=test_case_name,
                data_in=data_in,
                data_out_=data_out_2,
                protocol=EProtocol.NATIVE,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_string')),
                select_where=SelectWhere('col_int32 = col_int64'),
                data_source_kind=data_source_kind,
                schema=schema,
            ),
        ]

    def _json(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_json',
                    ydb_type=Type.JSON,
                    data_source_type=DataSourceType(pg=postgresql.Json()),
                ),
            ),
        )

        data_in = [
            ['{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}'],
            ['{ "TODO" : "unicode" }'],
            [None],
        ]

        data_out_1 = [
            ['{"age":35,"name":"James Holden"}'],
            [None],
            [None],
        ]

        data_source_kind = EDataSourceKind.POSTGRESQL

        test_case_name = 'json'

        return [
            TestCase(
                name_=test_case_name,
                data_in=data_in,
                data_out_=data_out_1,
                protocol=EProtocol.NATIVE,
                select_what=SelectWhat(SelectWhat.Item(name='JSON_QUERY(col_json, "$.friends[0]")', kind='expr')),
                select_where=None,
                data_source_kind=data_source_kind,
                pragmas=dict(),
                schema=schema,
            ),
        ]

    def make_test_cases(self) -> Sequence[TestCase]:
        return list(
            itertools.chain(
                self._primitive_types(),
                self._upper_case_column(),
                self._constant(),
                self._count(),
                self._pushdown(),
                self._json(),
            )
        )
