import datetime
import itertools
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.utils.types.ydb as types_ydb
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
    SelectWhere,
    makeYdbTypeFromTypeID,
    makeOptionalYdbTypeFromTypeID,
)

from ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common import TestCase


class Factory:
    def _primitive_types(self) -> Sequence[TestCase]:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32().to_non_nullable()),
                ),
                Column(
                    name='col_01_boolean',
                    ydb_type=makeYdbTypeFromTypeID(Type.BOOL),
                    data_source_type=DataSourceType(ydb=types_ydb.Bool().to_non_nullable()),
                ),
                Column(
                    name='col_02_int8',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT8),
                    data_source_type=DataSourceType(ydb=types_ydb.Int8().to_non_nullable()),
                ),
                Column(
                    name='col_03_uint8',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT8),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint8().to_non_nullable()),
                ),
                Column(
                    name='col_04_int16',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(ydb=types_ydb.Int16().to_non_nullable()),
                ),
                Column(
                    name='col_05_uint16',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT16),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint16().to_non_nullable()),
                ),
                Column(
                    name='col_06_int32',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32().to_non_nullable()),
                ),
                Column(
                    name='col_07_uint32',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint32().to_non_nullable()),
                ),
                Column(
                    name='col_08_int64',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ydb=types_ydb.Int64().to_non_nullable()),
                ),
                Column(
                    name='col_09_uint64',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT64),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint64().to_non_nullable()),
                ),
                Column(
                    name='col_10_float',
                    ydb_type=makeYdbTypeFromTypeID(Type.FLOAT),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint64().to_non_nullable()),
                ),
                Column(
                    name='col_11_double',
                    ydb_type=makeYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(ydb=types_ydb.Double().to_non_nullable()),
                ),
                Column(
                    name='col_12_string',
                    ydb_type=makeYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ydb=types_ydb.String().to_non_nullable()),
                ),
                Column(
                    name='col_13_utf8',
                    ydb_type=makeYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ydb=types_ydb.Utf8().to_non_nullable()),
                ),
                Column(
                    name='col_14_date',
                    ydb_type=makeYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(ydb=types_ydb.Date().to_non_nullable()),
                ),
                Column(
                    name='col_15_datetime',
                    ydb_type=makeYdbTypeFromTypeID(Type.DATETIME),
                    data_source_type=DataSourceType(ydb=types_ydb.Datetime().to_non_nullable()),
                ),
                Column(
                    name='col_16_datetime',
                    ydb_type=makeYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ydb=types_ydb.Timestamp().to_non_nullable()),
                ),
                Column(
                    name='col_17_json',
                    ydb_type=makeYdbTypeFromTypeID(Type.JSON),
                    data_source_type=DataSourceType(ydb=types_ydb.Json().to_non_nullable()),
                ),
            ),
        )

        test_case_name = 'primitive_types'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=None,
            data_out_=[
                [
                    1,
                    False,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10.10,
                    11.11,
                    'аз',
                    'az',
                    datetime.date(1988, 11, 20),
                    datetime.datetime(1988, 11, 20, 12, 55, 28),
                    datetime.datetime(1988, 11, 20, 12, 55, 28, 111000),
                    '{ "friends" : [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}',
                ],
                [
                    2,
                    True,
                    -2,
                    -3,
                    -4,
                    -5,
                    6,
                    7,
                    8,
                    9,
                    -10.10,
                    -11.11,
                    'буки',
                    'buki',
                    datetime.date(2024, 5, 27),
                    datetime.datetime(2024, 5, 27, 18, 43, 32),
                    datetime.datetime(2024, 5, 27, 18, 43, 32, 123456),
                    '{ "TODO" : "unicode" }',
                ],
            ],
            data_source_kind=EDataSourceKind.YDB,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [tc]

    def _optional_types(self) -> Sequence[TestCase]:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32().to_non_nullable()),
                ),
                Column(
                    name='col_01_boolean',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.BOOL),
                    data_source_type=DataSourceType(ydb=types_ydb.Bool()),
                ),
                Column(
                    name='col_02_int8',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT8),
                    data_source_type=DataSourceType(ydb=types_ydb.Int8()),
                ),
                Column(
                    name='col_03_uint8',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT8),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint8()),
                ),
                Column(
                    name='col_04_int16',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(ydb=types_ydb.Int16()),
                ),
                Column(
                    name='col_05_uint16',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT16),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint16()),
                ),
                Column(
                    name='col_06_int32',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32()),
                ),
                Column(
                    name='col_07_uint32',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint32()),
                ),
                Column(
                    name='col_08_int64',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ydb=types_ydb.Int64()),
                ),
                Column(
                    name='col_09_uint64',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT64),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint64()),
                ),
                Column(
                    name='col_10_float',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.FLOAT),
                    data_source_type=DataSourceType(ydb=types_ydb.Uint64()),
                ),
                Column(
                    name='col_11_double',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(ydb=types_ydb.Double()),
                ),
                Column(
                    name='col_12_string',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ydb=types_ydb.String()),
                ),
                Column(
                    name='col_13_utf8',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ydb=types_ydb.Utf8()),
                ),
                Column(
                    name='col_14_date',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(ydb=types_ydb.Date()),
                ),
                Column(
                    name='col_15_datetime',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATETIME),
                    data_source_type=DataSourceType(ydb=types_ydb.Datetime()),
                ),
                Column(
                    name='col_16_datetime',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ydb=types_ydb.Timestamp()),
                ),
                Column(
                    name='col_17_json',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.JSON),
                    data_source_type=DataSourceType(ydb=types_ydb.Json()),
                ),
            ),
        )

        test_case_name = 'optional_types'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=None,
            data_out_=[
                [
                    1,
                    False,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10.10,
                    11.11,
                    'аз',
                    'az',
                    datetime.date(1988, 11, 20),
                    datetime.datetime(1988, 11, 20, 12, 55, 28),
                    datetime.datetime(1988, 11, 20, 12, 55, 28, 111000),
                    '{ "friends" : [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}',
                ],
                [
                    2,
                    True,
                    -2,
                    -3,
                    -4,
                    -5,
                    6,
                    7,
                    8,
                    9,
                    -10.10,
                    -11.11,
                    'буки',
                    'buki',
                    datetime.date(2024, 5, 27),
                    datetime.datetime(2024, 5, 27, 18, 43, 32),
                    datetime.datetime(2024, 5, 27, 18, 43, 32, 123456),
                    '{ "TODO" : "unicode" }',
                ],
                [
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
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
            ],
            data_source_kind=EDataSourceKind.YDB,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [tc]

    def _constant(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT 42 from YDB table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32().to_non_nullable()),
                ),
            )
        )

        test_case_name = 'constant'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat(SelectWhat.Item(name='42', kind='expr')),
            select_where=None,
            data_in=None,
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
            data_source_kind=EDataSourceKind.YDB,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
        )

        return [tc]

    def _count(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT COUNT(*) from YDB table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32().to_non_nullable()),
                ),
            )
        )

        test_case_name = 'count'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat(SelectWhat.Item(name='COUNT(*)', kind='expr')),
            select_where=None,
            data_in=None,
            data_out_=[
                [
                    4,
                ],
            ],
            protocol=EProtocol.NATIVE,
            data_source_kind=EDataSourceKind.YDB,
            pragmas=dict(),
        )

        return [tc]

    def _pushdown(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32().to_non_nullable()),
                ),
                Column(
                    name='col_01_string',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32()),
                ),
            ),
        )

        data_out = [
            ['one'],
        ]

        return [
            TestCase(
                name_='pushdown',
                data_in=None,
                data_out_=data_out,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_01_string')),
                select_where=SelectWhere('col_00_id = 1'),
                data_source_kind=EDataSourceKind.YDB,
                protocol=EProtocol.NATIVE,
                schema=schema,
            )
        ]

    def _unsupported_types(self) -> Sequence[TestCase]:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32().to_non_nullable()),
                ),
                Column(
                    name='col_01_interval',
                    ydb_type=makeYdbTypeFromTypeID(Type.BOOL),
                    data_source_type=DataSourceType(ydb=types_ydb.Interval()),
                ),
            ),
        )

        test_case_name = 'unsupported_types'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=None,
            # Interval type is not supported, so the second column will be ommited
            data_out_=[
                [
                    1,
                ],
                [
                    2,
                ],
            ],
            data_source_kind=EDataSourceKind.YDB,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [tc]

    def _json(self) -> Sequence[TestCase]:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=types_ydb.Int32().to_non_nullable()),
                ),
                Column(
                    name='col_01_json',
                    ydb_type=makeYdbTypeFromTypeID(Type.JSON),
                    data_source_type=DataSourceType(ydb=types_ydb.Json().to_non_nullable()),
                ),
                Column(
                    name='col_02_json_nullable',
                    ydb_type=makeYdbTypeFromTypeID(Type.JSON),
                    data_source_type=DataSourceType(ydb=types_ydb.Json()),
                ),
            ),
        )

        test_case_name = 'json'

        data_in = [
            [
                1,
                '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}',
                '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}',
            ],
            [2, '{ "TODO" : "unicode" }', '{ "TODO" : "unicode" }'],
            [3, '{}', None],
        ]

        data_out_1 = [
            ['{"age":35,"name":"James Holden"}'],
            [None],
            [None],
        ]

        data_source_kind = EDataSourceKind.YDB

        test_case_name = 'json'

        return [
            TestCase(
                name_=test_case_name,
                data_in=data_in,
                data_out_=data_out_1,
                protocol=EProtocol.NATIVE,
                select_what=SelectWhat(SelectWhat.Item(name='JSON_QUERY(col_01_json, "$.friends[0]")', kind='expr')),
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
                self._optional_types(),
                self._constant(),
                self._count(),
                self._pushdown(),
                self._unsupported_types(),
                self._json(),
            )
        )
