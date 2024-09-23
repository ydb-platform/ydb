import datetime
import itertools
from dataclasses import replace
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.utils.types.clickhouse as clickhouse
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
    SelectWhere,
    makeYdbTypeFromTypeID,
    makeOptionalYdbTypeFromTypeID,
    makeOptionalYdbTypeFromYdbType,
)

from ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common import TestCase


class Factory:
    def _primitive_types(self) -> Sequence[TestCase]:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_01_boolean',
                    ydb_type=makeYdbTypeFromTypeID(Type.BOOL),
                    data_source_type=DataSourceType(ch=clickhouse.Boolean()),
                ),
                Column(
                    name='col_02_int8',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT8),
                    data_source_type=DataSourceType(ch=clickhouse.Int8()),
                ),
                Column(
                    name='col_03_uint8',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT8),
                    data_source_type=DataSourceType(ch=clickhouse.UInt8()),
                ),
                Column(
                    name='col_04_int16',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(ch=clickhouse.Int16()),
                ),
                Column(
                    name='col_05_uint16',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT16),
                    data_source_type=DataSourceType(ch=clickhouse.UInt16()),
                ),
                Column(
                    name='col_06_int32',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ch=clickhouse.Int32()),
                ),
                Column(
                    name='col_07_uint32',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT32),
                    data_source_type=DataSourceType(ch=clickhouse.UInt32()),
                ),
                Column(
                    name='col_08_int64',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ch=clickhouse.Int64()),
                ),
                Column(
                    name='col_09_uint64',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT64),
                    data_source_type=DataSourceType(ch=clickhouse.UInt64()),
                ),
                Column(
                    name='col_10_float32',
                    ydb_type=makeYdbTypeFromTypeID(Type.FLOAT),
                    data_source_type=DataSourceType(ch=clickhouse.Float32()),
                ),
                Column(
                    name='col_11_float64',
                    ydb_type=makeYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(ch=clickhouse.Float64()),
                ),
                Column(
                    name='col_12_string',
                    ydb_type=makeYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ch=clickhouse.String()),
                ),
                Column(
                    name='col_13_fixed_string',
                    ydb_type=makeYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ch=clickhouse.FixedString()),
                ),
                Column(
                    name='col_14_date',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(ch=clickhouse.Date()),
                ),
                Column(
                    name='col_15_date32',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(ch=clickhouse.Date32()),
                ),
                Column(
                    name='col_16_datetime',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATETIME),
                    data_source_type=DataSourceType(ch=clickhouse.DateTime()),
                ),
                Column(
                    name='col_17_datetime64',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ch=clickhouse.DateTime64()),
                ),
            ),
        )

        test_case_name = 'primitive_types'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=[
                [
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
                    'az',
                    'az   ',
                    '2023-01-09',
                    '2023-01-09',
                    '2023-01-09 13:19:11',
                    '2023-01-09 13:19:11.123456',
                ],
                [
                    True,
                    -2,
                    3,
                    -4,
                    5,
                    -6,
                    7,
                    -8,
                    9,
                    -10.10,
                    -11.11,
                    'buki',
                    'buki ',
                    '1988-11-20',
                    '1988-11-20',
                    '1988-11-20 12:00:00',
                    '1988-11-20 12:00:00.100000',
                ],
            ],
            data_out_=[
                [
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
                    'az',
                    'az   ',
                    datetime.date(2023, 1, 9),
                    datetime.date(2023, 1, 9),
                    datetime.datetime(2023, 1, 9, 13, 19, 11),
                    datetime.datetime(2023, 1, 9, 13, 19, 11, 123456),
                ],
                [
                    True,
                    -2,
                    3,
                    -4,
                    5,
                    -6,
                    7,
                    -8,
                    9,
                    -10.10,
                    -11.11,
                    'buki',
                    'buki ',
                    datetime.date(1988, 11, 20),
                    datetime.date(1988, 11, 20),
                    datetime.datetime(1988, 11, 20, 12, 00, 00),
                    datetime.datetime(1988, 11, 20, 12, 00, 00, 100000),
                ],
            ],
            data_source_kind=EDataSourceKind.CLICKHOUSE,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        # ClickHouse returns different data if columns are Nullable
        schema_nullable = Schema(columns=ColumnList())
        data_in_nullable = [[]]
        data_out_nullable = [[]]

        for i, col in enumerate(schema.columns):
            ch_type = col.data_source_type.ch

            # copy type and example value to new TestCase
            schema_nullable.columns.append(
                Column(
                    name=col.name,
                    ydb_type=makeOptionalYdbTypeFromYdbType(col.ydb_type),
                    data_source_type=DataSourceType(ch=ch_type.to_nullable()),
                )
            )
            data_in_nullable[0].append(tc.data_in[0][i])
            data_out_nullable[0].append(tc.data_out_[0][i])

        # Add row containing only NULL values
        data_in_nullable.append([None] * len(data_in_nullable[0]))
        data_out_nullable.append([None] * len(data_out_nullable[0]))

        # for the sake of CH output sorting
        data_in_nullable[1][0] = data_out_nullable[1][0] = True

        test_case_name_nullable = 'primitive_types_nullable'

        tc_nullable = TestCase(
            name_=test_case_name_nullable,
            schema=schema_nullable,
            select_what=SelectWhat.asterisk(schema_nullable.columns),
            select_where=None,
            data_source_kind=EDataSourceKind.CLICKHOUSE,
            data_in=data_in_nullable,
            data_out_=data_out_nullable,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [
            tc,
            tc_nullable,
        ]

    def _constant(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT 42 from a ch table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col',
                    ydb_type=Type.INT64,
                    data_source_type=DataSourceType(ch=clickhouse.Int64()),
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
            data_source_kind=EDataSourceKind.CLICKHOUSE,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
        )

        return [tc]

    def _count(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT COUNT(*) from a ch table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col',
                    ydb_type=Type.FLOAT,
                    data_source_type=DataSourceType(ch=clickhouse.Float64()),
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
                    3.14,
                ],
                [
                    1.0,
                ],
                [
                    2.718,
                ],
                [
                    -0.0,
                ],
            ],
            data_out_=[
                [
                    4,
                ],
            ],
            protocol=EProtocol.NATIVE,
            data_source_kind=EDataSourceKind.CLICKHOUSE,
            pragmas=dict(),
        )

        return [tc]

    def _pushdown(self) -> TestCase:
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
        test_case_name = 'pushdown'

        return [
            TestCase(
                name_=test_case_name,
                data_in=data_in,
                data_out_=data_out,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_string')),
                select_where=SelectWhere('col_int32 = 1'),
                data_source_kind=data_source_kind,
                protocol=EProtocol.NATIVE,
                schema=schema,
            )
        ]

    def make_test_cases(self) -> Sequence[TestCase]:
        protocols = [EProtocol.NATIVE, EProtocol.HTTP]

        base_test_cases = list(
            itertools.chain(
                self._primitive_types(),
                self._constant(),
                self._count(),
                self._pushdown(),
            )
        )

        test_cases = []
        for base_tc in base_test_cases:
            for protocol in protocols:
                tc = replace(base_tc)
                tc.protocol = protocol
                test_cases.append(tc)
        return test_cases
