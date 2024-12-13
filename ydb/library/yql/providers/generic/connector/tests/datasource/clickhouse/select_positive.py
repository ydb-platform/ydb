import datetime
import itertools
from dataclasses import replace
from typing import Sequence, Final

from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind, EGenericProtocol
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
    __primitive_types_schema: Final = Schema(
        columns=ColumnList(
            Column(
                name='col_00_id',
                ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                data_source_type=DataSourceType(ch=clickhouse.Int32()),
            ),
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

    def _primitive_types_non_nullable(self) -> Sequence[TestCase]:
        schema = self.__primitive_types_schema

        tc = TestCase(
            name_='primitive_types_non_nullable',
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
                    'az',
                    'az\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
                    datetime.date(1988, 11, 20),
                    datetime.date(1988, 11, 20),
                    datetime.datetime(1988, 11, 20, 12, 55, 28),
                    datetime.datetime(1988, 11, 20, 12, 55, 28, 123000),
                ],
                [
                    2,
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
                    'буки',
                    'буки\x00\x00\x00\x00\x00',
                    datetime.date(2023, 3, 21),
                    datetime.date(2023, 3, 21),
                    datetime.datetime(2023, 3, 21, 11, 21, 31),
                    datetime.datetime(2023, 3, 21, 11, 21, 31, 456000),
                ],
            ],
            data_source_kind=EGenericDataSourceKind.CLICKHOUSE,
            protocol=EGenericProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [
            tc,
        ]

    def _make_primitive_types_nullable_schema(self) -> Schema:
        schema = self.__primitive_types_schema
        schema_nullable = Schema(columns=ColumnList())

        for i, col in enumerate(schema.columns):
            # do not convert first column to nullable as it contains primary key
            if i == 0:
                schema_nullable.columns.append(col)
                continue

            ch_type = col.data_source_type.ch
            schema_nullable.columns.append(
                Column(
                    name=col.name,
                    ydb_type=makeOptionalYdbTypeFromYdbType(col.ydb_type),
                    data_source_type=DataSourceType(ch=ch_type.to_nullable()),
                )
            )

        return schema_nullable

    def _primitive_types_nullable(self) -> Sequence[TestCase]:
        schema = self._make_primitive_types_nullable_schema()

        tc = TestCase(
            name_='primitive_types_nullable',
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
                    'az',
                    'az\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
                    datetime.date(1988, 11, 20),
                    datetime.date(1988, 11, 20),
                    datetime.datetime(1988, 11, 20, 12, 55, 28),
                    datetime.datetime(1988, 11, 20, 12, 55, 28, 123000),
                ],
                [
                    2,
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
                [
                    3,
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
                    'буки',
                    'буки\x00\x00\x00\x00\x00',
                    datetime.date(2023, 3, 21),
                    datetime.date(2023, 3, 21),
                    datetime.datetime(2023, 3, 21, 11, 21, 31),
                    datetime.datetime(2023, 3, 21, 11, 21, 31, 456000),
                ],
            ],
            data_source_kind=EGenericDataSourceKind.CLICKHOUSE,
            protocol=EGenericProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [
            tc,
        ]

    def _constant(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT 42 from a ch table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ch=clickhouse.Int32()),
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
            data_source_kind=EGenericDataSourceKind.CLICKHOUSE,
            protocol=EGenericProtocol.NATIVE,
            pragmas=dict(),
        )

        return [tc]

    def _counts(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT COUNT(*) from a ch table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col',
                    ydb_type=makeYdbTypeFromTypeID(Type.FLOAT),
                    data_source_type=DataSourceType(ch=clickhouse.Float64()),
                ),
            )
        )

        test_case_name = 'counts'

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
            protocol=EGenericProtocol.NATIVE,
            data_source_kind=EGenericDataSourceKind.CLICKHOUSE,
            pragmas=dict(),
            check_output_schema=False,  # because the aggregate's value has other type
        )

        return [tc]

    def _pushdown(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_int32',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ch=clickhouse.Int32()),
                ),
                Column(
                    name='col_01_string',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ch=clickhouse.String()),
                ),
            ),
        )

        data_out = [
            ['one'],
        ]

        data_source_kind = EGenericDataSourceKind.CLICKHOUSE
        test_case_name = 'pushdown'

        return [
            TestCase(
                name_=test_case_name,
                data_in=None,
                data_out_=data_out,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_01_string')),
                select_where=SelectWhere('col_00_int32 = 1'),
                data_source_kind=data_source_kind,
                protocol=EGenericProtocol.NATIVE,
                schema=schema,
                # TODO: implement schema checkswhen selecting only one column
                check_output_schema=False,
            )
        ]

    def make_test_cases(self) -> Sequence[TestCase]:
        protocols = [EGenericProtocol.NATIVE, EGenericProtocol.HTTP]

        base_test_cases = list(
            itertools.chain(
                self._primitive_types_non_nullable(),
                self._primitive_types_nullable(),
                self._constant(),
                self._counts(),
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
