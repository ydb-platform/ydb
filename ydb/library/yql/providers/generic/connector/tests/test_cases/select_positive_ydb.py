import itertools
from dataclasses import replace
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.utils.ydb as ydb
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
    makeYdbTypeFromTypeID,
)

from ydb.library.yql.providers.generic.connector.tests.test_cases.select_positive_common import TestCase


class Factory:
    def _primitive_types(self) -> Sequence[TestCase]:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_01_boolean',
                    ydb_type=makeYdbTypeFromTypeID(Type.BOOL),
                    data_source_type=DataSourceType(ydb=ydb.Bool()),
                ),
                Column(
                    name='col_02_int8',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT8),
                    data_source_type=DataSourceType(ydb=ydb.Int8()),
                ),
                Column(
                    name='col_03_uint8',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT8),
                    data_source_type=DataSourceType(ydb=ydb.UInt8()),
                ),
                Column(
                    name='col_04_int16',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(ydb=ydb.Int16()),
                ),
                Column(
                    name='col_05_uint16',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT16),
                    data_source_type=DataSourceType(ydb=ydb.UInt16()),
                ),
                Column(
                    name='col_06_int32',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ydb=ydb.Int32()),
                ),
                Column(
                    name='col_07_uint32',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT32),
                    data_source_type=DataSourceType(ydb=ydb.UInt32()),
                ),
                Column(
                    name='col_08_int64',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ydb=ydb.Int64()),
                ),
                Column(
                    name='col_09_uint64',
                    ydb_type=makeYdbTypeFromTypeID(Type.UINT64),
                    data_source_type=DataSourceType(ydb=ydb.UInt64()),
                ),
                Column(
                    name='col_10_float64',
                    ydb_type=makeYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(ydb=ydb.Double()),
                ),
                Column(
                    name='col_11_string',
                    ydb_type=makeYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ydb=ydb.String()),
                ),
            ),
        )

        tc = TestCase(
            name_='primitive_types_ydb',
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
                    'az',
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
                    'buki',
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
                    'az',
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
                    'buki',
                ],
            ],
            data_source_kind=EDataSourceKind.YDB,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [
            tc,
        ]

    def make_test_cases(self) -> Sequence[TestCase]:
        protocols = [EProtocol.NATIVE]

        base_test_cases = list(
            itertools.chain(
                self._primitive_types(),
            )
        )

        test_cases = []
        for base_tc in base_test_cases:
            for protocol in protocols:
                tc = replace(base_tc)
                tc.protocol = protocol
                test_cases.append(tc)
        return test_cases
