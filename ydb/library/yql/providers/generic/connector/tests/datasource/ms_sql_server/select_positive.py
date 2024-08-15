import datetime
import itertools
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.utils.types.ms_sql_server as ms_sql_server
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
                    name='col_00_id',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ms=ms_sql_server.Int()),
                ),
                Column(
                    name='col_01_bit',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.BOOL),
                    data_source_type=DataSourceType(ms=ms_sql_server.Bit()),
                ),
                Column(
                    name='col_02_tinyint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT8),
                    data_source_type=DataSourceType(ms=ms_sql_server.TinyInt()),
                ),
                Column(
                    name='col_03_smallint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(ms=ms_sql_server.SmallInt()),
                ),
                Column(
                    name='col_04_int',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ms=ms_sql_server.Int()),
                ),
                Column(
                    name='col_05_bigint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ms=ms_sql_server.BigInt()),
                ),
                Column(
                    name='col_06_float',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(ms=ms_sql_server.Float()),
                ),
                Column(
                    name='col_07_real',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.FLOAT),
                    data_source_type=DataSourceType(ms=ms_sql_server.Real()),
                ),
                Column(
                    name='col_08_char',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ms=ms_sql_server.Char()),
                ),
                Column(
                    name='col_09_varchar',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ms=ms_sql_server.VarChar()),
                ),
                Column(
                    name='col_10_text',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ms=ms_sql_server.Text()),
                ),
                Column(
                    name='col_11_nchar',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ms=ms_sql_server.Char()),
                ),
                Column(
                    name='col_12_nvarchar',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ms=ms_sql_server.VarChar()),
                ),
                Column(
                    name='col_13_ntext',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ms=ms_sql_server.Text()),
                ),
                Column(
                    name='col_14_binary',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ms=ms_sql_server.Binary()),
                ),
                Column(
                    name='col_15_varbinary',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ms=ms_sql_server.VarBinary()),
                ),
                Column(
                    name='col_16_image',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ms=ms_sql_server.Image()),
                ),
                Column(
                    name='col_17_date',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(ms=ms_sql_server.Date()),
                ),
                Column(
                    name='col_18_smalldatetime',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATETIME),
                    data_source_type=DataSourceType(ms=ms_sql_server.SmallDatetime()),
                ),
                Column(
                    name='col_19_datetime',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ms=ms_sql_server.Datetime()),
                ),
                Column(
                    name='col_20_datetime2',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ms=ms_sql_server.Datetime()),
                ),
            )
        )

        tc = TestCase(
            name_='primitives',
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=None,
            data_out_=[
                [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6.6,
                    7.7,
                    'az      ',
                    'az',
                    'az',
                    'az      ',
                    'az',
                    'az',
                    b'\x12\x34\x56\x78\x90\xAB\xCD\xEF',
                    b'\x12\x34\x56\x78\x90\xAB\xCD\xEF',
                    b'\x12\x34\x56\x78\x90\xAB\xCD\xEF',
                    datetime.date(1988, 11, 20),
                    datetime.datetime(1988, 11, 20, 12, 55, 0, 0),
                    datetime.datetime(1988, 11, 20, 12, 55, 28, 123000),
                    datetime.datetime(1988, 11, 20, 12, 55, 28, 123123),
                ],
                [
                    1,
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
                    None,
                    None,
                    None,
                ],
                [
                    2,
                    0,
                    2,
                    -3,
                    -4,
                    -5,
                    -6.6,
                    -7.7,
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    b'\x12\x34\x56\x78\x90\xAB\xCD\xEF',
                    b'\x12\x34\x56\x78\x90\xAB\xCD\xEF',
                    b'\x12\x34\x56\x78\x90\xAB\xCD\xEF',
                    datetime.date(2023, 3, 21),
                    datetime.datetime(2023, 3, 21, 11, 21, 0, 0),
                    datetime.datetime(2023, 3, 21, 11, 21, 31, 0),
                    datetime.datetime(2023, 3, 21, 11, 21, 31, 0),
                ],
            ],
            data_source_kind=EDataSourceKind.MS_SQL_SERVER,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [tc]

    def _constant(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT 42 from ms_sql_server table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_01',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(ms=ms_sql_server.Int()),
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
            data_source_kind=EDataSourceKind.MS_SQL_SERVER,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
        )

        return [tc]

    def _count_rows(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT COUNT(*) from a pg table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_01',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(ms=ms_sql_server.Int()),
                ),
            )
        )

        test_case_name = 'count_rows'

        tc = TestCase(
            name_=test_case_name,
            schema=schema,
            select_what=SelectWhat(SelectWhat.Item(name='COUNT(*)', kind='expr')),
            select_where=None,
            data_in=None,
            data_out_=[
                [
                    3,
                ],
            ],
            data_source_kind=EDataSourceKind.MS_SQL_SERVER,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
        )

        return [tc]

    def _pushdown(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(ms=ms_sql_server.Int()),
                ),
                Column(
                    name='col_01_integer',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(ms=ms_sql_server.Int()),
                ),
                Column(
                    name='col_02_text',
                    ydb_type=Type.STRING,
                    data_source_type=DataSourceType(ms=ms_sql_server.Text()),
                ),
            ),
        )

        test_case_name = 'pushdown'

        return [
            TestCase(
                name_=test_case_name,
                data_in=None,
                data_out_=[[4, None, None]],
                protocol=EProtocol.NATIVE,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat.asterisk(schema.columns),
                select_where=SelectWhere('col_00_id = 4'),
                data_source_kind=EDataSourceKind.MS_SQL_SERVER,
                schema=schema,
            ),
            TestCase(
                name_=test_case_name,
                data_in=None,
                data_out_=[
                    ['b'],
                ],
                protocol=EProtocol.NATIVE,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='col_02_text')),
                select_where=SelectWhere('col_00_id = col_01_integer'),
                data_source_kind=EDataSourceKind.MS_SQL_SERVER,
                schema=schema,
            ),
        ]


    def make_test_cases(self) -> Sequence[TestCase]:
        return list(
            itertools.chain(
                self._primitive_types(),
                self._constant(),
                self._count_rows(),
                self._pushdown(),
            )
        )
