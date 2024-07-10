import datetime
import itertools
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.utils.types.mysql as mysql
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
                    data_source_type=DataSourceType(my=mysql.Integer()),
                ),
                Column(
                    name='col_01_tinyint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT8),
                    data_source_type=DataSourceType(my=mysql.TinyInt()),
                ),
                Column(
                    name='col_02_tinyint_unsigned',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT8),
                    data_source_type=DataSourceType(my=mysql.TinyIntUnsigned()),
                ),
                Column(
                    name='col_03_smallint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT16),
                    data_source_type=DataSourceType(my=mysql.SmallInt()),
                ),
                Column(
                    name='col_04_smallint_unsigned',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT16),
                    data_source_type=DataSourceType(my=mysql.SmallIntUnsigned()),
                ),
                Column(
                    name='col_05_mediumint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(my=mysql.MediumInt()),
                ),
                Column(
                    name='col_06_mediumint_unsigned',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT32),
                    data_source_type=DataSourceType(my=mysql.MediumIntUnsigned()),
                ),
                Column(
                    name='col_07_integer',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(my=mysql.Integer()),
                ),
                Column(
                    name='col_08_integer_unsigned',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT32),
                    data_source_type=DataSourceType(my=mysql.IntegerUnsigned()),
                ),
                Column(
                    name='col_09_bigint',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(my=mysql.BigInt()),
                ),
                Column(
                    name='col_10_bigint_unsigned',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UINT64),
                    data_source_type=DataSourceType(my=mysql.BigIntUnsigned()),
                ),
                Column(
                    name='col_11_float',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.FLOAT),
                    data_source_type=DataSourceType(my=mysql.Float()),
                ),
                Column(
                    name='col_12_double',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(my=mysql.Double()),
                ),
                Column(
                    name='col_13_date',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(my=mysql.Date()),
                ),
                Column(
                    name='col_14_datetime',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(my=mysql.Datetime()),
                ),
                Column(
                    name='col_15_timestamp',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(my=mysql.Timestamp()),
                ),
                Column(
                    name='col_16_char',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(my=mysql.Char()),
                ),
                Column(
                    name='col_17_varchar',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(my=mysql.VarChar()),
                ),
                Column(
                    name='col_18_tinytext',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(my=mysql.TinyText()),
                ),
                Column(
                    name='col_19_text',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(my=mysql.Text()),
                ),
                Column(
                    name='col_20_mediumtext',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(my=mysql.MediumText()),
                ),
                Column(
                    name='col_21_longtext',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(my=mysql.LongText()),
                ),
                # Don't be surprised - binary types look like UTF8 strings in Go
                Column(
                    name='col_22_binary',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(my=mysql.Binary()),
                ),
                Column(
                    name='col_23_varbinary',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(my=mysql.VarBinary()),
                ),
                Column(
                    name='col_24_tinyblob',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(my=mysql.TinyBlob()),
                ),
                Column(
                    name='col_25_blob',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(my=mysql.Blob()),
                ),
                Column(
                    name='col_26_mediumblob',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(my=mysql.MediumBlob()),
                ),
                Column(
                    name='col_27_longblob',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(my=mysql.LongBlob()),
                ),
                Column(
                    name='col_28_bool',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.BOOL),
                    data_source_type=DataSourceType(my=mysql.Bool()),
                ),
                Column(
                    name='col_29_json',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.JSON),
                    data_source_type=DataSourceType(my=mysql.Json()),
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
                    6,
                    7,
                    8,
                    9,
                    10,
                    11.11,
                    12.12,
                    datetime.date(1988, 11, 20),
                    datetime.datetime(1988, 11, 20, 12, 34, 56, 777777),
                    datetime.datetime(1988, 11, 20, 12, 34, 56, 777777),
                    'az',
                    'az',
                    'az',
                    'az',
                    'az',
                    'az',
                    'az\x00\x00\x00\x00\x00\x00',
                    'az',
                    'az',
                    'az',
                    'az',
                    'az',
                    True,
                    '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}',
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
                    -10,
                    20,
                    -30,
                    40,
                    -50,
                    60,
                    -70,
                    80,
                    -90,
                    100,
                    -1111.1111,
                    -1212.1212,
                    datetime.date(2024, 7, 1),
                    datetime.datetime(2024, 7, 1, 1, 2, 3, 444444),
                    datetime.datetime(2024, 7, 1, 1, 2, 3, 444444),
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    'буки',
                    False,
                    '{ "TODO" : "unicode" }',
                ],
            ],
            data_source_kind=EDataSourceKind.MYSQL,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [tc]

    def _constant(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT 42 from MySQL table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_01',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(my=mysql.Integer()),
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
            data_source_kind=EDataSourceKind.MYSQL,
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
                    data_source_type=DataSourceType(my=mysql.Integer()),
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
            data_source_kind=EDataSourceKind.MYSQL,
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
                    data_source_type=DataSourceType(my=mysql.Integer()),
                ),
                Column(
                    name='col_01_integer',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(my=mysql.Integer()),
                ),
                Column(
                    name='col_02_text',
                    ydb_type=Type.STRING,
                    data_source_type=DataSourceType(my=mysql.Text()),
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
                data_source_kind=EDataSourceKind.MYSQL,
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
                data_source_kind=EDataSourceKind.MYSQL,
                schema=schema,
            ),
        ]

    def _json(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(my=mysql.Integer()),
                ),
                Column(
                    name='col_01_json',
                    ydb_type=Type.JSON,
                    data_source_type=DataSourceType(my=mysql.Json()),
                ),
            ),
        )

        return [
            TestCase(
                name_='json',
                data_in=None,
                data_out_=[
                    ['{"age":35,"name":"James Holden"}'],
                    [None],
                    [None],
                ],
                protocol=EProtocol.NATIVE,
                select_what=SelectWhat(SelectWhat.Item(name='JSON_QUERY(col_01_json, "$.friends[0]")', kind='expr')),
                select_where=None,
                data_source_kind=EDataSourceKind.MYSQL,
                pragmas=dict(),
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
                self._json(),
            )
        )
