import datetime
from dataclasses import dataclass
import itertools
from typing import Sequence, Optional

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings
import ydb.library.yql.providers.generic.connector.tests.utils.types.oracle as oracle
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
    SelectWhere,
    makeOptionalYdbTypeFromTypeID,
)

from ydb.library.yql.providers.generic.connector.tests.common_test_cases.base import BaseTestCase


@dataclass
class TestCase(BaseTestCase):
    schema: Schema
    data_in: Sequence
    select_what: SelectWhat
    select_where: Optional[SelectWhere]
    service_name: str
    data_out_: Optional[Sequence] = None
    check_output_schema: bool = False

    @property
    def data_out(self) -> Sequence:
        return self.data_out_ if self.data_out_ else self.data_in

    @property
    def generic_settings(self) -> GenericSettings:
        gs = super().generic_settings
        for cluster in gs.oracle_clusters:
            cluster.service_name = self.service_name
        return gs


class Factory:
    ss: Settings

    def __init__(self, ss: Settings):
        self.ss = ss

    def _primitive_types(self) -> Sequence[TestCase]:
        """
        Every data source has its own type system;
        we test datasource-specific types in the following test cases.
        """
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='COL_00_ID',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
                Column(
                    name='COL_01_INT',  # is NUMBER, but INTEGER alias used
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
                # Column( TODO add support for Oracle FLOAT (words as NUMBER)
                #     name='COL_02_FLOAT',
                #     ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                #     data_source_type=DataSourceType(ora=oracle.Number()),
                # ),
                Column(
                    name='COL_03_INT_NUMBER',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
                # Column( TODO add support for Oracle NUMBER(p, s) with s > 0 (fractional numbers)
                #     name='COL_04_FRAC_NUMBER',
                #     ydb_type=makeOptionalYdbTypeFromTypeID(Type.DECIMAL),
                #     data_source_type=DataSourceType(ora=oracle.Decimal()),
                # ),
                # Column( TODO go-ora driver has a bug reading -1.1 as -1.2. Possibly problem in convertion function of go-ora
                #     name='COL_05_BINARY_FLOAT',
                #     ydb_type=makeOptionalYdbTypeFromTypeID(Type.FLOAT),
                #     data_source_type=DataSourceType(ora=oracle.BinaryFloat()),
                # ),
                Column(
                    name='COL_06_BINARY_DOUBLE',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DOUBLE),
                    data_source_type=DataSourceType(ora=oracle.BinaryDouble()),
                ),
                Column(
                    name='COL_07_VARCHAR2',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.VarChar2()),
                ),
                Column(
                    name='COL_08_NVARCHAR2',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.NVarChar2()),
                ),
                Column(
                    name='COL_09_CHAR_ONE',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.Char()),
                ),
                Column(
                    name='COL_10_CHAR_SMALL',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.Char()),
                ),
                Column(
                    name='COL_11_NCHAR_ONE',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.NChar()),
                ),
                Column(
                    name='COL_12_NCHAR_SMALL',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.NChar()),
                ),
                Column(
                    name='COL_13_CLOB',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.CLob()),
                ),
                Column(
                    name='COL_14_NCLOB',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.NCLob()),
                ),
                Column(
                    name='COL_15_RAW',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ora=oracle.NCLob()),
                ),
                Column(
                    name='COL_16_BLOB',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ora=oracle.Blob()),
                ),
                Column(
                    name='COL_17_DATE',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATETIME),
                    data_source_type=DataSourceType(ora=oracle.Date()),
                ),
                Column(
                    name='COL_18_TIMESTAMP',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ora=oracle.Timestamp()),
                ),
                Column(
                    name='COL_19_TIMESTAMP_W_TIMEZONE',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ora=oracle.TimestampWTZ()),
                ),
                Column(
                    name='COL_20_TIMESTAMP_W_LOCAL_TIMEZONE',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ora=oracle.TimestampWLocalTZ()),
                ),
                Column(
                    name='COL_21_JSON',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.JSON),
                    data_source_type=DataSourceType(ora=oracle.Json()),
                ),
            )
        )

        tc = TestCase(
            name_='PRIMITIVES',
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=None,
            data_out_=[
                [
                    1,
                    1,
                    # 1.1,
                    123,
                    # 1.1,
                    # 1.1,
                    1.1,
                    'varchar',
                    'варчар',
                    'c',
                    'cha',
                    'ч',
                    'чар',
                    'clob',
                    'клоб',
                    'ABCD',
                    'EF',
                    datetime.datetime(1970, 1, 1, 0, 0, 0, 000000),
                    datetime.datetime(1970, 1, 1, 1, 1, 1, 111111),
                    datetime.datetime(1970, 1, 1, 2, 1, 1, 111111),
                    datetime.datetime(1970, 1, 1, 2, 12, 1, 111111),
                    '{ "friends": [{"name": "James Holden","age": 35},{"name": "Naomi Nagata","age": 30}]}',
                ],
                [
                    2,
                    None,
                    # None,
                    None,
                    # None,
                    # None,
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
                    -1,
                    # -1.1,
                    -123,
                    # -1.1,
                    # -1.1,
                    -1.1,
                    'varchar',
                    'варчар',
                    'c',
                    'cha',
                    'ч',
                    'чар',
                    'clob',
                    'клоб',
                    '1234',
                    '5678',
                    datetime.datetime(1970, 1, 1, 0, 0, 0, 000000),
                    datetime.datetime(1970, 1, 1, 1, 1, 1, 111111),
                    datetime.datetime(1970, 1, 1, 2, 1, 1, 111111),
                    datetime.datetime(1970, 1, 1, 2, 2, 12, 111111),
                    '{ "TODO" : "unicode" }',
                ],
            ],
            data_source_kind=EDataSourceKind.ORACLE,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
            service_name=self.ss.oracle.service_name,
        )

        return [tc]

    def _longraw(self) -> Sequence[TestCase]:
        """
        Oracle can contain only ont LONG column in each table.
        In this test case we are checking for read in small LONG column
        """
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='COL_00_ID',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
                Column(
                    name='COL_01_LONG_RAW',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.STRING),
                    data_source_type=DataSourceType(ora=oracle.LongRaw()),
                ),
            )
        )

        tc = TestCase(
            name_='LONGRAW',
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=None,
            data_out_=[
                [
                    1,
                    '12',
                ],
                [2, None],
                [
                    3,
                    None,
                ],
            ],
            data_source_kind=EDataSourceKind.ORACLE,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
            service_name=self.ss.oracle.service_name,
        )

        return [tc]

    def _long_table(self) -> Sequence[TestCase]:
        """
        Oracle can contain only ont LONG column in each table.
        In this test case we are checking for read in small LONG column
        """
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='COL_00_ID',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.INT64),
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
                Column(
                    name='COL_01_LONG',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ora=oracle.Long()),
                ),
            )
        )

        tc = TestCase(
            name_='LONG_TABLE',
            schema=schema,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_in=None,
            data_out_=[
                [
                    1,
                    'long',
                ],
                [2, None],
                [
                    3,
                    None,
                ],
            ],
            data_source_kind=EDataSourceKind.ORACLE,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            check_output_schema=True,
            service_name=self.ss.oracle.service_name,
        )

        return [tc]

    def _constant(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT 42 from Oracle table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='COL_01',
                    ydb_type=Type.INT64,
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
            )
        )

        test_case_name = 'CONSTANT'

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
            data_source_kind=EDataSourceKind.ORACLE,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            service_name=self.ss.oracle.service_name,
        )

        return [tc]

    def _count_rows(self) -> Sequence[TestCase]:
        '''
        In this test case set we check SELECT COUNT(*) from a pg table.
        '''

        schema = Schema(
            columns=ColumnList(
                Column(
                    name='COL_01',
                    ydb_type=Type.INT64,
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
            )
        )

        test_case_name = 'COUNT_ROWS'

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
            data_source_kind=EDataSourceKind.ORACLE,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            service_name=self.ss.oracle.service_name,
        )

        return [tc]

    def _pushdown(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='COL_00_ID',
                    ydb_type=Type.INT64,
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
                Column(
                    name='COL_01_INTEGER',  # used INTEGER that is alias to Number
                    ydb_type=Type.INT64,
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
                Column(
                    name='COL_02_TEXT',
                    ydb_type=Type.STRING,
                    data_source_type=DataSourceType(ora=oracle.VarChar()),
                ),
            ),
        )

        test_case_name = 'PUSHDOWN'

        return [
            TestCase(
                name_=test_case_name,
                data_in=None,
                data_out_=[[4, None, None]],
                protocol=EProtocol.NATIVE,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat.asterisk(schema.columns),
                select_where=SelectWhere('COL_00_ID = 4'),
                data_source_kind=EDataSourceKind.ORACLE,
                schema=schema,
                service_name=self.ss.oracle.service_name,
            ),
            TestCase(
                name_=test_case_name,
                data_in=None,
                data_out_=[
                    ['b'],
                ],
                protocol=EProtocol.NATIVE,
                pragmas=dict({'generic.UsePredicatePushdown': 'true'}),
                select_what=SelectWhat(SelectWhat.Item(name='COL_02_TEXT')),
                select_where=SelectWhere('COL_00_ID = COL_01_INTEGER'),
                data_source_kind=EDataSourceKind.ORACLE,
                schema=schema,
                service_name=self.ss.oracle.service_name,
            ),
        ]

    def _json(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='ID',
                    ydb_type=Type.INT64,
                    data_source_type=DataSourceType(ora=oracle.Number()),
                ),
                Column(
                    name='COL_01_JSON',
                    ydb_type=Type.JSON,
                    data_source_type=DataSourceType(ora=oracle.Json()),
                ),
            ),
        )

        return [
            TestCase(
                name_='JSON',
                data_in=None,
                data_out_=[
                    ['{"age":35,"name":"James Holden"}'],
                    [None],
                    [None],
                ],
                protocol=EProtocol.NATIVE,
                select_what=SelectWhat(SelectWhat.Item(name='JSON_QUERY(COL_01_JSON, "$.friends[0]")', kind='expr')),
                select_where=None,
                data_source_kind=EDataSourceKind.ORACLE,
                pragmas=dict(),
                schema=schema,
                service_name=self.ss.oracle.service_name,
            ),
        ]

    def make_test_cases(self) -> Sequence[TestCase]:
        return list(
            itertools.chain(
                self._primitive_types(),
                self._longraw(),
                self._long_table(),
                self._constant(),
                self._count_rows(),
                self._json(),
                self._pushdown(),
            )
        )
