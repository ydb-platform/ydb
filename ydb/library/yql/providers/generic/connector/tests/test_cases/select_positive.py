import datetime
import itertools
from dataclasses import dataclass, replace
from typing import Sequence, Optional

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

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
    makeYdbTypeFromTypeID,
    makeOptionalYdbTypeFromTypeID,
    makeOptionalYdbTypeFromYdbType,
)

from ydb.library.yql.providers.generic.connector.tests.test_cases.base import BaseTestCase
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings

# TODO: Canonize test data in YQL way https://st.yandex-team.ru/YQ-2108


@dataclass
class TestCase(BaseTestCase):
    schema: Schema
    data_in: Sequence
    select_what: SelectWhat
    select_where: Optional[SelectWhere]
    data_out_: Optional[Sequence]
    protocol: EProtocol = EProtocol.NATIVE
    check_output_schema: bool = False

    @property
    def data_out(self) -> Sequence:
        return self.data_out_ if self.data_out_ else self.data_in

    @property
    def generic_settings(self) -> GenericSettings:
        gs = super().generic_settings
        for cluster in gs.clickhouse_clusters:
            cluster.protocol = self.protocol
        return gs


class Factory:
    def _primitive_types_postgresql(self) -> Sequence[TestCase]:
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
                Column(
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
            )
        )

        tc = TestCase(
            name='primitive_types_postgresql',
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
                ],
            ],
            data_out_=None,
            data_source_kind=EDataSourceKind.POSTGRESQL,
            database=Database.make_for_data_source_kind(EDataSourceKind.POSTGRESQL),
            pragmas=dict(),
            check_output_schema=True,
        )

        return [tc]

    def _primitive_types_clickhouse(self) -> Sequence[TestCase]:
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

        tc = TestCase(
            name='primitive_types_clickhouse',
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
            database=Database.make_for_data_source_kind(EDataSourceKind.CLICKHOUSE),
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

        tc_nullable = TestCase(
            name='primitive_types_clickhouse_nullable',
            schema=schema_nullable,
            select_what=SelectWhat.asterisk(schema_nullable.columns),
            select_where=None,
            data_source_kind=EDataSourceKind.CLICKHOUSE,
            database=Database.make_for_data_source_kind(EDataSourceKind.CLICKHOUSE),
            data_in=data_in_nullable,
            data_out_=data_out_nullable,
            pragmas=dict(),
            check_output_schema=True,
        )

        return [
            tc,
            tc_nullable,
        ]

    def _column_selection(self) -> Sequence[TestCase]:
        '''
        In these test case set we check SELECT from a small table with various SELECT parameters,
        like `SELECT * FROM table` or `SELECT a, b, c FROM table`.

        It's quite important that the table has at least one column with lowercase name and
        at least one column with uppercase name.
        Within YQL, every arrow block gets one extra column with this name:
        https://a.yandex-team.ru/arcadia/ydb/library/yql/core/yql_expr_type_annotation.h?rev=r11978071#L333
        This extra column may be added into the arbitrary place in the list of columns
        and affect the data layout used by MiniKQL.
        '''

        # only two columns in a table
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='COL1',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(ch=clickhouse.Int32(), pg=postgresql.Int4()),
                ),
                Column(
                    name='col2',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(ch=clickhouse.Int32(), pg=postgresql.Int4()),
                ),
            )
        )

        # only two rows
        data_in = [[1, 2], [10, 20]]

        # and many ways of performing the SELECT query
        params = (
            # SELECT * FROM table
            (
                SelectWhat.asterisk(column_list=schema.columns),
                data_in,
                (
                    EDataSourceKind.CLICKHOUSE,
                    EDataSourceKind.POSTGRESQL,
                ),
            ),
            # SELECT COL1 FROM table
            (
                SelectWhat(SelectWhat.Item(name='COL1')),
                [
                    [1],
                    [10],
                ],
                (
                    EDataSourceKind.CLICKHOUSE,
                    # doesn't work for PostgreSQL because of implicit cast to lowercase (COL1 -> col1)
                ),
            ),
            # SELECT col1 FROM table
            (
                SelectWhat(SelectWhat.Item(name='col1')),
                [
                    [1],
                    [10],
                ],
                (EDataSourceKind.POSTGRESQL,),  # works because of implicit cast to lowercase (COL1 -> col1)
            ),
            # SELECT col2 FROM table
            (
                SelectWhat(SelectWhat.Item(name='col2')),
                [
                    [2],
                    [20],
                ],
                (
                    EDataSourceKind.CLICKHOUSE,
                    EDataSourceKind.POSTGRESQL,
                ),
            ),
            # SELECT col2, COL1 FROM table
            (
                SelectWhat(SelectWhat.Item(name='col2'), SelectWhat.Item(name='COL1')),
                [
                    [2, 1],
                    [20, 10],
                ],
                (
                    EDataSourceKind.CLICKHOUSE,
                    # doesn't work for PostgreSQL because of implicit cast to lowercase (COL1 -> col1)
                ),
            ),
            # SELECT col2, col1 FROM table
            (
                SelectWhat(SelectWhat.Item(name='col2'), SelectWhat.Item(name='col1')),
                [
                    [2, 1],
                    [20, 10],
                ],
                (EDataSourceKind.POSTGRESQL,),  # works because of implicit cast to lowercase (COL1 -> col1)
            ),
            # Simple math computation:
            # SELECT COL1 + col2 AS col3 FROM table
            (
                SelectWhat(SelectWhat.Item(name='COL1 + col2', alias='col3')),
                [
                    [3],
                    [30],
                ],
                (
                    EDataSourceKind.CLICKHOUSE,
                    # doesn't work for PostgreSQL because of implicit cast to lowercase (COL1 -> col1)
                ),
            ),
            # Select the same column multiple times with different aliases
            # SELECT col2 AS A, col2 AS b, col2 AS C, col2 AS d, col2 AS E FROM table
            (
                SelectWhat(
                    SelectWhat.Item(name='col2', alias='A'),
                    SelectWhat.Item(name='col2', alias='b'),
                    SelectWhat.Item(name='col2', alias='C'),
                    SelectWhat.Item(name='col2', alias='d'),
                    SelectWhat.Item(name='col2', alias='E'),
                ),
                [
                    [2, 2, 2, 2, 2],
                    [20, 20, 20, 20, 20],
                ],
                (
                    EDataSourceKind.CLICKHOUSE,
                    EDataSourceKind.POSTGRESQL,
                ),
            ),
        )

        test_cases = []
        for param in params:
            (select_what, data_out, data_source_kinds) = param

            for data_source_kind in data_source_kinds:
                test_case = TestCase(
                    data_in=data_in,
                    data_source_kind=data_source_kind,
                    database=Database.make_for_data_source_kind(data_source_kind),
                    select_what=select_what,
                    select_where=None,
                    schema=schema,
                    data_out_=data_out,
                    name=f'column_selection_{EDataSourceKind.Name(data_source_kind)}_{select_what}',
                    pragmas=dict(),
                )

                test_cases.append(test_case)

        return test_cases

    def _constant_postgresql(self) -> Sequence[TestCase]:
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

        tc = TestCase(
            name='constant_postgresql',
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
            database=Database.make_for_data_source_kind(EDataSourceKind.POSTGRESQL),
            pragmas=dict(),
        )

        return [tc]

    def _constant_clickhouse(self) -> Sequence[TestCase]:
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

        tc = TestCase(
            name='constant_clickhouse',
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
            database=Database.make_for_data_source_kind(EDataSourceKind.CLICKHOUSE),
            pragmas=dict(),
        )

        return [tc]

    def _count_postgresql(self) -> Sequence[TestCase]:
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

        tc = TestCase(
            name='count_postgresql',
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
            database=Database.make_for_data_source_kind(EDataSourceKind.POSTGRESQL),
            pragmas=dict(),
        )

        return [tc]

    def _count_clickhouse(self) -> Sequence[TestCase]:
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

        tc = TestCase(
            name='count_clickhouse',
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
            data_source_kind=EDataSourceKind.CLICKHOUSE,
            database=Database.make_for_data_source_kind(EDataSourceKind.CLICKHOUSE),
            pragmas=dict(),
        )

        return [tc]

    def _select_upper_case_column_postgresql(self) -> Sequence[TestCase]:
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

        tc = TestCase(
            name='upper_case_column_postgresql',
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
            database=Database.make_for_data_source_kind(EDataSourceKind.POSTGRESQL),
            pragmas=dict(),
        )

        return [tc]

    def make_test_cases(self) -> Sequence[TestCase]:
        protocols = {
            EDataSourceKind.CLICKHOUSE: [EProtocol.NATIVE, EProtocol.HTTP],
            EDataSourceKind.POSTGRESQL: [EProtocol.NATIVE],
        }

        base_test_cases = list(
            itertools.chain(
                self._primitive_types_postgresql(),
                self._primitive_types_clickhouse(),
                self._column_selection(),
                self._constant_postgresql(),
                self._constant_clickhouse(),
                self._count_postgresql(),
                self._count_clickhouse(),
                self._select_upper_case_column_postgresql(),
            )
        )

        test_cases = []
        for base_tc in base_test_cases:
            for protocol in protocols[base_tc.data_source_kind]:
                tc = replace(base_tc)
                tc.name += f'_{protocol}'
                tc.protocol = protocol
                test_cases.append(tc)
        return test_cases
