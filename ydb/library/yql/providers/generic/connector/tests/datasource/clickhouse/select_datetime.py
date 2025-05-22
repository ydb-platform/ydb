from dataclasses import dataclass
import datetime
from typing import Sequence

from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind, EGenericProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as select_positive_common
import ydb.library.yql.providers.generic.connector.tests.utils.types.clickhouse as clickhouse
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
    makeYdbTypeFromTypeID,
    makeOptionalYdbTypeFromTypeID,
)
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings


@dataclass
class TestCase(select_positive_common.TestCase):
    date_time_format: EDateTimeFormat = EDateTimeFormat.YQL_FORMAT

    @property
    def data_out(self) -> Sequence:
        return self.data_out_ if self.data_out_ else self.data_in

    @property
    def generic_settings(self) -> GenericSettings:
        gs = super().generic_settings
        gs.date_time_format = self.date_time_format
        return gs


class Factory:
    _name = 'datetime'

    '''
    ClickHouse values' bounds:
    Date                    [1970-01-01, 2149-06-06]
    Date32                  [1900-01-01, 2299-12-31]
    Datetime                [1970-01-01 00:00:00, 2106-02-07 06:28:15]
    Datetime64              [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]

    YQL datetime bounds:    [1970-01-01 00:00:00, 2106-01-01 00:00:00]
    '''

    def _make_test_yql(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ch=clickhouse.Int32()),
                ),
                Column(
                    name='col_01_date',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(ch=clickhouse.Date()),
                ),
                Column(
                    name='col_02_date32',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATE),
                    data_source_type=DataSourceType(ch=clickhouse.Date32()),
                ),
                Column(
                    name='col_03_datetime',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.DATETIME),
                    data_source_type=DataSourceType(ch=clickhouse.DateTime()),
                ),
                Column(
                    name='col_04_datetime64',
                    ydb_type=makeOptionalYdbTypeFromTypeID(Type.TIMESTAMP),
                    data_source_type=DataSourceType(ch=clickhouse.DateTime64()),
                ),
            ),
        )

        data_out = [
            [
                1,
                datetime.date(1970, 1, 1),
                None,
                None,
                None,
            ],
            [2, datetime.date(1970, 1, 10), None, datetime.datetime(1980, 1, 10, 12, 23, 45), None],
            [
                3,
                datetime.date(2004, 1, 10),
                datetime.date(2004, 1, 10),
                datetime.datetime(2004, 1, 10, 12, 23, 45),
                datetime.datetime(2004, 1, 10, 12, 23, 45, 678910),
            ],
            [
                4,
                None,
                None,
                None,
                None,
            ],
            [
                5,
                None,
                None,
                datetime.datetime(1970, 12, 4, 5, 55, 29),
                None,
            ],
        ]

        test_case_name = self._name + '_YQL'

        return TestCase(
            name_=test_case_name,
            date_time_format=EDateTimeFormat.YQL_FORMAT,
            data_in=None,
            data_out_=data_out,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_source_kind=EGenericDataSourceKind.CLICKHOUSE,
            protocol=EGenericProtocol.NATIVE,
            schema=schema,
            pragmas=dict(),
            check_output_schema=True,
        )

    def _make_test_string(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_00_id',
                    ydb_type=makeYdbTypeFromTypeID(Type.INT32),
                    data_source_type=DataSourceType(ch=clickhouse.Int32()),
                ),
                Column(
                    name='col_01_date',
                    ydb_type=makeYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ch=clickhouse.Date()),
                ),
                Column(
                    name='col_02_date32',
                    ydb_type=makeYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ch=clickhouse.Date32()),
                ),
                Column(
                    name='col_03_datetime',
                    ydb_type=makeYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ch=clickhouse.DateTime()),
                ),
                Column(
                    name='col_04_datetime64',
                    ydb_type=makeYdbTypeFromTypeID(Type.UTF8),
                    data_source_type=DataSourceType(ch=clickhouse.DateTime64()),
                ),
            ),
        )

        data_out = [
            [1, '1970-01-01', '1900-01-01', '1970-01-01T00:00:00Z', '1950-01-10T12:23:45.67891Z'],
            [2, '1970-01-10', '1950-01-10', '1980-01-10T12:23:45Z', '1950-01-10T12:23:45.67891Z'],
            [3, '2004-01-10', '2004-01-10', '2004-01-10T12:23:45Z', '2004-01-10T12:23:45.67891Z'],
            [4, '2110-01-10', '2110-01-10', '2106-01-10T12:23:45Z', '2110-01-10T12:23:45.67891Z'],
            [
                5,
                '2149-06-06',
                '2299-12-31',
                '1970-12-04T05:55:29Z',
                '1900-01-01T00:00:00Z',  # strange overflow issue with DateTime64 in ClickHouse
            ],
        ]

        test_case_name = self._name + '_string'

        return TestCase(
            name_=test_case_name,
            date_time_format=EDateTimeFormat.STRING_FORMAT,
            protocol=EGenericProtocol.NATIVE,
            data_in=None,
            data_out_=data_out,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_source_kind=EGenericDataSourceKind.CLICKHOUSE,
            schema=schema,
            pragmas=dict(),
            check_output_schema=True,
        )

    def make_test_cases(self) -> Sequence[TestCase]:
        return [
            self._make_test_yql(),
            self._make_test_string(),
        ]
