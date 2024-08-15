from dataclasses import dataclass
import datetime
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as select_positive_common
import ydb.library.yql.providers.generic.connector.tests.utils.types.ms_sql_server as ms_sql_server
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
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
    _schema = Schema(
        columns=ColumnList(
            Column(
                name='col_00_id',
                ydb_type=Type.INT32,
                data_source_type=DataSourceType(ms=ms_sql_server.Int()),
            ),
            Column(
                name='col_01_date',
                ydb_type=Type.DATE,
                data_source_type=DataSourceType(ms=ms_sql_server.Date()),
            ),
            Column(
                name='col_02_smalldatetime',
                ydb_type=Type.DATETIME,
                data_source_type=DataSourceType(ms=ms_sql_server.SmallDatetime()),
            ),
            Column(
                name='col_03_datetime',
                ydb_type=Type.TIMESTAMP,
                data_source_type=DataSourceType(ms=ms_sql_server.Datetime()),
            ),
            Column(
                name='col_04_datetime2',
                ydb_type=Type.TIMESTAMP,
                data_source_type=DataSourceType(ms=ms_sql_server.Datetime2()),
            ),
        ),
    )

    def _make_test_yql(self) -> TestCase:
        data_out = [
            [
                1,
                None,
                None,
                None,
            ],
            [
                2,
                datetime.date(2023, 3, 21),
                datetime.datetime(2023, 3, 21, 11, 21, 0, 0),
                datetime.datetime(2023, 3, 21, 11, 21, 31, 0),
            ],
            [3, None, None, None],
        ]

        return TestCase(
            name_="datetimes",
            date_time_format=EDateTimeFormat.YQL_FORMAT,
            data_in=None,
            data_out_=data_out,
            select_what=SelectWhat.asterisk(self._schema.columns),
            select_where=None,
            data_source_kind=EDataSourceKind.MS_SQL_SERVER,
            protocol=EProtocol.NATIVE,
            schema=self._schema,
            pragmas=dict(),
        )

    def _make_test_string(self) -> TestCase:
        data_out = [
            [
                1,
                '1950-05-27',
                '1950-05-27T01:02:00Z',
                '1950-05-27T01:02:03.110Z',
                '1950-05-27T01:02:03.1111111Z',
            ],
            [
                2,
                '2023-03-21',
                '2023-03-21T11:21:00Z',
                '2023-03-21T11:21:31Z',
                '2023-03-21T11:21:31Z',
            ],
            [
                3,
                '2079-06-06',
                '2079-06-06T23:59:59.999Z',
                '2079-06-06T23:59:59.9999999Z'
            ],
        ]

        return TestCase(
            name_="datetimes",
            date_time_format=EDateTimeFormat.STRING_FORMAT,
            data_in=None,
            data_out_=data_out,
            select_what=SelectWhat.asterisk(self._schema.columns),
            select_where=None,
            data_source_kind=EDataSourceKind.MS_SQL_SERVER,
            protocol=EProtocol.NATIVE,
            schema=self._schema,
            pragmas=dict(),
        )

    def make_test_cases(self) -> Sequence[TestCase]:
        return [
            self._make_test_yql(),
            self._make_test_string(),
        ]
