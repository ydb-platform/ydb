import datetime
from dataclasses import dataclass
from typing import Sequence

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as select_positive_common
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings
import ydb.library.yql.providers.generic.connector.tests.utils.types.oracle as oracle
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
)


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
    ss: Settings

    def __init__(self, ss: Settings):
        self.ss = ss

    _schema = Schema(
        columns=ColumnList(
            Column(
                name='COL_00_ID',
                ydb_type=Type.INT64,
                data_source_type=DataSourceType(ora=oracle.Number()),
            ),
            Column(
                name='COL_01_DATE',
                ydb_type=Type.DATETIME,
                data_source_type=DataSourceType(ora=oracle.Date()),
            ),
            Column(
                name='COL_02_TIMESTAMP',
                ydb_type=Type.TIMESTAMP,
                data_source_type=DataSourceType(ora=oracle.Timestamp()),
            ),
        ),
    )

    def _make_test_yql(self) -> TestCase:
        data_out = [
            [
                1,
                None,
                None,
            ],
            [
                2,
                datetime.datetime(1988, 11, 20, 12, 55, 28, 000000),
                datetime.datetime(1988, 11, 20, 12, 55, 28, 123000),
            ],
            [
                3,
                datetime.datetime(2038, 1, 19, 3, 14, 7, 0),
                datetime.datetime(2038, 1, 19, 3, 14, 7, 0),
            ],
            [4, None, None]
        ]

        return TestCase(
            name_="DATETIMES",
            date_time_format=EDateTimeFormat.YQL_FORMAT,
            data_in=None,
            data_out_=data_out,
            select_what=SelectWhat.asterisk(self._schema.columns),
            select_where=None,
            data_source_kind=EDataSourceKind.ORACLE,
            protocol=EProtocol.NATIVE,
            schema=self._schema,
            pragmas=dict(),
            service_name=self.ss.oracle.service_name,
        )

    def _make_test_string(self) -> TestCase:
        data_out = [
            [
                1,
                '1950-05-27T01:02:03Z',
                '1950-05-27T01:02:03.111111Z',
            ],
            [
                2,
                '1988-11-20T12:55:28Z',
                '1988-11-20T12:55:28.123000Z',
            ],
            [3, '2038-01-19T03:14:07Z', '2038-01-19T03:14:07.000000Z'],
            [4, '9999-12-31T23:59:59Z', '9999-12-31T23:59:59.999999Z'],
        ]

        return TestCase(
            name_="DATETIMES",
            date_time_format=EDateTimeFormat.STRING_FORMAT,
            data_in=None,
            data_out_=data_out,
            select_what=SelectWhat.asterisk(self._schema.columns),
            select_where=None,
            data_source_kind=EDataSourceKind.ORACLE,
            protocol=EProtocol.NATIVE,
            schema=self._schema,
            pragmas=dict(),
            service_name=self.ss.oracle.service_name,
        )

    def make_test_cases(self) -> Sequence[TestCase]:
        return [
            self._make_test_yql(),
            self._make_test_string(),
        ]
