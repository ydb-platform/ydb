from dataclasses import dataclass
import datetime
from typing import Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.public.api.protos.ydb_value_pb2 import Type

import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as select_positive_common
import ydb.library.yql.providers.generic.connector.tests.utils.types.postgresql as postgresql
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
    _name = 'datetime'

    def _make_test_yql_postgresql(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_0_id',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(pg=postgresql.Int4()),
                ),
                # TODO: timestamp
                Column(
                    name='col_1_datetime64',
                    ydb_type=Type.TIMESTAMP,
                    data_source_type=DataSourceType(pg=postgresql.TimestampWithoutTimeZone()),
                ),
            ),
        )
        data_in = [
            # Date is OK for CH, but too early for YQL
            [
                1,
                datetime.datetime(1950, 5, 27, 12, 23, 45, 678910),
            ],
            # Date is OK for both CH and YQL
            [2, datetime.datetime(1988, 11, 20, 12, 23, 45, 678910)],
            # Date is OK for CH, but too late for YQL
            [
                3,
                datetime.datetime(2108, 1, 1, 12, 23, 45, 678910),
            ],
        ]

        data_out = [
            [
                1,
                None,
            ],
            [
                2,
                # datetime.datetime(1988, 11, 20, 12, 23, 45, 678000).astimezone(ZoneInfo('UTC')).replace(tzinfo=None),
                datetime.datetime(1988, 11, 20, 12, 23, 45, 678910),
            ],
            [
                3,
                None,
            ],
        ]

        test_case_name = self._name + '_YQL'

        return TestCase(
            name_=test_case_name,
            date_time_format=EDateTimeFormat.YQL_FORMAT,
            data_in=data_in,
            data_out_=data_out,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_source_kind=EDataSourceKind.POSTGRESQL,
            protocol=EProtocol.NATIVE,
            schema=schema,
            pragmas=dict(),
        )

    def _make_test_string_postgresql(self) -> TestCase:
        schema = Schema(
            columns=ColumnList(
                Column(
                    name='col_0_id',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(pg=postgresql.Int4()),
                ),
                # TODO: timestamp
                Column(
                    name='col_1_datetime64',
                    ydb_type=Type.TIMESTAMP,
                    data_source_type=DataSourceType(pg=postgresql.TimestampWithoutTimeZone()),
                ),
            ),
        )
        data_in = [
            [
                1,
                datetime.datetime(1950, 5, 27, 12, 23, 45, 678910),
            ],
            [2, datetime.datetime(1988, 11, 20, 12, 23, 45, 678910)],
            [
                3,
                datetime.datetime(2108, 1, 1, 12, 23, 45, 678910),
            ],
        ]

        data_out = [
            [
                1,
                '1950-05-27T12:23:45.67891Z',
            ],
            [
                2,
                '1988-11-20T12:23:45.67891Z',
            ],
            [
                3,
                '2108-01-01T12:23:45.67891Z',
            ],
        ]

        test_case_name = self._name + '_string'

        return TestCase(
            name_=test_case_name,
            date_time_format=EDateTimeFormat.STRING_FORMAT,
            data_in=data_in,
            data_out_=data_out,
            select_what=SelectWhat.asterisk(schema.columns),
            select_where=None,
            data_source_kind=EDataSourceKind.POSTGRESQL,
            protocol=EProtocol.NATIVE,
            schema=schema,
            pragmas=dict(),
        )

    def make_test_cases(self) -> Sequence[TestCase]:
        return [
            self._make_test_yql_postgresql(),
            self._make_test_string_postgresql(),
        ]
