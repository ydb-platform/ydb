import itertools
from dataclasses import dataclass
from typing import Sequence, Optional
from random import choice
from string import ascii_lowercase, digits

from utils.settings import GenericSettings

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
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
)
from ydb.library.yql.providers.generic.connector.tests.test_cases.base import BaseTestCase


@dataclass
class TestCase(BaseTestCase):
    schema: Schema
    data_in: Sequence
    select_what: SelectWhat
    pg_schema: str
    data_out_: Optional[Sequence] = None

    @property
    def data_out(self) -> Sequence:
        return self.data_out_ if self.data_out_ else self.data_in

    @property
    def generic_settings(self) -> GenericSettings:
        gs = super().generic_settings
        for cluster in gs.postgresql_clusters:
            cluster.schema = self.pg_schema
        return gs


class Factory:
    def _select_with_pg_schema(self) -> Sequence[TestCase]:
        pg_schema = choice(ascii_lowercase)
        pg_schema += ''.join(choice(ascii_lowercase + digits + '_') for i in range(8))

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

        select_what = SelectWhat.asterisk(column_list=schema.columns)
        test_case = TestCase(
            data_in=[[1, 2], [10, 20]],
            data_source_kind=EDataSourceKind.POSTGRESQL,
            database=Database.make_for_data_source_kind(EDataSourceKind.POSTGRESQL),
            select_what=select_what,
            schema=schema,
            data_out_=[[1, 2], [10, 20]],
            name=f'select_with_pg_schema_{EDataSourceKind.Name(EDataSourceKind.POSTGRESQL)}_{select_what}',
            pg_schema=pg_schema,
            pragmas=dict(),
        )

        return [test_case]

    def make_test_cases(self) -> Sequence[TestCase]:
        return list(
            itertools.chain(
                self._select_with_pg_schema(),
            )
        )
