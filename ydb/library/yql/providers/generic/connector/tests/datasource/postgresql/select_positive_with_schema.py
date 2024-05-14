import itertools
from dataclasses import dataclass
from typing import Sequence, Optional
from random import choice
from string import ascii_lowercase, digits


from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.public.api.protos.ydb_value_pb2 import Type

from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings
import ydb.library.yql.providers.generic.connector.tests.utils.types.postgresql as postgresql
from ydb.library.yql.providers.generic.connector.tests.utils.schema import (
    Schema,
    Column,
    ColumnList,
    DataSourceType,
    SelectWhat,
)
from ydb.library.yql.providers.generic.connector.tests.common_test_cases.base import BaseTestCase


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
                    data_source_type=DataSourceType(pg=postgresql.Int4()),
                ),
                Column(
                    name='col2',
                    ydb_type=Type.INT32,
                    data_source_type=DataSourceType(pg=postgresql.Int4()),
                ),
            )
        )

        select_what = SelectWhat.asterisk(column_list=schema.columns)

        test_case_name = f'select_positive_with_schema_{select_what}'

        test_case = TestCase(
            name_=test_case_name,
            data_in=[[1, 2], [10, 20]],
            data_source_kind=EDataSourceKind.POSTGRESQL,
            protocol=EProtocol.NATIVE,
            select_what=select_what,
            schema=schema,
            data_out_=[[1, 2], [10, 20]],
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
