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
                    # NOTE: YQ-2264: doesn't work for PostgreSQL because of implicit cast to lowercase (COL1 -> col1)
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
                    # NOTE: YQ-2264: doesn't work for PostgreSQL because of implicit cast to lowercase (COL1 -> col1)
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
                    # NOTE: YQ-2264: doesn't work for PostgreSQL because of implicit cast to lowercase (COL1 -> col1)
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

    def make_test_cases(self, data_source_kind: EDataSourceKind) -> Sequence[TestCase]:
        protocols = {
            EDataSourceKind.CLICKHOUSE: [EProtocol.NATIVE, EProtocol.HTTP],
            EDataSourceKind.POSTGRESQL: [EProtocol.NATIVE],
        }

        base_test_cases = list(
            itertools.chain(
                self._column_selection(),
            )
        )

        test_cases = []
        for base_tc in base_test_cases:
            if base_tc != data_source_kind:
                continue
            for protocol in protocols[base_tc.data_source_kind]:
                tc = replace(base_tc)
                tc.name += f'_{protocol}'
                tc.protocol = protocol
                test_cases.append(tc)
        return test_cases
