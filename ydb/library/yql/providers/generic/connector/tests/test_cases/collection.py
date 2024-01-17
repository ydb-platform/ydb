from typing import Sequence, Mapping

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
import ydb.library.yql.providers.generic.connector.tests.test_cases.join as join
import ydb.library.yql.providers.generic.connector.tests.test_cases.select_datetime as select_datetime
import ydb.library.yql.providers.generic.connector.tests.test_cases.select_missing_database as select_missing_database
import ydb.library.yql.providers.generic.connector.tests.test_cases.select_missing_table as select_missing_table
import ydb.library.yql.providers.generic.connector.tests.test_cases.select_pg_schema as select_pg_schema
import ydb.library.yql.providers.generic.connector.tests.test_cases.select_positive_clickhouse as select_positive_clickhouse
import ydb.library.yql.providers.generic.connector.tests.test_cases.select_positive_common as select_positive_common
import ydb.library.yql.providers.generic.connector.tests.test_cases.select_positive_postgresql as select_positive_postgresql
from utils.settings import Settings


class Collection(object):
    _test_cases: Mapping[str, Sequence]

    def __init__(self, ss: Settings):
        self._test_cases = {
            'join': join.Factory().make_test_cases(),
            'select_missing_database': select_missing_database.Factory().make_test_cases(),
            'select_missing_table': select_missing_table.Factory().make_test_cases(),
            'select_positive_postgresql': select_positive_postgresql.Factory().make_test_cases()
            + select_positive_common.Factory(ss).make_test_cases(EDataSourceKind.POSTGRESQL),
            'select_positive_clickhouse': select_positive_clickhouse.Factory().make_test_cases()
            + select_positive_common.Factory(ss).make_test_cases(EDataSourceKind.CLICKHOUSE),
            'select_datetime': select_datetime.Factory().make_test_cases(),
            'select_pg_schema': select_pg_schema.Factory().make_test_cases(),
        }

    def get(self, key: str) -> Sequence:
        if key not in self._test_cases:
            raise ValueError(f'no such test: {key}')

        return self._test_cases[key]

    def ids(self, key: str) -> Sequence[str]:
        if key not in self._test_cases:
            raise ValueError(f'no such test: {key}')

        return map(lambda tc: tc.name, self._test_cases[key])
