from typing import List

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.test_cases.base import BaseTestCase


TestCase = BaseTestCase


class Factory:
    def make_test_cases(self) -> List[TestCase]:
        data_source_kinds = (
            EDataSourceKind.CLICKHOUSE,
            EDataSourceKind.POSTGRESQL,
        )

        test_cases = []
        for data_source_kind in data_source_kinds:
            test_case_name = 'missing_database'

            test_case = TestCase(
                name=test_case_name,
                database=Database(name=test_case_name, kind=data_source_kind),
                data_source_kind=data_source_kind,
                pragmas=dict(),
            )

            test_cases.append(test_case)

        return test_cases
