from typing import List

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.tests.common_test_cases.base import BaseTestCase


TestCase = BaseTestCase


class Factory:
    def make_test_cases(self, data_source_kind: EDataSourceKind) -> List[TestCase]:
        test_cases = []

        test_case_name = 'missing_table'

        test_case = TestCase(
            name_=test_case_name,
            data_source_kind=data_source_kind,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
        )

        test_cases.append(test_case)

        return test_cases
