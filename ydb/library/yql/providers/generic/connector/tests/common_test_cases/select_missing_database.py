from typing import List

from ydb.library.yql.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind, EGenericProtocol
from ydb.library.yql.providers.generic.connector.tests.common_test_cases.base import BaseTestCase


TestCase = BaseTestCase


class Factory:
    def make_test_cases(self, data_source_kind: EGenericDataSourceKind) -> List[TestCase]:
        test_cases = []

        test_case_name = 'missing_database'

        test_case = TestCase(
            name_=test_case_name,
            data_source_kind=data_source_kind,
            protocol=EGenericProtocol.NATIVE,
            pragmas=dict(),
        )

        test_cases.append(test_case)

        return test_cases
