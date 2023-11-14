import itertools
from typing import List, Sequence, Tuple

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.test_cases.base import BaseTestCase


TestCase = BaseTestCase


class Factory:
    def __basic_cases(self) -> Sequence[Tuple[str, Database]]:
        return (('missing_database', Database()),)

    def make_test_cases(self) -> List[TestCase]:
        data_source_kinds = (
            EDataSourceKind.CLICKHOUSE,
            EDataSourceKind.POSTGRESQL,
        )

        product = itertools.product(self.__basic_cases(), data_source_kinds)

        test_cases = []
        for p in product:
            test_case = TestCase(name=p[0][0], database=p[0][1], data_source_kind=p[1], pragmas=dict())

            # modify data source kind
            test_case.data_source_kind = test_case.data_source_kind
            test_case.name += f'_{EDataSourceKind.Name(test_case.data_source_kind)}'

            test_cases.append(test_case)

        return test_cases
