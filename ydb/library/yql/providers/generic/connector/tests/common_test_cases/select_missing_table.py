from typing import List
from dataclasses import dataclass
from typing import Sequence

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.tests.common_test_cases.base import BaseTestCase
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings


@dataclass
class TestCase(BaseTestCase):
    service_name: str = None

    @property
    def data_out(self) -> Sequence:
        return self.data_out_ if self.data_out_ else self.data_in

    @property
    def generic_settings(self) -> GenericSettings:
        gs = super().generic_settings
        for cluster in gs.oracle_clusters:
            if self.service_name is not None:
                cluster.service_name = self.service_name
        return gs


class Factory:
    ss: Settings

    def __init__(self, ss: Settings):
        self.ss = ss

    def make_test_cases(self, data_source_kind: EDataSourceKind) -> List[TestCase]:
        test_cases = []

        test_case_name = 'missing_table'

        test_case = TestCase(
            name_=test_case_name,
            data_source_kind=data_source_kind,
            protocol=EProtocol.NATIVE,
            pragmas=dict(),
            service_name=self.ss.oracle.service_name if self.ss.oracle is not None else None,
        )

        test_cases.append(test_case)

        return test_cases
