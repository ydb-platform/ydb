from typing import List
from dataclasses import dataclass

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.tests.common_test_cases.base import BaseTestCase
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings


@dataclass
class TestCase(BaseTestCase):
    @property
    def generic_settings(self) -> GenericSettings:
        gs = super().generic_settings

        # Overload setting for MySQL database
        if self.data_source_kind == EDataSourceKind.MYSQL:
            for cluster in gs.mysql_clusters:
                cluster.database = "missing_database"

        return gs


class Factory:
    def make_test_cases(self, data_source_kind: EDataSourceKind) -> List[TestCase]:
        return [
            TestCase(
                name_="missing_database",
                data_source_kind=data_source_kind,
                protocol=EProtocol.NATIVE,
                pragmas=dict(),
            )
        ]
