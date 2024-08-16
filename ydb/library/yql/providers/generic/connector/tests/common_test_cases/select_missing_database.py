from typing import List
from dataclasses import dataclass

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.tests.common_test_cases.base import BaseTestCase
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings


@dataclass
class TestCase(BaseTestCase):
    service_name: str = None

    @property
    def generic_settings(self) -> GenericSettings:
        gs = super().generic_settings

        # Overload setting for MySQL database
        if self.data_source_kind == EDataSourceKind.MYSQL:
            for cluster in gs.mysql_clusters:
                cluster.database = "missing_database"
        for cluster in gs.oracle_clusters:
            if self.service_name is not None:
                cluster.service_name = self.service_name

        if self.data_source_kind == EDataSourceKind.MS_SQL_SERVER:
            for cluster in gs.ms_sql_server_clusters:
                cluster.database = "missing_database"

        return gs


class Factory:
    ss: Settings

    def __init__(self, ss: Settings):
        self.ss = ss

    def make_test_cases(self, data_source_kind: EDataSourceKind) -> List[TestCase]:
        return [
            TestCase(
                name_="missing_database",
                data_source_kind=data_source_kind,
                protocol=EProtocol.NATIVE,
                pragmas=dict(),
                service_name=self.ss.oracle.service_name if self.ss.oracle is not None else None,
            )
        ]
