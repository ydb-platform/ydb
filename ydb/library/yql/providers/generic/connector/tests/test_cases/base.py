import hashlib
from dataclasses import dataclass
from typing import Dict

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings


@dataclass
class BaseTestCase:
    name: str
    database: Database
    data_source_kind: EDataSourceKind.ValueType
    pragmas: Dict[str, str]

    @property
    def _table_name(self) -> str:
        '''
        We cannot use test case name as table name because of special symbols,
        so we hash it, convert to hex and take first N symbols.
        Than we optionally add database prefix to it.
        '''
        return 't' + hashlib.sha256(self.name.encode('utf-8')).hexdigest()[:8]

    @property
    def sql_table_name(self) -> str:
        return self._table_name

    @property
    def qualified_table_name(self) -> str:
        return self._table_name

    @property
    def pragmas_sql_string(self) -> str:
        result: str = ''
        for name, value in self.pragmas.items():
            result += f'PRAGMA {name}="{value}";\n'
        return result

    @property
    def generic_settings(self) -> GenericSettings:
        match self.data_source_kind:
            case EDataSourceKind.CLICKHOUSE:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    clickhouse_clusters=[
                        GenericSettings.ClickHouseCluster(database=self.database.name, protocol=EProtocol.NATIVE)
                    ],
                    postgresql_clusters=[],
                )

            case EDataSourceKind.POSTGRESQL:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    clickhouse_clusters=[],
                    postgresql_clusters=[GenericSettings.PostgreSQLCluster(database=self.database.name, schema=None)],
                )
            case _:
                raise Exception(f'invalid data source: {self.data_source_kind}')
