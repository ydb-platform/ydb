import hashlib
import random
from dataclasses import dataclass
from typing import Dict
import functools

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings


@dataclass
class BaseTestCase:
    name_: str
    data_source_kind: EDataSourceKind.ValueType
    pragmas: Dict[str, str]
    protocol: EProtocol

    @property
    def name(self) -> str:
        return f'{self.name_}_{EProtocol.Name(self.protocol)}'

    @property
    def database(self) -> Database:
        '''
        We want to create a distinct database on every test case
        '''
        return Database(self.name, self.data_source_kind)

    @functools.cached_property
    def _table_name(self) -> str:
        '''
        In general, we cannot use test case name as table name because of special symbols,
        so we provide a random table name instead.
        '''
        match self.data_source_kind:
            case EDataSourceKind.POSTGRESQL:
                return 't' + hashlib.sha256(str(random.randint(0, 65536)).encode('ascii')).hexdigest()[:8]
            case EDataSourceKind.CLICKHOUSE:
                return 't' + hashlib.sha256(str(random.randint(0, 65536)).encode('ascii')).hexdigest()[:8]
            case EDataSourceKind.YDB:
                return self.name

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
            case EDataSourceKind.YDB:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    ydb_clusters=[GenericSettings.YdbCluster(database=self.database.name)],
                )
            case _:
                raise Exception(f'invalid data source: {self.data_source_kind}')
