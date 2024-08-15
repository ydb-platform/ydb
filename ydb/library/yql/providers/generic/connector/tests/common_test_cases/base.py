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
        match self.data_source_kind:
            case EDataSourceKind.CLICKHOUSE:
                # ClickHouse has two kinds of network protocols: NATIVE and HTTP,
                # so we append protocol name to the test case name
                return f'{self.name_}_{EProtocol.Name(self.protocol)}'
            case EDataSourceKind.MS_SQL_SERVER:
                return self.name_
            case EDataSourceKind.MYSQL:
                return self.name_
            case EDataSourceKind.ORACLE:
                return self.name_
            case EDataSourceKind.POSTGRESQL:
                return self.name_
            case EDataSourceKind.YDB:
                return self.name_
            case _:
                raise Exception(f'invalid data source: {self.data_source_kind}')

    @property
    def database(self) -> Database:
        '''
        For PG/CH we create a distinct database on every test case.
        For YDB/MySQL/Microsoft SQL Server we use single predefined database.
        '''
        # FIXME: do not hardcode databases here
        match self.data_source_kind:
            case EDataSourceKind.CLICKHOUSE:
                return Database(self.name, self.data_source_kind)
            case EDataSourceKind.MS_SQL_SERVER:
                return Database("master", self.data_source_kind)
            case EDataSourceKind.MYSQL:
                return Database("db", self.data_source_kind)
            case EDataSourceKind.ORACLE:
                return Database(self.name, self.data_source_kind)
            case EDataSourceKind.POSTGRESQL:
                return Database(self.name, self.data_source_kind)
            case EDataSourceKind.YDB:
                return Database("local", self.data_source_kind)

    @functools.cached_property
    def table_name(self) -> str:
        '''
        For some kinds of RDBMS we cannot use test case name as table name because of special symbols,
        so we provide a random table name instead where necessary.
        '''
        match self.data_source_kind:
            case EDataSourceKind.CLICKHOUSE:
                return 't' + make_random_string(8)
            case EDataSourceKind.MYSQL:
                return self.name
            case EDataSourceKind.ORACLE:
                return self.name
            case EDataSourceKind.POSTGRESQL:
                return 't' + make_random_string(8)
            case EDataSourceKind.YDB:
                return self.name
            case _:
                raise Exception(f'invalid data source: {self.data_source_kind}')

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
                )
            case EDataSourceKind.MYSQL:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    mysql_clusters=[GenericSettings.MySQLCluster(database=self.database.name)],
                )
            case EDataSourceKind.ORACLE:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    oracle_clusters=[GenericSettings.OracleCluster(database=self.database.name, service_name=None)],
                )
            case EDataSourceKind.POSTGRESQL:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    postgresql_clusters=[GenericSettings.PostgreSQLCluster(database=self.database.name, schema=None)],
                )
            case EDataSourceKind.YDB:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    ydb_clusters=[GenericSettings.YdbCluster(database=self.database.name)],
                )
            case _:
                raise Exception(f'invalid data source: {self.data_source_kind}')


def make_random_string(length: int) -> str:
    return hashlib.sha256(str(random.randint(0, 65536)).encode('ascii')).hexdigest()[:length]
