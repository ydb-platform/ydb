import hashlib
import random
from dataclasses import dataclass
from typing import Dict
import functools

from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind, EGenericProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.library.yql.providers.generic.connector.tests.utils.database import Database
from ydb.library.yql.providers.generic.connector.tests.utils.settings import GenericSettings


@dataclass
class BaseTestCase:
    name_: str
    data_source_kind: EGenericDataSourceKind.ValueType
    pragmas: Dict[str, str]
    protocol: EGenericProtocol

    @property
    def name(self) -> str:
        match self.data_source_kind:
            case EGenericDataSourceKind.CLICKHOUSE:
                # ClickHouse has two kinds of network protocols: NATIVE and HTTP,
                # so we append protocol name to the test case name
                return f'{self.name_}_{EGenericProtocol.Name(self.protocol)}'
            case EGenericDataSourceKind.MS_SQL_SERVER:
                return self.name_
            case EGenericDataSourceKind.MYSQL:
                return self.name_
            case EGenericDataSourceKind.ORACLE:
                return self.name_
            case EGenericDataSourceKind.POSTGRESQL:
                return self.name_
            case EGenericDataSourceKind.YDB:
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
            case EGenericDataSourceKind.CLICKHOUSE:
                return Database(self.name, self.data_source_kind)
            case EGenericDataSourceKind.MS_SQL_SERVER:
                return Database("master", self.data_source_kind)
            case EGenericDataSourceKind.MYSQL:
                return Database("db", self.data_source_kind)
            case EGenericDataSourceKind.ORACLE:
                return Database(self.name, self.data_source_kind)
            case EGenericDataSourceKind.POSTGRESQL:
                return Database(self.name, self.data_source_kind)
            case EGenericDataSourceKind.YDB:
                return Database("local", self.data_source_kind)

    @functools.cached_property
    def table_name(self) -> str:
        '''
        For some kinds of RDBMS we cannot use test case name as table name because of special symbols,
        so we provide a random table name instead where necessary.
        '''
        match self.data_source_kind:
            case EGenericDataSourceKind.CLICKHOUSE:
                return self.name_  # without protocol
            case EGenericDataSourceKind.MS_SQL_SERVER:
                return self.name
            case EGenericDataSourceKind.MYSQL:
                return self.name
            case EGenericDataSourceKind.ORACLE:
                return self.name
            case EGenericDataSourceKind.POSTGRESQL:
                return 't' + make_random_string(8)
            case EGenericDataSourceKind.YDB:
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
            case EGenericDataSourceKind.CLICKHOUSE:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    clickhouse_clusters=[
                        GenericSettings.ClickHouseCluster(database=self.database.name, protocol=EGenericProtocol.NATIVE)
                    ],
                )
            case EGenericDataSourceKind.MS_SQL_SERVER:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    ms_sql_server_clusters=[GenericSettings.MsSQLServerCluster(database=self.database.name)],
                )
            case EGenericDataSourceKind.MYSQL:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    mysql_clusters=[GenericSettings.MySQLCluster(database=self.database.name)],
                )
            case EGenericDataSourceKind.ORACLE:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    oracle_clusters=[GenericSettings.OracleCluster(database=self.database.name, service_name=None)],
                )
            case EGenericDataSourceKind.POSTGRESQL:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    postgresql_clusters=[GenericSettings.PostgreSQLCluster(database=self.database.name, schema=None)],
                )
            case EGenericDataSourceKind.YDB:
                return GenericSettings(
                    date_time_format=EDateTimeFormat.YQL_FORMAT,
                    ydb_clusters=[GenericSettings.YdbCluster(database=self.database.name)],
                )
            case _:
                raise Exception(f'invalid data source: {self.data_source_kind}')


def make_random_string(length: int) -> str:
    return hashlib.sha256(str(random.randint(0, 65536)).encode('ascii')).hexdigest()[:length]
