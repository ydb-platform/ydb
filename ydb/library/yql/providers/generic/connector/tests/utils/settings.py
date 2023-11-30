from os import environ
from dataclasses import dataclass
from typing import Optional

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat


@dataclass
class Settings:
    @dataclass
    class Connector:
        grpc_host: str
        grpc_port: int

    connector: Connector

    @dataclass
    class ClickHouse:
        cluster_name: str
        username: str
        password: str
        host: str
        http_port: int
        native_port: int
        protocol: str

    clickhouse: ClickHouse

    @dataclass
    class PostgreSQL:
        dbname: str
        cluster_name: str
        username: str
        password: Optional[str]
        host: str
        port: int
        max_connections: int

    postgresql: PostgreSQL

    @classmethod
    def from_env(cls) -> 'Settings':
        return cls(
            connector=cls.Connector(
                grpc_host=environ['YQL_RECIPE_CONNECTOR_GRPC_HOST'],
                grpc_port=int(environ['YQL_RECIPE_CONNECTOR_GRPC_PORT']),
            ),
            clickhouse=cls.ClickHouse(
                cluster_name='clickhouse_integration_test',
                host=environ['RECIPE_CLICKHOUSE_HOST'],
                http_port=int(environ['RECIPE_CLICKHOUSE_HTTP_PORT']),
                native_port=int(environ['RECIPE_CLICKHOUSE_NATIVE_PORT']),
                username=environ['RECIPE_CLICKHOUSE_USER'],
                password=environ['RECIPE_CLICKHOUSE_PASSWORD'],
                protocol='native',
            ),
            postgresql=cls.PostgreSQL(
                cluster_name='postgresql_integration_test',
                host=environ['POSTGRES_RECIPE_HOST'],
                port=int(environ['POSTGRES_RECIPE_PORT']),
                dbname=environ['POSTGRES_RECIPE_DBNAME'],
                username=environ['POSTGRES_RECIPE_USER'],
                password=None,
                max_connections=int(environ['POSTGRES_RECIPE_MAX_CONNECTIONS']),
            ),
        )

    def get_cluster_name(self, data_source_kind: EDataSourceKind) -> str:
        match data_source_kind:
            case EDataSourceKind.CLICKHOUSE:
                return self.clickhouse.cluster_name
            case EDataSourceKind.POSTGRESQL:
                return self.postgresql.cluster_name
            case _:
                raise Exception(f'invalid data source: {data_source_kind}')


@dataclass
class GenericSettings:
    @dataclass
    class ClickHouseCluster:
        def __hash__(self) -> int:
            return hash(self.database) + hash(self.protocol)

        database: str
        protocol: EProtocol

    clickhouse_clusters: list[ClickHouseCluster]

    @dataclass
    class PostgreSQLCluster:
        def __hash__(self) -> int:
            return hash(self.database) + hash(self.schema)

        database: str
        schema: str

    postgresql_clusters: list[PostgreSQLCluster]

    date_time_format: EDateTimeFormat
