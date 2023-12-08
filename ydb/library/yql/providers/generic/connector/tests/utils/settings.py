from os import environ
from dataclasses import dataclass
from typing import Optional, Sequence

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat


@dataclass
class Settings:
    @dataclass
    class Connector:
        grpc_host: str
        grpc_port: int
        paging_bytes_per_page: int
        paging_prefetch_queue_capacity: int

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

    postgresql: PostgreSQL

    @classmethod
    def from_env(cls) -> 'Settings':
        return cls(
            connector=cls.Connector(
                grpc_host=environ['YDB_CONNECTOR_RECIPE_GRPC_HOST'],
                grpc_port=int(environ['YDB_CONNECTOR_RECIPE_GRPC_PORT']),
                paging_bytes_per_page=int(environ['YDB_CONNECTOR_RECIPE_GRPC_PAGING_BYTES_PER_PAGE']),
                paging_prefetch_queue_capacity=int(environ['YDB_CONNECTOR_RECIPE_GRPC_PAGING_PREFETCH_QUEUE_CAPACITY']),
            ),
            clickhouse=cls.ClickHouse(
                cluster_name='clickhouse_integration_test',
                host='localhost',
                http_port=18123,
                native_port=19000,
                username='user',
                password='password',
                protocol='native',
            ),
            postgresql=cls.PostgreSQL(
                cluster_name='postgresql_integration_test',
                host='localhost',
                port=15432,
                dbname='db',
                username='user',
                password='password',
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

    clickhouse_clusters: Sequence[ClickHouseCluster]

    @dataclass
    class PostgreSQLCluster:
        def __hash__(self) -> int:
            return hash(self.database) + hash(self.schema)

        database: str
        schema: str

    postgresql_clusters: Sequence[PostgreSQLCluster]

    date_time_format: EDateTimeFormat
