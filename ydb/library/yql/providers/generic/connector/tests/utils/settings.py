from dataclasses import dataclass, field
from typing import Optional, Sequence
import pathlib

import yatest.common

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind, EProtocol
from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.library.yql.providers.generic.connector.tests.utils.docker_compose import DockerComposeHelper


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
        host_external: str
        host_internal: str
        http_port_external: int
        http_port_internal: int
        native_port_external: int
        native_port_internal: int
        protocol: str

    clickhouse: ClickHouse

    @dataclass
    class MySQL:
        dbname: str
        cluster_name: str
        username: str
        password: Optional[str]  # TODO: why optional?
        host_external: str
        host_internal: str
        port_external: int
        port_internal: int

    mysql: MySQL

    @dataclass
    class Oracle:
        dbname: str
        cluster_name: str
        service_name: str
        username: str
        password: Optional[str]  # TODO: why optional?
        host_external: str
        host_internal: str
        port_external: int
        port_internal: int

    oracle: Oracle

    @dataclass
    class PostgreSQL:
        dbname: str
        cluster_name: str
        username: str
        password: Optional[str]  # TODO: why optional?
        host_external: str
        host_internal: str
        port_external: int
        port_internal: int

    postgresql: PostgreSQL

    @dataclass
    class Ydb:
        dbname: str
        cluster_name: str
        username: str
        password: str
        host_internal: str
        port_internal: int

    ydb: Ydb

    @classmethod
    def from_env(cls, docker_compose_dir: pathlib.Path, data_source_kinds: Sequence[EDataSourceKind]) -> 'Settings':
        docker_compose_file_relative_path = str(docker_compose_dir / 'docker-compose.yml')
        docker_compose_file_abs_path = yatest.common.source_path(docker_compose_file_relative_path)
        endpoint_determiner = DockerComposeHelper(docker_compose_file_abs_path)

        data_sources = dict()

        for data_source_kind in data_source_kinds:
            match data_source_kind:
                case EDataSourceKind.CLICKHOUSE:
                    data_sources[data_source_kind] = cls.ClickHouse(
                        cluster_name='clickhouse_integration_test',
                        host_external='0.0.0.0',
                        # This hack is due to YQ-3003.
                        # Previously we used container names instead of container ips:
                        # host_internal=docker_compose_file['services']['clickhouse']['container_name'],
                        host_internal=endpoint_determiner.get_internal_ip('clickhouse'),
                        http_port_external=endpoint_determiner.get_external_port('clickhouse', 8123),
                        native_port_external=endpoint_determiner.get_external_port('clickhouse', 9000),
                        http_port_internal=8123,
                        native_port_internal=9000,
                        username='user',
                        password='password',
                        protocol='native',
                    )
                case EDataSourceKind.MYSQL:
                    data_sources[data_source_kind] = cls.MySQL(
                        cluster_name='mysql_integration_test',
                        host_external='0.0.0.0',
                        # This hack is due to YQ-3003.
                        # Previously we used container names instead of container ips:
                        # host_internal=docker_compose_file['services']['mysql']['container_name'],
                        host_internal=endpoint_determiner.get_internal_ip('mysql'),
                        port_external=endpoint_determiner.get_external_port('mysql', 3306),
                        port_internal=3306,
                        dbname='db',
                        username='root',
                        password='password',
                    )
                case EDataSourceKind.ORACLE:
                    data_sources[data_source_kind] = cls.Oracle(
                        cluster_name='oracle_integration_test',
                        host_external='0.0.0.0',
                        # This hack is due to YQ-3003.
                        # Previously we used container names instead of container ips:
                        # host_internal=docker_compose_file['services']['mysql']['container_name'],
                        host_internal=endpoint_determiner.get_internal_ip('oracle'),
                        port_external=endpoint_determiner.get_external_port('oracle', 1521),
                        port_internal=1521,
                        dbname='db',
                        username='C##ADMIN',  # user that is created in oracle init. Maybe change to SYSTEM
                        password='password',
                        service_name="FREE",
                    )
                case EDataSourceKind.POSTGRESQL:
                    data_sources[data_source_kind] = cls.PostgreSQL(
                        cluster_name='postgresql_integration_test',
                        host_external='0.0.0.0',
                        # This hack is due to YQ-3003.
                        # Previously we used container names instead of container ips:
                        # host_internal=docker_compose_file['services']['postgresql']['container_name'],
                        host_internal=endpoint_determiner.get_internal_ip('postgresql'),
                        port_external=endpoint_determiner.get_external_port('postgresql', 5432),
                        port_internal=5432,
                        dbname='db',
                        username='user',
                        password='password',
                    )
                case EDataSourceKind.YDB:
                    data_sources[data_source_kind] = cls.Ydb(
                        cluster_name='ydb_integration_test',
                        host_internal=endpoint_determiner.get_container_name('ydb'),
                        port_internal=2136,
                        dbname="local",
                        username='user',
                        password='password',
                    )
                case _:
                    raise Exception(f'invalid data source: {data_source_kind}')

        return cls(
            clickhouse=data_sources.get(EDataSourceKind.CLICKHOUSE),
            connector=cls.Connector(
                grpc_host='localhost',
                grpc_port=endpoint_determiner.get_external_port('fq-connector-go', 2130),
                paging_bytes_per_page=4 * 1024 * 1024,
                paging_prefetch_queue_capacity=2,
            ),
            mysql=data_sources.get(EDataSourceKind.MYSQL),
            oracle=data_sources.get(EDataSourceKind.ORACLE),
            postgresql=data_sources.get(EDataSourceKind.POSTGRESQL),
            ydb=data_sources.get(EDataSourceKind.YDB),
        )

    def get_cluster_name(self, data_source_kind: EDataSourceKind) -> str:
        match data_source_kind:
            case EDataSourceKind.CLICKHOUSE:
                return self.clickhouse.cluster_name
            case EDataSourceKind.MYSQL:
                return self.mysql.cluster_name
            case EDataSourceKind.ORACLE:
                return self.oracle.cluster_name
            case EDataSourceKind.POSTGRESQL:
                return self.postgresql.cluster_name
            case EDataSourceKind.YDB:
                return self.ydb.cluster_name
            case _:
                raise Exception(f'invalid data source: {EDataSourceKind.Name(data_source_kind)}')


@dataclass
class GenericSettings:
    date_time_format: EDateTimeFormat

    @dataclass
    class ClickHouseCluster:
        def __hash__(self) -> int:
            return hash(self.database) + hash(self.protocol)

        database: str
        protocol: EProtocol

    clickhouse_clusters: Sequence[ClickHouseCluster] = field(default_factory=list)

    @dataclass
    class MySQLCluster:
        def __hash__(self) -> int:
            return hash(self.database)

        database: str

    mysql_clusters: Sequence[MySQLCluster] = field(default_factory=list)

    @dataclass
    class OracleCluster:
        def __hash__(self) -> int:
            return hash(self.database) + hash(self.service_name)

        database: str
        service_name: str

    oracle_clusters: Sequence[OracleCluster] = field(default_factory=list)

    @dataclass
    class PostgreSQLCluster:
        def __hash__(self) -> int:
            return hash(self.database) + hash(self.schema)

        database: str
        schema: str

    postgresql_clusters: Sequence[PostgreSQLCluster] = field(default_factory=list)

    @dataclass
    class YdbCluster:
        database: str

    ydb_clusters: Sequence[YdbCluster] = field(default_factory=list)
