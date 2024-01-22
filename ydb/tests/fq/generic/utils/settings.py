from os import environ
from dataclasses import dataclass
from typing import Optional

import yatest.common

from ydb.tests.tools.docker_compose_helpers.endpoint_determiner import EndpointDeterminer


@dataclass
class Settings:
    @dataclass
    class Connector:
        grpc_host: str
        grpc_port: int

    connector: Connector

    @dataclass
    class MdbMock:
        endpoint: str

    mdb_mock: MdbMock

    @dataclass
    class TokenAccessorMock:
        endpoint: str
        hmac_secret_file: str

    token_accessor_mock: TokenAccessorMock

    @dataclass
    class ClickHouse:
        dbname: str
        username: str
        password: str
        protocol: str

    clickhouse: ClickHouse

    @dataclass
    class PostgreSQL:
        dbname: str
        username: str
        password: Optional[str]

    postgresql: PostgreSQL

    @classmethod
    def from_env(cls) -> 'Settings':
        docker_compose_file = yatest.common.source_path('ydb/tests/fq/generic/docker-compose.yml')
        endpoint_determiner = EndpointDeterminer(docker_compose_file)

        s = cls(
            connector=cls.Connector(
                grpc_host='localhost',
                grpc_port=endpoint_determiner.get_port('connector', 50051),
            ),
            mdb_mock=cls.MdbMock(
                endpoint=environ['MDB_MOCK_ENDPOINT'],
            ),
            token_accessor_mock=cls.TokenAccessorMock(
                endpoint=environ['TOKEN_ACCESSOR_MOCK_ENDPOINT'],
                hmac_secret_file=environ['TOKEN_ACCESSOR_HMAC_SECRET_FILE'],
            ),
            clickhouse=cls.ClickHouse(
                dbname='db',
                username='user',
                password='password',
                protocol='native',
            ),
            postgresql=cls.PostgreSQL(
                dbname='db',
                username='user',
                password='password',
            ),
        )

        return s
