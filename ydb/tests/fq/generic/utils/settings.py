from os import environ
from dataclasses import dataclass
from typing import Optional

import yaml
import yatest.common

from ydb.tests.fq.generic.utils.endpoint_determiner import EndpointDeterminer


@dataclass
class Settings:
    # infrastructure services

    @dataclass
    class Connector:
        grpc_host: str
        grpc_port: int

    connector: Connector

    @dataclass
    class TokenAccessorMock:
        endpoint: str
        hmac_secret_file: str

    token_accessor_mock: TokenAccessorMock

    @dataclass
    class MdbMock:
        endpoint: str

    mdb_mock: Optional[MdbMock] = None

    # databases

    @dataclass
    class ClickHouse:
        dbname: str
        username: str
        password: str
        protocol: str

    clickhouse: Optional[ClickHouse] = None

    @dataclass
    class Greenplum:
        dbname: str
        username: str
        password: str

    greenplum: Optional[Greenplum] = None

    @dataclass
    class PostgreSQL:
        dbname: str
        username: str
        password: Optional[str]

    postgresql: Optional[PostgreSQL] = None

    @dataclass
    class Ydb:
        dbname: str
        username: str
        password: str

    ydb: Optional[Ydb] = None

    @classmethod
    def from_env(cls, docker_compose_file_path: str) -> 'Settings':
        docker_compose_file_abs_path = yatest.common.source_path(docker_compose_file_path)
        endpoint_determiner = EndpointDeterminer(docker_compose_file_abs_path)

        with open(docker_compose_file_abs_path, 'r') as f:
            docker_compose_yml_data = yaml.load(f, Loader=yaml.SafeLoader)

        s = cls(
            connector=cls.Connector(
                grpc_host='localhost',
                grpc_port=endpoint_determiner.get_port('fq-connector-go', 2130),
            ),
            token_accessor_mock=cls.TokenAccessorMock(
                endpoint=environ['TOKEN_ACCESSOR_MOCK_ENDPOINT'],
                hmac_secret_file=environ['TOKEN_ACCESSOR_HMAC_SECRET_FILE'],
            ),
        )

        if 'MDB_MOCK_ENDPOINT' in environ.keys():
            s.mdb_mock = cls.MdbMock(
                endpoint=environ['MDB_MOCK_ENDPOINT'],
            )

        if 'clickhouse' in docker_compose_yml_data['services']:
            s.clickhouse = cls.ClickHouse(
                dbname='db',
                username='user',
                password='password',
                protocol='native',
            )

        if 'greenplum' in docker_compose_yml_data['services']:
            s.greenplum = cls.Greenplum(
                dbname='template1',
                username='gpadmin',
                password='123456',
            )

        if 'postgresql' in docker_compose_yml_data['services']:
            s.postgresql = cls.PostgreSQL(
                dbname='db',
                username='user',
                password='password',
            )

        if 'ydb' in docker_compose_yml_data['services']:
            s.ydb = cls.Ydb(dbname='local', username='user', password='password')

        return s
