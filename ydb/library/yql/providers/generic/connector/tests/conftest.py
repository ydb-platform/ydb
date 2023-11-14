from typing import TypeAlias

import grpc
import pytest

import ydb.library.yql.providers.generic.connector.api.service.connector_pb2_grpc as api
import yatest.common as yat

from utils.settings import Settings
import utils.clickhouse
import utils.dqrun as dqrun
import utils.postgresql


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env()


@pytest.fixture
def clickhouse_client(settings) -> utils.clickhouse.Client:
    return utils.clickhouse.MakeClient(settings.clickhouse)


@pytest.fixture
def postgresql_client(settings) -> utils.postgresql.Client:
    return utils.postgresql.Client(settings.postgresql)


ConnectorClient: TypeAlias = api.ConnectorStub


@pytest.fixture
def connector_client(settings) -> ConnectorClient:
    s = settings.connector

    channel = grpc.insecure_channel(f'{s.host}:{s.port}')
    stub = ConnectorClient(channel)
    return stub


@pytest.fixture
def dqrun_runner(settings) -> dqrun.Runner:
    return dqrun.Runner(dqrun_path=yat.build_path("ydb/library/yql/tools/dqrun/dqrun"), settings=settings)
