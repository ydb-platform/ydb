from typing import TypeAlias

import grpc
import pytest

import ydb.library.yql.providers.generic.connector.api.service.connector_pb2_grpc as api
import yatest.common as yat

from utils.settings import Settings
import utils.clickhouse
from utils.dqrun import DqRunner
from utils.kqprun import KqpRunner
from utils.runner import Runner
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


def configure_runner(runner, settings) -> Runner:
    if runner is DqRunner:
        return DqRunner(dqrun_path=yat.build_path("ydb/library/yql/tools/dqrun/dqrun"), settings=settings)
    elif runner is KqpRunner:
        return KqpRunner(kqprun_path=yat.build_path("ydb/tests/tools/kqprun/kqprun"), settings=settings)
    return None
