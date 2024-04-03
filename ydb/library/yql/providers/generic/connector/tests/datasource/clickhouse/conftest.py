from typing import Final
import pathlib

import pytest

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.clients.clickhouse import Client, make_client

docker_compose_dir: Final = pathlib.Path("ydb/library/yql/providers/generic/connector/tests/datasource/clickhouse")


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(docker_compose_dir=docker_compose_dir, data_source_kinds=[EDataSourceKind.CLICKHOUSE])


@pytest.fixture
def clickhouse_client(settings) -> Client:
    cl = make_client(settings.clickhouse)
    yield cl
    cl.close()
