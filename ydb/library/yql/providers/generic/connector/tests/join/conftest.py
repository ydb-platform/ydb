from typing import Final
import dataclasses
import pathlib

import pytest

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.clients.clickhouse import (
    make_client as make_clickhouse_client,
    Client as ClickHouseClient,
)
from ydb.library.yql.providers.generic.connector.tests.utils.clients.postgresql import Client as PostgreSQLClient
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings


docker_compose_dir: Final = pathlib.Path("ydb/library/yql/providers/generic/connector/tests/join")


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(
        docker_compose_dir=docker_compose_dir,
        data_source_kinds=[EDataSourceKind.POSTGRESQL, EDataSourceKind.CLICKHOUSE],
    )


@dataclasses.dataclass
class Clients:
    ClickHouse: ClickHouseClient
    PostgreSQL: PostgreSQLClient


@pytest.fixture
def clients(settings):
    return Clients(
        ClickHouse=make_clickhouse_client(settings=settings.clickhouse),
        PostgreSQL=PostgreSQLClient(settings=settings.postgresql),
    )
