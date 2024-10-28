from typing import Final
import dataclasses
import pathlib

import pytest

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.clients.postgresql import Client as PostgreSQLClient
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings


docker_compose_dir: Final = pathlib.Path("ydb/library/yql/providers/generic/connector/tests/join")


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(
        docker_compose_dir=docker_compose_dir,
        data_source_kinds=[EDataSourceKind.POSTGRESQL, EDataSourceKind.CLICKHOUSE],
    )


# TODO: avoid using clients, initialize
@dataclasses.dataclass
class Clients:
    PostgreSQL: PostgreSQLClient


@pytest.fixture
def clients(settings):
    return Clients(
        PostgreSQL=PostgreSQLClient(settings=settings.postgresql),
    )
