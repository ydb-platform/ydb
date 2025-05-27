from typing import Final
import pathlib

import pytest


from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.clients.postgresql import Client


docker_compose_dir: Final = pathlib.Path("ydb/library/yql/providers/generic/connector/tests/datasource/postgresql")


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(
        docker_compose_dir=docker_compose_dir, data_source_kinds=[EGenericDataSourceKind.POSTGRESQL]
    )


@pytest.fixture
def postgresql_client(settings) -> Client:
    return Client(settings.postgresql)
