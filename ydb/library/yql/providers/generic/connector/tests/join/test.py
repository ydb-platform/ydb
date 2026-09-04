import pytest

from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.run.runners import configure_runner

import conftest
import scenario
from collection import Collection
from test_case import TestCase

# Global collection of test cases dependent on environment
tc_collection = Collection(
    Settings.from_env(
        docker_compose_dir=conftest.docker_compose_dir,
        data_source_kinds=[EGenericDataSourceKind.CLICKHOUSE, EGenericDataSourceKind.POSTGRESQL],
    )
)


@pytest.mark.parametrize("test_case", tc_collection.get('join'), ids=tc_collection.ids('join'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clients")
def test_join(
    request: pytest.FixtureRequest,
    settings: Settings,
    clients: conftest.Clients,
    test_case: TestCase,
):
    runner = configure_runner(settings=settings)
    scenario.join(
        test_name=request.node.name,
        postgresql_client=clients.PostgreSQL,
        runner=runner,
        settings=settings,
        test_case=test_case,
    )
