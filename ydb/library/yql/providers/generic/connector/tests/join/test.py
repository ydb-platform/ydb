import pytest

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.runner import Runner
import ydb.library.yql.providers.generic.connector.tests.utils.dqrun as dqrun
import ydb.library.yql.providers.generic.connector.tests.utils.kqprun as kqprun

import conftest  # import configure_runner, docker_compose_dir, Clients, clients
import scenario
from collection import Collection
from test_case import TestCase

# Global collection of test cases dependent on environment
tc_collection = Collection(
    Settings.from_env(
        docker_compose_dir=conftest.docker_compose_dir,
        data_source_kinds=[EDataSourceKind.CLICKHOUSE, EDataSourceKind.POSTGRESQL],
    )
)

runners = (dqrun.DqRunner, kqprun.KqpRunner)
runners_ids = ("dqrun", "kqprun")


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize("test_case", tc_collection.get('join'), ids=tc_collection.ids('join'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clients")
def test_join(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: Runner,
    clients: conftest.Clients,
    test_case: TestCase,
):
    runner = conftest.configure_runner(runner=runner_type, settings=settings)
    scenario.join(
        test_name=request.node.name,
        clickhouse_client=clients.ClickHouse,
        postgresql_client=clients.PostgreSQL,
        runner=runner,
        settings=settings,
        test_case=test_case,
    )
