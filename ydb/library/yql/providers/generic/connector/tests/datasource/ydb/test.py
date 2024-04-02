import pytest
import time

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings

# from ydb.library.yql.providers.generic.connector.tests.utils.run.runners import runner_types, configure_runner
from ydb.library.yql.providers.generic.connector.tests.utils.run.runners import configure_runner
import ydb.library.yql.providers.generic.connector.tests.utils.scenario.ydb as scenario

from conftest import docker_compose_dir
from collection import Collection

import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as select_positive_common

settings = Settings.from_env(docker_compose_dir=docker_compose_dir, data_source_kinds=[EDataSourceKind.YDB])
tc_collection = Collection(settings)


# @pytest.mark.parametrize("runner_type", runner_types)
@pytest.mark.parametrize("runner_type", ['dqrun'])
@pytest.mark.parametrize("test_case", tc_collection.get('select_positive'), ids=tc_collection.ids('select_positive'))
def test_select_positive(
    request: pytest.FixtureRequest,
    runner_type: str,
    test_case: select_positive_common.TestCase,
):
    time.sleep(2)
    runner = configure_runner(runner_type=runner_type, settings=settings)
    scenario.select_positive(
        settings=settings,
        runner=runner,
        test_case=test_case,
        test_name=request.node.name,
    )
