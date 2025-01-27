import pytest

from yql.essentials.providers.common.proto.gateways_config_pb2 import EGenericDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.run.runners import runner_types, configure_runner
from ydb.library.yql.providers.generic.connector.tests.utils.one_time_waiter import OneTimeWaiter
import ydb.library.yql.providers.generic.connector.tests.utils.scenario.ydb as scenario
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as select_positive_common

# import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_database as select_missing_database
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_table as select_missing_table

from conftest import docker_compose_dir
from collection import Collection

one_time_waiter = OneTimeWaiter(
    data_source_kind=EGenericDataSourceKind.YDB,
    docker_compose_file_path=str(docker_compose_dir / 'docker-compose.yml'),
    expected_tables=[
        "column_selection_A_b_C_d_E",
        "column_selection_COL1",
        "column_selection_asterisk",
        "column_selection_col2_COL1",
        "column_selection_col2",
        "column_selection_col3",
        "primitive_types",
        "optional_types",
        "constant",
        "count",
        "pushdown",
        "unsupported_types",
        "json",
        "dummy_table",
        "json_document",
    ],
)

settings = Settings.from_env(docker_compose_dir=docker_compose_dir, data_source_kinds=[EGenericDataSourceKind.YDB])
tc_collection = Collection(settings)


@pytest.mark.parametrize("runner_type", runner_types)
@pytest.mark.parametrize("test_case", tc_collection.get('select_positive'), ids=tc_collection.ids('select_positive'))
def test_select_positive(
    request: pytest.FixtureRequest,
    runner_type: str,
    test_case: select_positive_common.TestCase,
):
    # Let YDB container initialize tables
    one_time_waiter.wait()

    runner = configure_runner(runner_type=runner_type, settings=settings)
    scenario.select_positive(
        settings=settings,
        runner=runner,
        test_case=test_case,
        test_name=request.node.name,
    )


# FIXME: YQ-3315
@pytest.mark.parametrize("runner_type", runner_types)
@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_table'), ids=tc_collection.ids('select_missing_table')
)
def test_select_missing_table(
    request: pytest.FixtureRequest,
    runner_type: str,
    test_case: select_missing_table.TestCase,
):
    # Let YDB container initialize tables
    one_time_waiter.wait()

    runner = configure_runner(runner_type=runner_type, settings=settings)
    scenario.select_missing_table(
        test_name=request.node.name,
        settings=settings,
        runner=runner,
        test_case=test_case,
    )
