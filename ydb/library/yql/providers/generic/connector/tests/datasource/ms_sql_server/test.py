from typing import Set
from datetime import datetime
import time

import pytest

import yatest.common

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.docker_compose import DockerComposeHelper
from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger
from ydb.library.yql.providers.generic.connector.tests.utils.run.runners import runner_types, configure_runner
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings

from conftest import docker_compose_dir
from collection import Collection

import ydb.library.yql.providers.generic.connector.tests.utils.scenario.ms_sql_server as scenario
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_database as select_missing_database
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_table as select_missing_table
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as select_positive_common

LOGGER = make_logger(__name__)


# Global collection of test cases dependent on environment
tc_collection = Collection(
    Settings.from_env(docker_compose_dir=docker_compose_dir, data_source_kinds=[EDataSourceKind.MS_SQL_SERVER])
)


class OneTimeWaiter:
    __launched: bool = False

    __expected_tables: Set[str] = {
        'column_selection_A_b_C_d_E',
        'column_selection_COL1',
        'column_selection_col1',
        'column_selection_asterisk',
        'column_selection_col2_COL1',
        'column_selection_col2_col1',
        'column_selection_col2',
        'column_selection_col3',
        'primitives',
        'constant',
        'count_rows',
        'pushdown',
        'json',
        'datetimes',
    }

    def __init__(self):
        docker_compose_file_relative_path = str(docker_compose_dir / 'docker-compose.yml')
        docker_compose_file_abs_path = yatest.common.source_path(docker_compose_file_relative_path)
        self.docker_compose_helper = DockerComposeHelper(docker_compose_yml_path=docker_compose_file_abs_path)

    def wait(self):
        if self.__launched:
            return

        start = datetime.now()
        actual_tables: Set[str] = None
        timeout = 60

        while (datetime.now() - start).total_seconds() < timeout:
            try:
                actual_tables = set(self.docker_compose_helper.list_ms_sql_server_tables())
            except Exception as e:
                LOGGER.error(f"list ms_sql_server tables error: {e}")
                time.sleep(5)
            else:
                if self.__expected_tables.issubset(actual_tables):
                    self.__launched = True
                    return

        raise RuntimeError(f"No expected tables in ms_sql_server. Latest result: {actual_tables}")


one_time_waiter = OneTimeWaiter()


@pytest.mark.parametrize("runner_type", runner_types)
@pytest.mark.parametrize("test_case", tc_collection.get('select_positive'), ids=tc_collection.ids('select_positive'))
@pytest.mark.usefixtures("settings")
def test_select_positive(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: str,
    test_case: select_positive_common.TestCase,
):
    # Let ms_sql_server initialize
    one_time_waiter.wait()

    runner = configure_runner(runner_type=runner_type, settings=settings)
    scenario.select_positive(
        settings=settings,
        runner=runner,
        test_case=test_case,
        test_name=request.node.name,
    )


@pytest.mark.parametrize("runner_type", runner_types)
@pytest.mark.parametrize("test_case", tc_collection.get('select_datetime'), ids=tc_collection.ids('select_datetime'))
@pytest.mark.usefixtures("settings")
def test_select_datetime(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: str,
    test_case: select_positive_common.TestCase,
):
    # Let ms_sql_server initialize
    one_time_waiter.wait()

    runner = configure_runner(runner_type=runner_type, settings=settings)
    scenario.select_positive(
        settings=settings,
        runner=runner,
        test_case=test_case,
        test_name=request.node.name,
    )


@pytest.mark.parametrize("runner_type", runner_types)
@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_database'), ids=tc_collection.ids('select_missing_database')
)
@pytest.mark.usefixtures("settings")
def test_select_missing_database(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: str,
    test_case: select_missing_database.TestCase,
):
    # Let ms_sql_server initialize
    one_time_waiter.wait()

    runner = configure_runner(runner_type=runner_type, settings=settings)
    scenario.select_missing_database(
        settings=settings,
        runner=runner,
        test_case=test_case,
        test_name=request.node.name,
    )


@pytest.mark.parametrize("runner_type", runner_types)
@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_table'), ids=tc_collection.ids('select_missing_table')
)
@pytest.mark.usefixtures("settings")
def test_select_missing_table(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: str,
    test_case: select_missing_table.TestCase,
):
    # Let ms_sql_server initialize
    one_time_waiter.wait()

    runner = configure_runner(runner_type=runner_type, settings=settings)
    scenario.select_missing_table(
        test_name=request.node.name,
        settings=settings,
        runner=runner,
        test_case=test_case,
    )
