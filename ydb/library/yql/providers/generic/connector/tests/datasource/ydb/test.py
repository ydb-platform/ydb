import pytest
from datetime import datetime
import time
from typing import Sequence

import yatest.common

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.log import make_logger
from ydb.library.yql.providers.generic.connector.tests.utils.docker_compose import DockerComposeHelper
from ydb.library.yql.providers.generic.connector.tests.utils.run.runners import runner_types, configure_runner
import ydb.library.yql.providers.generic.connector.tests.utils.scenario.ydb as scenario
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_positive_common as select_positive_common

# import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_database as select_missing_database
import ydb.library.yql.providers.generic.connector.tests.common_test_cases.select_missing_table as select_missing_table

from conftest import docker_compose_dir
from collection import Collection

LOGGER = make_logger(__name__)


class OneTimeWaiter:
    __launched: bool = False

    def __init__(self, expected_tables: Sequence[str]):
        docker_compose_file_relative_path = str(docker_compose_dir / 'docker-compose.yml')
        docker_compose_file_abs_path = yatest.common.source_path(docker_compose_file_relative_path)
        self.docker_compose_helper = DockerComposeHelper(docker_compose_yml_path=docker_compose_file_abs_path)
        self.expected_tables = set(expected_tables)

    def wait(self):
        if self.__launched:
            return

        # This should be enough for tables to initialize
        start = datetime.now()

        timeout = 60
        while (datetime.now() - start).total_seconds() < timeout:
            self.actual_tables = set(self.docker_compose_helper.list_ydb_tables())

            # check if all the required tables have been created
            if self.expected_tables <= self.actual_tables:
                self.__launched = True
                return

            LOGGER.warning(f"Not enough YDB tables: expected={self.expected_tables}, actual={self.actual_tables}")
            time.sleep(5)

        raise ValueError(f"YDB was not able to initialize in {timeout} seconds, latest table set: {self.actual_tables}")


one_time_waiter = OneTimeWaiter(
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
    ]
)

settings = Settings.from_env(docker_compose_dir=docker_compose_dir, data_source_kinds=[EDataSourceKind.YDB])
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
