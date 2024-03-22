from pathlib import Path
import pytest


import ydb.library.yql.providers.generic.connector.tests.test_cases as test_cases
import ydb.library.yql.providers.generic.connector.tests.utils as utils

from utils.settings import Settings
from utils.runner import Runner
import utils.dqrun as dqrun
import utils.kqprun as kqprun

from conftest import configure_runner
import scenario

from test_cases.collection import Collection
import test_cases.select_missing_database
import test_cases.select_missing_table
import test_cases.select_positive_common


# Global collection of test cases dependent on environment
tc_collection = Collection(Settings.from_env())

runners = (dqrun.DqRunner, kqprun.KqpRunner)
runners_ids = ("dqrun", "kqprun")


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_positive_clickhouse'), ids=tc_collection.ids('select_positive_clickhouse')
)
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
def test_select_positive_clickhouse(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: Runner,
    clickhouse_client: utils.clickhouse.Client,
    test_case: test_cases.select_positive_common.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    scenario.select_positive(
        test_name=request.node.name, settings=settings, runner=runner, client=clickhouse_client, test_case=test_case
    )


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_database'), ids=tc_collection.ids('select_missing_database')
)
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
def test_select_missing_database(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: Runner,
    clickhouse_client: utils.clickhouse.Client,
    test_case: test_cases.select_missing_database.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    scenario.select_missing_table(
        settings=settings,
        runner=runner,
        client=clickhouse_client,
        test_case=test_case,
        test_name=request.node.name,
    )


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_table'), ids=tc_collection.ids('select_missing_table')
)
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
def test_select_missing_table(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: Runner,
    clickhouse_client: utils.clickhouse.Client,
    test_case: test_cases.select_missing_table.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    scenario.select_missing_table(
        test_name=request.node.name,
        settings=settings,
        runner=runner,
        client=clickhouse_client,
        test_case=test_case,
    )


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize("test_case", tc_collection.get('select_datetime'), ids=tc_collection.ids('select_datetime'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
def test_select_datetime(
    request: pytest.FixtureRequest,
    settings: Settings,
    runner_type: Runner,
    clickhouse_client: utils.clickhouse.Client,
    test_case: test_cases.select_positive_common.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    scenario.select_positive(
        test_name=request.node.name,
        test_case=test_case,
        settings=settings,
        runner=runner,
        client=clickhouse_client,
    )
