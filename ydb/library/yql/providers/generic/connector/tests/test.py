from pathlib import Path
import pytest

from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind

from utils.settings import Settings
import clickhouse
import join
import postgresql
from test_cases.collection import TestCaseCollection
import test_cases.join
import test_cases.select_missing_database
import test_cases.select_missing_table
import test_cases.select_positive
import utils.clickhouse
from utils.runner import Runner
from conftest import configure_runner
import utils.dqrun as dqrun
import utils.kqprun as kqprun
import utils.postgresql


# Global collection of test cases dependent on environment
tc_collection = TestCaseCollection(Settings.from_env())

runners = (dqrun.DqRunner, kqprun.KqpRunner)
runners_ids = ("dqrun", "kqprun")


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize("test_case", tc_collection.get('select_positive'), ids=tc_collection.ids('select_positive'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_positive(
    tmp_path: Path,
    settings: Settings,
    runner_type: Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_positive.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_positive(tmp_path, settings, runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_positive(tmp_path, settings, runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_database'), ids=tc_collection.ids('select_missing_database')
)
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_missing_database(
    tmp_path: Path,
    settings: Settings,
    runner_type: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_missing_database.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_missing_table(tmp_path, settings, runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_missing_table(tmp_path, settings, runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_table'), ids=tc_collection.ids('select_missing_table')
)
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_missing_table(
    tmp_path: Path,
    settings: Settings,
    runner_type: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_missing_table.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_missing_table(tmp_path, settings, runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_missing_table(tmp_path, settings, runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize("test_case", tc_collection.get('join'), ids=tc_collection.ids('join'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_join(
    tmp_path: Path,
    settings: Settings,
    runner_type: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.join.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    join.join(
        tmp_path=tmp_path,
        clickhouse_client=clickhouse_client,
        postgresql_client=postgresql_client,
        runner=runner,
        settings=settings,
        test_case=test_case,
    )


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize("test_case", tc_collection.get('select_datetime'), ids=tc_collection.ids('select_datetime'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_datetime(
    tmp_path: Path,
    settings: Settings,
    runner_type: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_positive.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_positive(tmp_path, settings, runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_positive(tmp_path, settings, runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize("test_case", tc_collection.get('select_pg_schema'), ids=tc_collection.ids('select_pg_schema'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_pg_schema(
    tmp_path: Path,
    settings: Settings,
    runner_type: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_positive.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    postgresql.select_pg_schema(tmp_path, settings, runner, postgresql_client, test_case)


@pytest.mark.parametrize("runner_type", runners, ids=runners_ids)
@pytest.mark.parametrize("test_case", tc_collection.get('select_pushdown'), ids=tc_collection.ids('select_pushdown'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_pushdown(
    tmp_path: Path,
    settings: Settings,
    runner_type: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_positive.TestCase,
):
    runner = configure_runner(runner=runner_type, settings=settings)
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_positive(tmp_path, settings, runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_positive(tmp_path, settings, runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')
