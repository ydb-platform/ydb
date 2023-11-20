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
import utils.dqrun as dqrun
import utils.postgresql


# Global collection of test cases dependent on environment
tc_collection = TestCaseCollection(Settings.from_env())


@pytest.mark.parametrize("test_case", tc_collection.get('select_positive'), ids=tc_collection.ids('select_positive'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("dqrun_runner")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_positive(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_positive.TestCase,
):
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_positive(tmp_path, settings, dqrun_runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_positive(tmp_path, settings, dqrun_runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')


@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_database'), ids=tc_collection.ids('select_missing_database')
)
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("dqrun_runner")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_missing_database(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_missing_database.TestCase,
):
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_missing_table(tmp_path, settings, dqrun_runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_missing_table(tmp_path, settings, dqrun_runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')


@pytest.mark.parametrize(
    "test_case", tc_collection.get('select_missing_table'), ids=tc_collection.ids('select_missing_table')
)
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("dqrun_runner")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_missing_table(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_missing_table.TestCase,
):
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_missing_table(tmp_path, settings, dqrun_runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_missing_table(tmp_path, settings, dqrun_runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')


@pytest.mark.parametrize("test_case", tc_collection.get('join'), ids=tc_collection.ids('join'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("dqrun_runner")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_join(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.join.TestCase,
):
    join.join(
        tmp_path=tmp_path,
        clickhouse_client=clickhouse_client,
        postgresql_client=postgresql_client,
        dqrun_runner=dqrun_runner,
        settings=settings,
        test_case=test_case,
    )


@pytest.mark.parametrize("test_case", tc_collection.get('select_datetime'), ids=tc_collection.ids('select_datetime'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("dqrun_runner")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_datetime(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_positive.TestCase,
):
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_positive(tmp_path, settings, dqrun_runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_positive(tmp_path, settings, dqrun_runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')


@pytest.mark.parametrize("test_case", tc_collection.get('select_pg_schema'), ids=tc_collection.ids('select_pg_schema'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("dqrun_runner")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_pg_schema(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_positive.TestCase,
):
    postgresql.select_pg_schema(tmp_path, settings, dqrun_runner, postgresql_client, test_case)


@pytest.mark.parametrize("test_case", tc_collection.get('select_pushdown'), ids=tc_collection.ids('select_pushdown'))
@pytest.mark.usefixtures("settings")
@pytest.mark.usefixtures("dqrun_runner")
@pytest.mark.usefixtures("clickhouse_client")
@pytest.mark.usefixtures("postgresql_client")
def test_select_pushdown(
    tmp_path: Path,
    settings: Settings,
    dqrun_runner: dqrun.Runner,
    clickhouse_client: utils.clickhouse.Client,
    postgresql_client: utils.postgresql.Client,
    test_case: test_cases.select_positive.TestCase,
):
    match test_case.data_source_kind:
        case EDataSourceKind.CLICKHOUSE:
            clickhouse.select_positive(tmp_path, settings, dqrun_runner, clickhouse_client, test_case)
        case EDataSourceKind.POSTGRESQL:
            postgresql.select_positive(tmp_path, settings, dqrun_runner, postgresql_client, test_case)
        case _:
            raise Exception(f'invalid data source: {test_case.data_source_kind}')
