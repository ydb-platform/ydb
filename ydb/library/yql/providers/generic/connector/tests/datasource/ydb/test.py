import pytest

from ydb.library.yql.providers.generic.connector.api.service.protos.connector_pb2 import EDateTimeFormat
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings, GenericSettings
from ydb.library.yql.providers.generic.connector.tests.utils.run.runners import configure_runner


@pytest.mark.usefixtures("settings")
def test_select(
    settings: Settings,
):
    runner = configure_runner(runner_type='dqrun', settings=settings)
    yql_script = f"""
        SELECT * FROM {settings.ydb.cluster_name}.simple
    """

    result = runner.run(
        test_name="crab",
        script=yql_script,
        generic_settings=GenericSettings(
            clickhouse_clusters=[],
            postgresql_clusters=[],
            ydb_clusters=[GenericSettings.YdbCluster(database="local")],
            date_time_format=EDateTimeFormat.YQL_FORMAT,
        ),
    )

    assert result.returncode == 0, result.stderr
