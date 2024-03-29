import pytest

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings
from ydb.library.yql.providers.generic.connector.tests.utils.run.runners import runner_types, configure_runner


@pytest.mark.usefixtures("settings")
def test_select(
    settings: Settings = settings,
):
    runner = configure_runner(runner_type='dqrun', settings=settings)
    yql_script = f"""
        SELECT * FROM {settings.Ydb.cluster_name}.simple
    """

    result = runner.run(
        test_name="crab",
        script=yql_script,
        generic_settings=None,
    )

    assert result.returncode == 0, result.stderr
