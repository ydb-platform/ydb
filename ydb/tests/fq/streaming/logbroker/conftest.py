import pytest

from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env


@pytest.fixture(scope="module")
def kikimr(request):
    set_test_env(request)
    cluster = Kikimr(get_ydb_config(request))
    try:
        yield cluster
    finally:
        cluster.stop()
