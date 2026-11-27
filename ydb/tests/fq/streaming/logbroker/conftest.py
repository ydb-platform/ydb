import pytest

from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env
from ydb.tests.library.logbroker_federation import LogbrokerFederation


@pytest.fixture(scope="module")
def logbroker_federation(request):
    federation = LogbrokerFederation(
        ydb_cluster_names=getattr(request, "param", ("cluster_a", "cluster_b", "cluster_c")),
        MockFederationDiscovery=True,
    )
    try:
        federation.start()
        yield federation
    finally:
        federation.stop()


@pytest.fixture(scope="module")
def kikimr(request, logbroker_federation):
    set_test_env(request)
    retry_max_time_ms = getattr(request, "param", {}).get("pq_read_actor_retry_policy_max_time_ms")
    with pytest.MonkeyPatch.context() as monkeypatch:
        if retry_max_time_ms is not None:
            monkeypatch.setenv("YDB_TEST_PQ_READ_ACTOR_RETRY_POLICY_MAX_TIME_MS", retry_max_time_ms)
        cluster = Kikimr(get_ydb_config(request))
        try:
            yield cluster
        finally:
            cluster.stop()
