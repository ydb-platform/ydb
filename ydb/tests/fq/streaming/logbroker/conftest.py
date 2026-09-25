import pytest

from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env
from ydb.tests.library.logbroker_federation import LogbrokerFederation


@pytest.fixture(scope="module")
def logbroker_federation():
    federation = LogbrokerFederation(
        accounts=["admin"],
        ydb_cluster_names=("cluster_a", "cluster_b", "cluster_c"),
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
    cluster = Kikimr(get_ydb_config(request))
    try:
        yield cluster
    finally:
        cluster.stop()
