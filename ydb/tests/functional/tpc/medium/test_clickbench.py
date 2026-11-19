import ydb.tests.olap.load.lib.clickbench as clickbench
import pytest
import yatest.common
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


@pytest.fixture(scope="module")
def clickbench_cluster():
    class ClusterOwner(FunctionalTestBase):
        pass

    try:
        ClusterOwner.setup_cluster()
        ClusterOwner.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'init', '--store=column', '--datetime-types=dt64'])
        ClusterOwner.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'import', 'files', '--input', yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")])
        yield ClusterOwner.cluster
    finally:
        if ClusterOwner.cluster is not None:
            YdbCluster.get_ydb_driver().stop()
            ClusterOwner.cluster.stop()


class SharedClickbench(FunctionalTestBase):
    @classmethod
    def setup_class(cls):
        # Verification runs after the shared cluster is ready.
        pass

    @classmethod
    def do_teardown_class(cls):
        # The module fixture owns the cluster lifetime.
        pass

    @pytest.fixture(scope="class", autouse=True)
    def prepared_clickbench(self, request, clickbench_cluster):
        cls = request.cls
        cls.cluster = clickbench_cluster
        try:
            cls.perform_verification()
            yield
        finally:
            cls.cluster = None


class TestClickbench(SharedClickbench, clickbench.TestClickbench):
    iterations: int = 1
    verify_data: bool = False


class TestClickbenchParallel(SharedClickbench, clickbench.TestClickbenchParallel8):
    verify_data: bool = False
    iterations: int = 2


class TestClickbenchPg(SharedClickbench, clickbench.TestClickbenchPg):
    verify_data: bool = False
    iterations: int = 1
