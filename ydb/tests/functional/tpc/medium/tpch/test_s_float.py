import pytest

import ydb.tests.olap.load.lib.tpch as tpch
from s1 import test_s1
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TpchS0_1(test_s1.TestTpchS1):
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1


@pytest.fixture(scope="module")
def tpch_cluster():
    class ClusterOwner(FunctionalTestBase):
        pass

    try:
        ClusterOwner.setup_cluster()
        paths = {TestTpchS0_1._get_path(), TestTpchParallelS0_1T10.get_path()}
        for path in sorted(paths):
            ClusterOwner.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{path}', 'init', '--store=column', '--datetime-types=dt64'])
            ClusterOwner.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{path}', 'import', 'generator', '--scale=0.1'])
        yield ClusterOwner.cluster
    finally:
        if ClusterOwner.cluster is not None:
            YdbCluster.get_ydb_driver().stop()
            ClusterOwner.cluster.stop()


class SharedTpch(FunctionalTestBase):
    @classmethod
    def setup_class(cls):
        # Verification runs after the shared cluster is ready.
        pass

    @classmethod
    def do_teardown_class(cls):
        # The module fixture owns the cluster lifetime.
        pass

    @pytest.fixture(scope="class", autouse=True)
    def prepared_tpch(self, request, tpch_cluster):
        cls = request.cls
        cls.cluster = tpch_cluster
        try:
            cls.perform_verification()
            yield
        finally:
            cls.cluster = None


class TestTpchS0_1(SharedTpch, TpchS0_1):
    pass


class TestTpchParallelS0_1T10(SharedTpch, tpch.TpchParallelBase):
    iterations: int = 1
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1
    threads: int = 10
