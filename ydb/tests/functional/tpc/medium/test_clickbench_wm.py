import logging
from threading import Event

import pytest
import yatest.common
import ydb

import ydb.tests.workload_manager.common.workload_manager as wm
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


@pytest.fixture(scope="module")
def clickbench_cluster():
    class ClusterOwner(FunctionalTestBase):
        pass

    try:
        ClusterOwner.setup_cluster_ext(extra_feature_flags=['enable_composite_conveyor_scheduling'])
        ClusterOwner.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'init', '--store=column', '--datetime-types=dt64'])
        ClusterOwner.run_cli([
            'workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits',
            'import', 'files', '--input',
            yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv"),
        ])
        logging.info("ClickBench WM shared cluster and import are ready")
        yield ClusterOwner.cluster
    finally:
        if ClusterOwner.cluster is not None:
            YdbCluster.get_ydb_driver().stop()
            ClusterOwner.cluster.stop()


class SharedClickbenchWM(FunctionalTestBase):
    @classmethod
    def setup_class(cls):
        # Verification requires the shared-cluster fixture to have run first.
        pass

    @classmethod
    def do_teardown_class(cls):
        # The module fixture owns the cluster lifetime.
        pass

    @pytest.fixture(scope="class", autouse=True)
    def workload_manager_state(self, request, clickbench_cluster):
        cls = request.cls
        cls.cluster = clickbench_cluster
        cls.stop_checking = Event()
        cls.signal_errors = []
        cls.metrics = []
        cls.metrics_keys = set()
        cls.max_in_fly = 0.0
        cls.hard_query_limit = 0
        try:
            cls.perform_verification()
            yield
        finally:
            cls.stop_checking.set()
            with ydb.QuerySessionPool(YdbCluster.get_ydb_driver()) as sessions:
                for pool in cls.get_resource_pools():
                    sessions.execute_with_retries(f'DROP RESOURCE POOL CLASSIFIER {pool.name}')
                    sessions.execute_with_retries(f'DROP RESOURCE POOL {pool.name}')
                    if len(pool.users) > 1:
                        sessions.execute_with_retries(f'DROP GROUP IF EXISTS {pool.name}_users')
                    for user in pool.users:
                        sessions.execute_with_retries(f'DROP USER IF EXISTS {user}')
            cls.cluster = None


class TestClickbenchWM(SharedClickbenchWM, wm.TestWorkloadManagerClickbenchConcurrentQueryLimit):
    iterations: int = 2
    verify_data: bool = False


class TestClickbenchWMComputeSchedulerP1T1(SharedClickbenchWM, wm.TestWorkloadManagerClickbenchComputeSchedulerP1T1):
    iterations: int = 2
    verify_data: bool = False
