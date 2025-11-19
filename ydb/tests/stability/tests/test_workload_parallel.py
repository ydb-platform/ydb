import pytest
from ydb.tests.library.stability.run_stress import StressRunExecutor
from ydb.tests.library.stability.workload_executor_parallel import ParallelWorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from all_workloads import all_workloads

import logging
LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    'nemesis_enabled', [True, False],
    ids=['nemesis_true', 'nemesis_false']
)
class TestWorkloadParallel(ParallelWorkloadTestBase):
    timeout = int(get_external_param('workload_duration', 120))

    def test_all_workloads_parallel(self, nemesis_enabled: bool, stress_executor: StressRunExecutor, binary_deployer, olap_load_base):

        runnable_workloads = all_workloads

        for wl, arg in runnable_workloads.items():
            arg['args'] += ["--database", f"/{YdbCluster.ydb_database}"]

        self.execute_parallel_workloads_test(
            stress_executor,
            binary_deployer,
            olap_load_base,
            workload_params=runnable_workloads,
            duration_value=self.timeout,
            nemesis_enabled=nemesis_enabled,
            nodes_percentage=100
        )
