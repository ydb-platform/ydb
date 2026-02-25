import pytest
from ydb.tests.library.stability.run_stress import StressRunExecutor
from ydb.tests.library.stability.workload_executor_parallel import ParallelWorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.library.stability.utils.utils import get_external_param
from all_workloads import get_all_stress_utils

import logging

LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    'nemesis_enabled', [True, False],
    ids=['nemesis_true', 'nemesis_false']
)
class TestWorkloadParallel(ParallelWorkloadTestBase):
    timeout = int(get_external_param('workload_duration', 120))

    def test_all_workloads_parallel(self, nemesis_enabled: bool, stress_executor: StressRunExecutor, binary_deployer):

        runnable_workloads = get_all_stress_utils(["--database", f"/{YdbCluster.ydb_database}"])

        self.execute_parallel_workloads_test(
            stress_executor,
            binary_deployer,
            workload_params=runnable_workloads,
            duration_value=self.timeout,
            nemesis_enabled=nemesis_enabled,
        )
