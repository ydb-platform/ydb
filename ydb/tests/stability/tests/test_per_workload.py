import pytest
import allure
from ydb.tests.library.stability.run_stress import StressRunExecutor
from ydb.tests.library.stability.workload_executor_parallel import ParallelWorkloadTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.library.stability.utils.utils import get_external_param
from all_workloads import get_all_stress_names, get_stress_util

import logging
LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    'nemesis_enabled', [True, False],
    ids=['nemesis_true', 'nemesis_false']
)
@pytest.mark.parametrize(
    'stress_name', get_all_stress_names(),
)
@allure.title("{stress_name}[{nemesis_enabled}]")
class TestPerWorkload(ParallelWorkloadTestBase):
    timeout = int(get_external_param('workload_duration', 120))

    def test_stress_util(self, stress_name, nemesis_enabled: bool, stress_executor: StressRunExecutor, binary_deployer):
        allure.dynamic.title(f'{stress_name}[nemesis_{nemesis_enabled}]')
        stress_util = get_stress_util(stress_name, ["--database", f"/{YdbCluster.ydb_database}"])

        stress_dict = {}
        stress_dict[stress_name] = stress_util
        self.execute_parallel_workloads_test(
            stress_executor,
            binary_deployer,
            workload_params=stress_dict,
            duration_value=self.timeout,
            nemesis_enabled=nemesis_enabled,
        )
