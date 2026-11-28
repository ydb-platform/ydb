import time

import pytest

import ydb.tests.workload_manager.common.workload_manager as wm
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestWMOltpAdHoc(wm.TestWorkloadManagerOltpAdHoc, FunctionalTestBase):
    tpcc_warehouses: int = 10
    iterations = 3

    @classmethod
    def before_workload(cls, result):
        super().before_workload(result)
        deadline = time.monotonic() + 20
        execution = cls._tpcc_executions[0][1]
        while time.monotonic() < deadline:
            if 'WM_TPCC_OK' in execution.read_stderr():
                return
            time.sleep(0.05)
        cls.terminate_tpcc()
        cls.wait_tpcc()
        pytest.fail('TPC-C did not complete a transaction before the readiness timeout')

    @classmethod
    def get_resource_pools(cls) -> list[wm.ResourcePool]:
        return [
            wm.ResourcePool('test_pool_10', ['testuser10'], total_cpu_limit_percent_per_node=50),
        ]

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster_ext(extra_feature_flags=['enable_composite_conveyor_scheduling'])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'init', '--warehouses', str(cls.tpcc_warehouses)])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'import', '--warehouses', str(cls.tpcc_warehouses), '--no-tui'])
        super().setup_class()
