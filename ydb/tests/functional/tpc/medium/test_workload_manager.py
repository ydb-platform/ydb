import ydb.tests.olap.load.lib.workload_manager as wm
import yatest.common
import pytest
import time
from pathlib import Path
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestClickbenchWM(wm.TestWorkloadManagerClickbenchConcurrentQueryLimit, FunctionalTestBase):
    iterations: int = 2
    verify_data: bool = False

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'init', '--store=column', '--datetime-types=dt64'])
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'import', 'files', '--input', yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")])
        super().setup_class()


class TestTpchWMS0_1(wm.WorkloadManagerTpchBase, wm.WorkloadManagerConcurrentQueryLimit, FunctionalTestBase):
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1
    iterations: int = 1

    @classmethod
    def addition_init_params(cls) -> list[str]:
        if cls.float_mode:
            return ['--float-mode', cls.float_mode]
        return []

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'init', '--store=column', '--datetime-types=dt64'] + cls.addition_init_params())
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()


class TestClickbenchWMComputeSchedulerP1T1(wm.TestWorkloadManagerClickbenchComputeSchedulerP1T1, FunctionalTestBase):
    iterations: int = 2
    verify_data: bool = False

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'init', '--store=column', '--datetime-types=dt64'])
        cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'import', 'files', '--input', yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")])
        super().setup_class()


# class TestClickbenchWMScheduler(wm.TestWorkloadManagerClickbenchComputeScheduler, FunctionalTestBase):
#     iterations: int = 1
#     verify_data: bool = False
#     timeout = 100

#     @classmethod
#     def setup_class(cls) -> None:
#         cls.setup_cluster()
#         cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'init', '--store=column', '--datetime-types=dt64'])
#         cls.run_cli(['workload', 'clickbench', '-p', 'olap_yatests/clickbench/hits', 'import', 'files', '--input',
#                      yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")])
#         super().setup_class()

class TestWMOltp50(wm.TestWorkloadManagerOltp50, FunctionalTestBase):
    timeout: float = 30
    tpcc_warehouses: int = 10
    tpcc_warmup: float = 2

    @classmethod
    def before_stop_checking(cls):
        cls.wait_tpcc()

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'init', '--warehouses', str(cls.tpcc_warehouses)])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'import', '--warehouses', str(cls.tpcc_warehouses), '--no-tui'])
        super().setup_class()


class TestWMOltpTpchS0_1(wm.WorkloadManagerOltpTpch20Base, FunctionalTestBase):
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1
    iterations: int = 1
    tpcc_warehouses: int = 5

    @classmethod
    def get_resource_pools(cls) -> list[wm.ResourcePool]:
        return [
            wm.ResourcePool('test_pool_20', ['testuser20'], total_cpu_limit_percent_per_node=50, resource_weight=4),
        ]

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'init', '--warehouses', str(cls.tpcc_warehouses)])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'import', '--warehouses', str(cls.tpcc_warehouses), '--no-tui'])
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'init', '--store=column', '--datetime-types=dt64'])
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()


class TestWMOltpAdHoc(wm.TestWorkloadManagerOltpAdHoc, FunctionalTestBase):
    tpcc_warehouses: int = 10
    iterations = 3

    @classmethod
    def before_workload(cls, result):
        super().before_workload(result)
        deadline = time.monotonic() + 20
        execution = cls._tpcc_executions[0][1]
        log = Path(execution._err_path)
        while time.monotonic() < deadline:
            try:
                if 'WM_TPCC_OK' in log.read_text():
                    return
            except FileNotFoundError:
                pass
            time.sleep(0.05)
        cls.terminate_tpcc()
        cls.wait_tpcc()
        pytest.fail('TPC-C did not complete a transaction before the readiness timeout')

    @classmethod
    def get_resource_pools(cls) -> list[wm.ResourcePool]:
        return [
            wm.ResourcePool('test_pool_10', ['testuser10'], total_cpu_limit_percent_per_node=50, resource_weight=4),
        ]

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'init', '--warehouses', str(cls.tpcc_warehouses)])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'import', '--warehouses', str(cls.tpcc_warehouses), '--no-tui'])
        super().setup_class()
