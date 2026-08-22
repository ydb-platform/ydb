import ydb.tests.olap.load.lib.workload_manager as wm
import yatest.common
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
    timeout: float = 60
    tpcc_warehouses: int = 10

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
    tpcc_warehouses: int = 10

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
