import ydb.tests.workload_manager.common.workload_manager as wm
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


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
            wm.ResourcePool('test_pool_20', ['testuser20'], total_cpu_limit_percent_per_node=50),
        ]

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster_ext(extra_feature_flags=['enable_composite_conveyor_scheduling'])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'init', '--warehouses', str(cls.tpcc_warehouses)])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'import', '--warehouses', str(cls.tpcc_warehouses), '--no-tui'])
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'init', '--store=column', '--datetime-types=dt64'])
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()
