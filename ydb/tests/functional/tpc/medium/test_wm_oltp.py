import ydb.tests.workload_manager.common.workload_manager as wm
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestWMOltp50(wm.TestWorkloadManagerOltp50, FunctionalTestBase):
    timeout: float = 30
    tpcc_warehouses: int = 10
    tpcc_warmup: float = 2

    @classmethod
    def before_stop_checking(cls):
        cls.wait_tpcc()

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster_ext(extra_feature_flags=['enable_composite_conveyor_scheduling'])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'init', '--warehouses', str(cls.tpcc_warehouses)])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'import', '--warehouses', str(cls.tpcc_warehouses), '--no-tui'])
        super().setup_class()
