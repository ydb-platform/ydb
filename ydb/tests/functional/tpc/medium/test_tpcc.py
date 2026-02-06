import ydb.tests.olap.load.lib.tpcc as tpcc
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpccW10T4(tpcc.TpccSuiteBase, FunctionalTestBase):
    time_s: float = 60
    warehouses: int = 10
    threads: int = 4

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'init', '--warehouses', str(cls.warehouses)])
        cls.run_cli(['workload', 'tpcc', '-p', f'olap_yatests/{cls.get_tpcc_path()}', 'import', '--warehouses', str(cls.warehouses), '--no-tui'])
        super().setup_class()
