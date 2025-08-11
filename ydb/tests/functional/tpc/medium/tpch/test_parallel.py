import ydb.tests.olap.load.lib.tpch as tpch
import test_s_float
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpchParallelS0_1T10(tpch.TpchParallelBase, FunctionalTestBase):
    iterations: int = 1
    tables_size = test_s_float.TestTpchS0_1.tables_size
    scale: float = test_s_float.TestTpchS0_1.scale
    threads: int = 10

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'init', '--store=column', '--datetime-types=dt64'])
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()
