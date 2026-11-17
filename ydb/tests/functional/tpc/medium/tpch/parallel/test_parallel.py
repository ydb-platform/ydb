import ydb.tests.olap.load.lib.tpch as tpch
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpchParallelS0_1T10(tpch.TpchParallelBase, FunctionalTestBase):
    iterations: int = 1
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1
    threads: int = 10

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'init', '--store=column', '--datetime-types=dt64'])
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls.get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()
