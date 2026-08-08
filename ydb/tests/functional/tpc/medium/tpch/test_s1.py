import ydb.tests.olap.load.lib.tpch as tpch
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpchS1(tpch.TestTpch1, FunctionalTestBase):
    iterations: int = 1

    @classmethod
    def addition_init_params(cls) -> list[str]:
        if cls.float_mode:
            return ['--float-mode', cls.float_mode]
        return []

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls._get_path()}', 'init', '--store=row', '--datetime-types=dt64'] + cls.addition_init_params())
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls._get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()
