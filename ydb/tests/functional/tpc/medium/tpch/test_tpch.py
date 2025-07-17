import ydb.tests.olap.load.lib.tpch as tpch
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TpchSuiteBase(tpch.TpchSuiteBase, FunctionalTestBase):
    iterations: int = 1

    @classmethod
    def addition_init_params(cls) -> list[str]:
        if cls.float_mode:
            return ['--float-mode', cls.float_mode]
        return []

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/tpch/s{cls.scale}', 'init', '--store=column'] + cls.addition_init_params())
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/tpch/s{cls.scale}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()


class TestTpchS1(TpchSuiteBase):
    scale = tpch.TestTpch1.scale
    tables_size = tpch.TestTpch1.tables_size

class TestTpchS1Decimal_22_9(TestTpchS1):
    float_mode = 'decimal_ydb'


class TestTpchS0_1(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1
