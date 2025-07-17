import ydb.tests.olap.load.lib.tpcds as tpcds
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpcdsS1(tpcds.TestTpcds1, FunctionalTestBase):
    iterations: int = 1

    memory_controller_config = {
        'hard_limit_bytes': 107374182400,
    }

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster(memory_controller_config=cls.memory_controller_config)
        cls.run_cli(['workload', 'tpcds', '-p', f'olap_yatests/{cls._get_path()}', 'init', '--store=column'])
        cls.run_cli(['workload', 'tpcds', '-p', f'olap_yatests/{cls._get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()


class TestTpcdsS0_1(TestTpcdsS1):
    scale = 0.1
    tables_size: dict[str, int] = {
        'customer_demographics': 192080,
        'date_dim': 7304,
        'household_demographics': 720,
        'income_band': 2,
        'ship_mode': 2,
        'time_dim': 8640,
        'call_center': 1,
        'catalog_page': 1171,
        'catalog_returns': 14416,
        'catalog_sales': 143657,
        'customer_address': 5000,
        'customer': 10000,
        'inventory': 1174500,
        'item': 1800,
        'promotion': 30,
        'reason': 3,
        'store': 1,
        'store_returns': 28704,
        'store_sales': 288464,
        'warehouse': 1,
        'web_page': 6,
        'web_returns': 7061,
        'web_sales': 71632,
        'web_site': 3,
    }
