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
        cls.run_cli(['workload', 'tpcds', '-p', 'olap_yatests/tpcds/s1', 'init', '--store=column'])
        cls.run_cli(['workload', 'tpcds', '-p', 'olap_yatests/tpcds/s1', 'import', 'generator', '--scale=1'])
        tpcds.TestTpcds1.setup_class()
