import os
import ydb.tests.olap.load.lib.tpch as tpch
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpchSpillingS1(tpch.TestTpch1, FunctionalTestBase):
    iterations: int = 1

    @classmethod
    def setup_class(cls) -> None:
        os.environ['YDB_HARD_MEMORY_LIMIT_BYTES'] = '1073741824'
        cls.setup_cluster(with_spilling=True)
        cls.run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s1', 'init', '--store=column'])
        cls.run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s1', 'import', 'generator', '--scale=1'])
        tpch.TestTpch1.setup_class()
