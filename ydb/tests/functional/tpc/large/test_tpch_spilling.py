import os
import ydb.tests.olap.load.lib.tpch as tpch
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpchSpillingS10(tpch.TestTpch10, FunctionalTestBase):
    iterations: int = 1

    @classmethod
    def setup_class(cls) -> None:
        os.environ['YDB_HARD_MEMORY_LIMIT_BYTES'] = '16106127360'
        cls.setup_cluster(with_spilling=True)
        cls.run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s10', 'init', '--store=column'])
        cls.run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s10', 'import', 'generator', '--scale=10'])
        tpch.TestTpch10.setup_class()
