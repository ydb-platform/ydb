import ydb.tests.olap.load.lib.tpch as tpch
from conftest import run_cli


class TestTpchS1(tpch.TestTpch1):
    iterations: int = 1
 
    @classmethod
    def setup_class(cls) -> None:
        run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s1', 'init', '--store=column'])
        run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s1', 'import', 'generator', '--scale=1'])
        tpch.TestTpch1.setup_class()
