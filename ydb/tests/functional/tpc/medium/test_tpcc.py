import ydb.tests.olap.load.lib.tpcc as tpcc
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpccW10T4(tpcc.TpccSuiteBase, FunctionalTestBase):
    time_s: float = 60
    warehouses: int = 10
    threads: int = 4

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        super().setup_class()
