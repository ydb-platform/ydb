import ydb.tests.olap.load.lib.tpcc as tpcc
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpccW10T4SdkCompaction(tpcc.TpccSuiteBase, FunctionalTestBase):
    time_s: float = 30
    warmup_s: float = 2
    warehouses: int = 10
    threads: int = 4
    compaction_mode = tpcc.CompactionMode.SDK

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        super().setup_class()


class TestTpccW5T4LegacyCompaction(TestTpccW10T4SdkCompaction):
    warehouses: int = 5
    warmup_s: float = 1
    compaction_mode = tpcc.CompactionMode.LEGACY
