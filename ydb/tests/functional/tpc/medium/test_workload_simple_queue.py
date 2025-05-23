import ydb.tests.olap.load.lib.workload_simple_queue as workload_simple_queue
import yatest.common
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestWorkloadSimpleQueue(workload_simple_queue.TestWorkloadSimpleQueue, FunctionalTestBase):
    iterations: int = 1
    verify_data: bool = False

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        super().setup_class()
