import ydb.tests.olap.load.lib.workload_simple_queue as workload_simple_queue
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestWorkloadSimpleQueue(workload_simple_queue.TestSimpleQueue, FunctionalTestBase):
    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        workload_simple_queue.TestSimpleQueue.setup_class()
