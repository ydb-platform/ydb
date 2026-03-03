import ydb.tests.olap.load.lib.workload_oltp as workload_oltp
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestWorkloadSimpleQueue(workload_oltp.TestOltpWorkload, FunctionalTestBase):
    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        workload_oltp.TestOltpWorkload.setup_class()

    @classmethod
    def do_teardown_class(cls) -> None:
        if cls.cluster is not None:
            cls.cluster.stop()
