import ydb.tests.olap.load.lib.upload as upload
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestUploadTpchS1(upload.TestUploadTpch1, FunctionalTestBase):
    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        upload.TestUploadTpch1.setup_class()
