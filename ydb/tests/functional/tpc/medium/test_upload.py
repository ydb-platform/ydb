import ydb.tests.olap.load.lib.upload as upload
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TestUploadTpchS1(upload.TestUploadTpch1, FunctionalTestBase):
    @classmethod
    def setup_class(cls) -> None:
        YdbCluster._tables_path = ''
        cls.setup_cluster()
        upload.TestUploadTpch1.setup_class()
