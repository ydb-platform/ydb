from ydb.tests.olap.load.lib.import_csv import ImportFileCsvBase # noqa
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class LocalClusterFunctionalTestBase(FunctionalTestBase):
    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        super().setup_class()


class TestExternalImportCsv(ImportFileCsvBase, LocalClusterFunctionalTestBase):
    external_folder: str = 'e1'


class TestExternalImportCsvArrow(ImportFileCsvBase, LocalClusterFunctionalTestBase):
    external_folder: str = 'e1'
    send_format: str = 'arrow'
