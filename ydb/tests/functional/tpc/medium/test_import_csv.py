from ydb.tests.olap.load.lib.import_csv import ImportFileCsvBase # noqa
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestExternalImportCsv(ImportFileCsvBase, FunctionalTestBase):
    external_folder: str = 'e1'

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        super().setup_class()
