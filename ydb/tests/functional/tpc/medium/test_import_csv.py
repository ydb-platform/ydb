from ydb.tests.olap.load.lib.import_csv import ImportFileCsvBase, pytest_generate_tests # noqa
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestExternalImportCsv(ImportFileCsvBase, FunctionalTestBase):
    external_folder: str = 'e1'
