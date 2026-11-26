from ydb.tests.olap.lib.fill_tables import Dataset
from ydb.tests.olap.load.lib.external import ExternalSuiteBase, pytest_generate_tests # noqa
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TestExternalE1(ExternalSuiteBase, FunctionalTestBase):
    iterations: int = 1
    external_folder: str = 'e1'

    @classmethod
    def setup_class(cls) -> None:
        YdbCluster._tables_path = ''
        cls.setup_cluster()
        cls.fill_tables(Dataset('query', cls.external_folder, suite_path=cls.get_external_path()))
        super().setup_class()

    @classmethod
    def teardown_class(cls) -> None:
        try:
            cls.run_cli(['workload', 'query', '-p', f'{cls.external_folder}', 'clean'])
        finally:
            super().teardown_class()
