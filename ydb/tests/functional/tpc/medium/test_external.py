from ydb.tests.olap.load.lib.external import ExternalSuiteBase, pytest_generate_tests # noqa
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestExternalE1(ExternalSuiteBase, FunctionalTestBase):
    iterations: int = 1
    external_folder: str = 'e1'

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli([
            'workload', 'query', '-p', f'olap_yatests/{cls.external_folder}', 'init', '--suite-path', cls.get_external_path()
        ])
        cls.run_cli([
            'workload', 'query', '-p', f'olap_yatests/{cls.external_folder}', 'import', '--suite-path', cls.get_external_path()
        ])
        super().setup_class()

    @classmethod
    def teardown_class(cls) -> None:
        cls.run_cli(['workload', 'query', '-p', f'olap_yatests/{cls.external_folder}', 'clean'])
