from ydb.tests.olap.load.lib.external import ExternalSuiteBase, pytest_generate_tests # noqa
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
import os


class TestExternalE1(ExternalSuiteBase, FunctionalTestBase):
    iterations: int = 1
    external_folder: str = 'e1'

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli([
            'workload', 'query', '-d', cls.get_external_path(), '-p', f'olap_yatests/{cls.external_folder}', 'init'
        ])
        cls.run_cli([
            'workload', 'query', '-d', cls.get_external_path(), '-p', f'olap_yatests/{cls.external_folder}',
            'import', 'dir', '--input', os.path.join(cls.get_external_path(), 'data')
        ])
        super().setup_class()

    @classmethod
    def teardown_class(cls) -> None:
        cls.run_cli(['workload', 'query', '-d', cls.get_external_path(), '-p', f'olap_yatests/{cls.external_folder}', 'clean'])
