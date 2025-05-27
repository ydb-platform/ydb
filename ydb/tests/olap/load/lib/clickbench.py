import allure
import pytest
from .conftest import LoadSuiteBase, LoadSuiteParallel
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, YdbCliHelper, CheckCanonicalPolicy
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param


QUERY_NAMES = [f'Query{query_num:02d}' for query_num in range(0, 43)]


class TestClickbench(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.Clickbench
    path = get_external_param('table-path-clickbench', f'{YdbCluster.tables_path}/clickbench/hits')

    @classmethod
    def do_setup_class(cls):
        if not cls.verify_data or getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_CLICKBENCH', '0') == '1':
            return

        cls.check_tables_size(folder=None, tables={'clickbench/hits': 99997497})

        fail_count = 0
        results = YdbCliHelper.workload_run(
            path=cls.path,
            query_names=set(QUERY_NAMES),
            iterations=1,
            workload_type=cls.workload_type,
            timeout=cls._get_query_settings().timeout,
            check_canonical=CheckCanonicalPolicy.ERROR,
            threads=8
        )
        for query_name, result in results.items():
            try:
                with allure.step(f'request {query_name}'):
                    cls.process_query_result(result=result, query_name=query_name, upload=False)
            except BaseException:
                fail_count += 1

        if fail_count > 0:
            pytest.fail(f'{fail_count} verification queries failed')

    @pytest.mark.parametrize('query_name', QUERY_NAMES)
    def test_clickbench(self, query_name):
        self.run_workload_test(self.path, query_name=query_name)


class TestClickbenchPg(TestClickbench):
    query_syntax = 'pg'


class TestClickbenchParallel(LoadSuiteParallel):
    workload_type: WorkloadType = WorkloadType.Clickbench
    iterations: int = 10

    def get_query_list() -> list[str]:
        return QUERY_NAMES

    def get_path() -> str:
        return get_external_param('table-path-clickbench', f'{YdbCluster.tables_path}/clickbench/hits')

    @classmethod
    def do_setup_class(cls):
        if cls.verify_data and getenv('NO_VERIFY_DATA', '0') != '1' and getenv('NO_VERIFY_DATA_CLICKBENCH', '0') != '1':
            cls.check_tables_size(folder=None, tables={'clickbench/hits': 99997497})
        super().do_setup_class()
