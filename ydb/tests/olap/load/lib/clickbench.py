import allure
import pytest
from .conftest import LoadSuiteBase
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, YdbCliHelper, CheckCanonicalPolicy
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param


class TestClickbench(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.Clickbench
    refference: str = 'CH.60'
    path = get_external_param('table-path-clickbench', f'{YdbCluster.tables_path}/clickbench/hits')

    @classmethod
    def do_setup_class(cls):
        if getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_CLICKBENCH', '0') == '1':
            return

        cls.check_tables_size(folder=None, tables={'clickbench/hits': 99997497})

        fail_count = 0
        for query_num in range(0, 43):
            try:
                with allure.step(f'request {query_num}'):
                    result = YdbCliHelper.workload_run(
                        path=cls.path,
                        query_num=query_num,
                        iterations=1,
                        workload_type=cls.workload_type,
                        timeout=cls._get_query_settings(query_num).timeout,
                        check_canonical=CheckCanonicalPolicy.ERROR
                    )
                    cls.process_query_result(result=result, query_num=query_num, iterations=1, upload=False)
            except BaseException:
                fail_count += 1

        if fail_count > 0:
            pytest.fail(f'{fail_count} verification queries failed')

    @pytest.mark.parametrize('query_num', [i for i in range(0, 43)])
    def test_clickbench(self, query_num):
        self.run_workload_test(self.path, query_num)


class TestClickbenchPg(TestClickbench):
    query_syntax = 'pg'
