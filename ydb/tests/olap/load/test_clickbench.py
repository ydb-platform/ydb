import allure
import pytest
from time import time
from conftest import LoadSuiteBase
from os import getenv
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.ydb_cli import WorkloadType, YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param


class TestClickbench(LoadSuiteBase):
    suite = 'Clickbench'
    workload_type: WorkloadType = WorkloadType.Clickbench
    refference: str = 'CH.60'

    def setup_class(self):
        if getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_CLICKBECNH', '0') == '1':
            return
        root_path = YdbCluster.tables_path
        path = get_external_param('table-path-clickbench', f'{root_path}/clickbench/hits')
        fail_count = 0
        start_time = time()
        for query_num in range(0, 43):
            try:
                with allure.step(f'request {query_num}'):
                    result = YdbCliHelper.workload_run(
                        path=path,
                        query_num=query_num,
                        iterations=1,
                        workload_type=self.workload_type,
                        timeout=self.timeout,
                        check_canonical=True
                    )
                    self.process_query_result(self, result=result, query_num=query_num, iterations=1, upload=False)
            except BaseException:
                fail_count += 1

        test = '_Verification'
        ResultsProcessor.upload_results(
            kind='Load',
            suite=self.suite,
            test=test,
            timestamp=start_time,
            is_successful=(fail_count == 0)
        )
        if fail_count > 0:
            pytest.fail(f'{fail_count} verification queries failed')

    @pytest.mark.parametrize('query_num', [i for i in range(0, 43)])
    def test_clickbench(self, query_num):
        root_path = YdbCluster.tables_path
        path = get_external_param(f'table-path-{self.suite}', f'{root_path}/clickbench/hits')
        self.run_workload_test(path, query_num)
