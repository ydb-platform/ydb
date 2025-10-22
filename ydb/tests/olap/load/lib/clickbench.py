import allure
import pytest
from .conftest import LoadSuiteBase, LoadSuiteParallel
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, YdbCliHelper, CheckCanonicalPolicy
from ydb.tests.olap.lib.utils import get_external_param


QUERY_NAMES = [f'Query{query_num:02d}' for query_num in range(0, 43)]


class TestClickbench(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.Clickbench
    path = get_external_param('table-path-clickbench', 'clickbench/hits')

    @classmethod
    def do_setup_class(cls):
        if not cls.verify_data or getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_CLICKBENCH', '0') == '1':
            return

        cls.check_tables_size(folder=None, tables={'clickbench/hits': 99997497})

        fail_count = 0
        results = YdbCliHelper.workload_run(
            path=cls.path,
            query_names=QUERY_NAMES,
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

    def _test_impl(self, query_name):
        self.run_workload_test(self.path, query_name=query_name)

    @pytest.mark.parametrize('query_name', QUERY_NAMES)
    def test_clickbench(self, query_name):
        self._test_impl(query_name)


class TestClickbenchPg(TestClickbench):
    query_syntax = 'pg'

    def _test_impl(self, query_name):
        if query_name in {'Query18', 'Query28', 'Query39'}:
            pytest.xfail('https://github.com/ydb-platform/ydb/issues/8630')
        self.run_workload_test(self.path, query_name=query_name)


class ClickbenchParallelBase(LoadSuiteParallel):
    workload_type: WorkloadType = WorkloadType.Clickbench
    iterations: int = 5

    @classmethod
    def get_query_list(cls) -> list[str]:
        return QUERY_NAMES

    @classmethod
    def get_path(cls) -> str:
        return get_external_param('table-path-clickbench', 'clickbench/hits')

    @classmethod
    def do_setup_class(cls):
        if cls.verify_data and getenv('NO_VERIFY_DATA', '0') != '1' and getenv('NO_VERIFY_DATA_CLICKBENCH', '0') != '1':
            cls.check_tables_size(folder=None, tables={'clickbench/hits': 99997497})
        super().do_setup_class()


class TestClickbenchParallel1(ClickbenchParallelBase):
    threads: int = 1


class TestClickbenchParallel2(ClickbenchParallelBase):
    threads: int = 2


class TestClickbenchParallel4(ClickbenchParallelBase):
    threads: int = 4


class TestClickbenchParallel8(ClickbenchParallelBase):
    threads: int = 8


class TestClickbenchParallel16(ClickbenchParallelBase):
    threads: int = 16
