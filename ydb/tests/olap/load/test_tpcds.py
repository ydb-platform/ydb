import pytest
from conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import WorkloadType
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TpcdsSuiteBase(LoadSuiteBase):
    size: int = None
    workload_type: WorkloadType = WorkloadType.TPC_DS
    iterations: int = 3

    @pytest.mark.parametrize('query_num', [i for i in range(1, 100)])
    def test_tpcds(self, query_num: int):
        root_path = YdbCluster.tables_path
        tpcds_path = get_external_param('table-path-tpcds', f'{root_path}/tpcds')
        path = get_external_param(f'table-path-{self.suite}', f'{tpcds_path}/s{self.size}')
        self.run_workload_test(path, query_num)


class TestTpcds1(TpcdsSuiteBase):
    size: int = 1


class TestTpcds10(TpcdsSuiteBase):
    size: int = 10
    timeout = max(TpcdsSuiteBase.timeout, 300.)


class TestTpcds100(TpcdsSuiteBase):
    size: int = 100
    timeout = max(TpcdsSuiteBase.timeout, 3600.)


class TestTpcds1000(TpcdsSuiteBase):
    size: int = 1000
    timeout = max(TpcdsSuiteBase.timeout, 3*3600.)
