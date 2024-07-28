import pytest
from conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import WorkloadType
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TpchSuiteBase(LoadSuiteBase):
    size: int = None
    workload_type: WorkloadType = WorkloadType.TPC_H
    iterations: int = 3

    @pytest.mark.parametrize('query_num', [i for i in range(1, 23)])
    def test_tpch(self, query_num: int):
        root_path = YdbCluster.tables_path
        tpch_path = get_external_param('table-path-tpch', f'{root_path}/tpch')
        path = get_external_param(f'table-path-{self.suite}', f'{tpch_path}/s{self.size}')
        self.run_workload_test(path, query_num)


class TestTpch1(TpchSuiteBase):
    size: int = 1


class TestTpch10(TpchSuiteBase):
    size: int = 10


class TestTpch100(TpchSuiteBase):
    size: int = 100
    timeout = max(TpchSuiteBase.timeout, 300.)


class TestTpch1000(TpchSuiteBase):
    size: int = 1000
    timeout = max(TpchSuiteBase.timeout, 1000.)


class TestTpch10000(TpchSuiteBase):
    size: int = 10000
    timeout = max(TpchSuiteBase.timeout, 3600.)
