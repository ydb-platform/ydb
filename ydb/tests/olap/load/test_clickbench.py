import pytest
from conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import WorkloadType
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TestClickbench(LoadSuiteBase):
    suite = 'Clickbench'
    workload_type: WorkloadType = WorkloadType.Clickbench
    refference: str = 'CH.60'

    @pytest.mark.parametrize('query_num', [i for i in range(0, 43)])
    def test_clickbench(self, query_num):
        root_path = YdbCluster.tables_path
        path = get_external_param(f'table-path-{self.suite}', f'{root_path}/clickbench/hits')
        self.run_workload_test(path, query_num)
