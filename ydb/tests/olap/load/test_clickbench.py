import pytest
from conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import WorkloadType


class TestClickbench(LoadSuiteBase):
    suite = 'Clickbench'
    workload_type: WorkloadType = WorkloadType.Clickbench
    refference: str = 'CH.60'

    @pytest.mark.parametrize('query_num', [i for i in range(0, 43)])
    def test_clickbench(self, query_num):
        self.run_workload_test('clickbench/hits', query_num)
