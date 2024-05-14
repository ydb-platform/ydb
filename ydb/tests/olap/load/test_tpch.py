import pytest
from conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import WorkloadType


class TpchSuiteBase(LoadSuiteBase):
    size: int = None
    workload_type: WorkloadType = WorkloadType.TPC_H
    iterations: int = 3

    @pytest.mark.parametrize('query_num', [i for i in range(1, 22)])
    def test_tpch(self, query_num: int):
        self.run_workload_test(f'tpch/s{self.size}/', query_num)


class TestTpch1(TpchSuiteBase):
    size: int = 1


class TestTpch10(TpchSuiteBase):
    size: int = 10


class TestTpch100(TpchSuiteBase):
    size: int = 100
    timeout = 300.


# class TestTpch1000(TpchSuiteBase):
#    size: int = 1000
#    timeout = 1000.
