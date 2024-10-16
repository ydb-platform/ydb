from __future__ import annotations
import pytest
from conftest import LoadSuiteBase
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TpchSuiteBase(LoadSuiteBase):
    size: int = 0
    workload_type: WorkloadType = WorkloadType.TPC_H
    iterations: int = 3
    tables_size: dict[str, int] = {}

    def _get_tables_size(self) -> dict[str, int]:
        result: dict[str, int] = {
            'customer': 150000 * self.size,
            'nation': 25,
            'orders': 1500000 * self.size,
            'part': 200000 * self.size,
            'partsupp': 800000 * self.size,
            'region': 5,
            'supplier': 10000 * self.size,
        }
        result.update(self.tables_size)
        return result

    def _get_path(self, full: bool = True) -> str:
        if full:
            tpch_path = get_external_param('table-path-tpch', f'{YdbCluster.tables_path}/tpch')
        else:
            tpch_path = 'tpch'
        return get_external_param(f'table-path-{self.suite}', f'{tpch_path}/s{self.size}')

    def do_setup_class(self):
        if getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_TPCH', '0') == '1' or getenv(f'NO_VERIFY_DATA_TPCH_{self.size}'):
            return
        self.check_tables_size(self, folder=self._get_path(self, False), tables=self._get_tables_size(self))

    @pytest.mark.parametrize('query_num', [i for i in range(1, 23)])
    def test_tpch(self, query_num: int):
        self.run_workload_test(self._get_path(), query_num)


class TestTpch1(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 6001215,
    }
    size: int = 1


class TestTpch10(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 59986052,
    }
    size: int = 10


class TestTpch100(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 600037902,
    }
    size: int = 100
    timeout = max(TpchSuiteBase.timeout, 300.)


class TestTpch1000(TpchSuiteBase):
    size: int = 1000
    timeout = max(TpchSuiteBase.timeout, 1000.)


class TestTpch10000(TpchSuiteBase):
    size: int = 10000
    timeout = max(TpchSuiteBase.timeout, 3600.)
