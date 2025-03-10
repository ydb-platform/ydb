from __future__ import annotations
import pytest
from .conftest import LoadSuiteBase
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, CheckCanonicalPolicy
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TpchSuiteBase(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.TPC_H
    iterations: int = 3
    tables_size: dict[str, int] = {}
    skip_tests: list = []

    @classmethod
    def _get_tables_size(cls) -> dict[str, int]:
        result: dict[str, int] = {
            'customer': 150000 * cls.scale,
            'nation': 25,
            'orders': 1500000 * cls.scale,
            'part': 200000 * cls.scale,
            'partsupp': 800000 * cls.scale,
            'region': 5,
            'supplier': 10000 * cls.scale,
        }
        result.update(cls.tables_size)
        return result

    @classmethod
    def _get_path(cls, full: bool = True) -> str:
        if full:
            tpch_path = get_external_param('table-path-tpch', f'{YdbCluster.tables_path}/tpch')
        else:
            tpch_path = 'tpch'
        return get_external_param(f'table-path-{cls.suite()}', f'{tpch_path}/s{cls.scale}')

    @classmethod
    def do_setup_class(cls):
        if getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_TPCH', '0') == '1' or getenv(f'NO_VERIFY_DATA_TPCH_{cls.scale}'):
            return
        cls.check_tables_size(folder=cls._get_path(False), tables=cls._get_tables_size())

    @pytest.mark.parametrize('query_num', [i for i in range(1, 23)])
    def test_tpch(self, query_num: int):
        if query_num in self.skip_tests:
            return
        self.run_workload_test(self._get_path(), query_num)


class TestTpch1(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 6001215,
    }
    scale: int = 1
    check_canonical: bool = CheckCanonicalPolicy.ERROR


class TestTpch10(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 59986052,
    }
    scale: int = 10
    check_canonical: bool = CheckCanonicalPolicy.ERROR


class TestTpch100(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 600037902,
    }
    scale: int = 100
    check_canonical: bool = CheckCanonicalPolicy.ERROR
    timeout = max(TpchSuiteBase.timeout, 300.)


class TestTpch1000(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 5999989709,
    }
    scale: int = 1000
    check_canonical: bool = CheckCanonicalPolicy.WARNING
    timeout = max(TpchSuiteBase.timeout, 3600.)


class TestTpch10000(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 59999994267,
    }
    query_settings = {
        9: LoadSuiteBase.QuerySettings(timeout=max(TpchSuiteBase.timeout, 7200.)),
        17: LoadSuiteBase.QuerySettings(timeout=max(TpchSuiteBase.timeout, 7200.)),
        18: LoadSuiteBase.QuerySettings(timeout=max(TpchSuiteBase.timeout, 7200.)),
        20: LoadSuiteBase.QuerySettings(timeout=max(TpchSuiteBase.timeout, 7200.)),
        21: LoadSuiteBase.QuerySettings(timeout=max(TpchSuiteBase.timeout, 7200.)),
    }

    scale: int = 10000
    iterations: int = 2
    check_canonical: bool = CheckCanonicalPolicy.WARNING
    timeout = max(TpchSuiteBase.timeout, 3600.)
