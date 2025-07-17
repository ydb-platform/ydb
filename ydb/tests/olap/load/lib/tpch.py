from __future__ import annotations
import pytest
from .conftest import LoadSuiteBase
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, CheckCanonicalPolicy
from ydb.tests.olap.lib.utils import get_external_param


class TpchSuiteBase(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.TPC_H
    iterations: int = 3
    tables_size: dict[str, int] = {}
    skip_tests: list = []
    check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.ERROR

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
    def _get_path(cls) -> str:
        return get_external_param(f'table-path-{cls.suite()}', f'tpch/s{cls.scale}')

    @classmethod
    def do_setup_class(cls):
        if not cls.verify_data or getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_TPCH', '0') == '1' or getenv(f'NO_VERIFY_DATA_TPCH_{cls.scale}'):
            return
        cls.check_tables_size(folder=cls._get_path(), tables=cls._get_tables_size())

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


class TestTpch10(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 59986052,
    }
    scale: int = 10


class TestTpch100(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 600037902,
    }
    scale: int = 100
    timeout = max(TpchSuiteBase.timeout, 300.)


class TestTpch1000(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 5999989709,
    }
    scale: int = 1000
    timeout = max(TpchSuiteBase.timeout, 3600.)


class TestTpch10000(TpchSuiteBase):
    tables_size: dict[str, int] = {
        'lineitem': 59999994267,
    }

    scale: int = 10000
    iterations: int = 1
    timeout = max(TpchSuiteBase.timeout, 14400.)
