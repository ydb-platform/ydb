from __future__ import annotations
import pytest
from .conftest import LoadSuiteBase, LoadSuiteParallel
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, CheckCanonicalPolicy
from ydb.tests.olap.lib.utils import get_external_param


class TpchSuiteBase(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.TPC_H
    iterations: int = 3
    tables_size: dict[str, int] = {}
    skip_tests: list = []
    check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.ERROR

    @staticmethod
    def _get_tables_size(test_class) -> dict[str, int]:
        result: dict[str, int] = {
            'customer': 150000 * test_class.scale,
            'nation': 25,
            'orders': 1500000 * test_class.scale,
            'part': 200000 * test_class.scale,
            'partsupp': 800000 * test_class.scale,
            'region': 5,
            'supplier': 10000 * test_class.scale,
        }
        result.update(test_class.tables_size)
        return result

    @classmethod
    def _get_path(cls) -> str:
        return get_external_param(f'table-path-{cls.suite()}', f'tpch/s{cls.scale}'.replace('.', '_'))

    @classmethod
    def do_setup_class(cls):
        if not cls.verify_data or getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_TPCH', '0') == '1' or getenv(f'NO_VERIFY_DATA_TPCH_{cls.scale}'):
            return
        cls.check_tables_size(folder=cls._get_path(), tables=TpchSuiteBase._get_tables_size(cls))

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
    iterations: int = 10
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


class TpchParallelBase(LoadSuiteParallel):
    workload_type: WorkloadType = TpchSuiteBase.workload_type
    iterations: int = 10

    @classmethod
    def get_query_list(cls) -> list[str]:
        return [f'Query{query_num:02d}' for query_num in range(1, 23)]

    @classmethod
    def get_path(cls) -> str:
        return get_external_param(f'table-path-{cls.suite()}', f'tpch/s{cls.scale}'.replace('.', '_'))

    @classmethod
    def do_setup_class(cls):
        if not cls.verify_data or getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_TPCH', '0') == '1' or getenv(f'NO_VERIFY_DATA_TPCH_{cls.scale}'):
            return
        cls.check_tables_size(folder=cls.get_path(), tables=TpchSuiteBase._get_tables_size(cls))
        super().do_setup_class()


class TpchParallelS1T10(TpchParallelBase):
    tables_size = TestTpch1.tables_size
    scale: int = 1
    threads: int = 10


class TestTpchParallelS100T10(TpchParallelBase):
    tables_size = TestTpch100.tables_size
    scale: int = 100
    threads: int = 10
