from __future__ import annotations
import pytest
from .conftest import LoadSuiteBase
from os import getenv
from ydb.tests.olap.lib.ydb_cli import WorkloadType, CheckCanonicalPolicy
from ydb.tests.olap.lib.utils import get_external_param


class TpcdsSuiteBase(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.TPC_DS
    iterations: int = 3
    tables_size: dict[str, int] = {}
    check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.ERROR

    @classmethod
    def _get_tables_size(cls) -> dict[str, int]:
        result: dict[str, int] = {
            'customer_demographics': 1920800,
            'date_dim': 73049,
            'household_demographics': 7200,
            'income_band': 20,
            'ship_mode': 20,
            'time_dim': 86400,
        }
        result.update(cls.tables_size)
        return result

    @classmethod
    def _get_path(cls) -> str:
        return get_external_param(f'table-path-{cls.suite()}', f'tpcds/s{cls.scale}')

    @classmethod
    def do_setup_class(cls):
        if not cls.verify_data or getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_TPCH', '0') == '1' or getenv(f'NO_VERIFY_DATA_TPCH_{cls.scale}'):
            return
        cls.check_tables_size(folder=cls._get_path(), tables=cls._get_tables_size())

    @pytest.mark.parametrize('query_num', [i for i in range(1, 100)])
    def test_tpcds(self, query_num: int):
        self.run_workload_test(self._get_path(), query_num)


class TestTpcds1(TpcdsSuiteBase):
    scale: int = 1
    tables_size: dict[str, int] = {
        'call_center': 6,
        'catalog_page': 11718,
        'catalog_returns': 144067,
        'catalog_sales': 1441548,
        'customer_address': 50000,
        'customer': 100000,
        'inventory': 11745000,
        'item': 18000,
        'promotion': 300,
        'reason': 35,
        'store': 12,
        'store_returns': 287514,
        'store_sales': 2880404,
        'warehouse': 5,
        'web_page': 60,
        'web_returns': 71763,
        'web_sales': 719384,
        'web_site': 30,
    }


class TestTpcds10(TpcdsSuiteBase):
    scale: int = 10
    timeout = max(TpcdsSuiteBase.timeout, 300.)
    query_settings = {
        # temporary, https://github.com/ydb-platform/ydb/issues/11767#issuecomment-2553353146
        72: LoadSuiteBase.QuerySettings(query_prefix='pragma ydb.UseGraceJoinCoreForMap = "false";'),
    }
    tables_size: dict[str, int] = {
        'call_center': 24,
        'catalog_page': 12000,
        'catalog_returns': 1439749,
        'catalog_sales': 14401261,
        'customer': 500000,
        'customer_address': 250000,
        'inventory': 133110000,
        'item': 102000,
        'promotion': 500,
        'reason': 45,
        'store': 102,
        'store_returns': 2875432,
        'store_sales': 28800991,
        'warehouse': 10,
        'web_page': 200,
        'web_returns': 719217,
        'web_sales': 7197566,
        'web_site': 42,
    }


class TestTpcds100(TpcdsSuiteBase):
    scale: int = 100
    iterations: int = 2
    timeout = max(TpcdsSuiteBase.timeout, 3600.)
    query_settings = {
        14: LoadSuiteBase.QuerySettings(timeout=max(TpcdsSuiteBase.timeout, 7200.)),
        72: LoadSuiteBase.QuerySettings(timeout=max(TpcdsSuiteBase.timeout, 7200.)),
    }
    tables_size: dict[str, int] = {
        'call_center': 30,
        'catalog_page': 20400,
        'catalog_returns': 14404374,
        'catalog_sales': 143997065,
        'customer': 2000000,
        'customer_address': 1000000,
        'inventory': 399330000,
        'item': 204000,
        'promotion': 1000,
        'reason': 55,
        'store': 402,
        'store_returns': 28795080,
        'store_sales': 287997024,
        'warehouse': 15,
        'web_page': 2040,
        'web_returns': 7197670,
        'web_sales': 72001237,
        'web_site': 24,
    }


class TestTpcds1000(TpcdsSuiteBase):
    scale: int = 1000
    iterations: int = 1
    timeout = max(TpcdsSuiteBase.timeout, 3600.)
    tables_size: dict[str, int] = {
        'call_center': 42,
        'catalog_page': 30000,
        'catalog_returns': 143996756,
        'catalog_sales': 1439980416,
        'customer': 12000000,
        'customer_address': 6000000,
        'inventory': 783000000,
        'item': 300000,
        'promotion': 1500,
        'reason': 65,
        'store': 1002,
        'store_returns': 287999764,
        'store_sales': 2879987999,
        'warehouse': 20,
        'web_page': 3000,
        'web_returns': 71997522,
        'web_sales': 720000376,
        'web_site': 54,
    }
