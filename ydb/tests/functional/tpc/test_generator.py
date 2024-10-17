# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import logging
import csv
import hashlib
import pytest
import json
import random

from ydb.tests.library.common import yatest_common
from ydb.tests.oss.canonical import set_canondata_root
from threading import Thread

logger = logging.getLogger(__name__)


def ydb_bin():
    if os.getenv('YDB_CLI_BINARY'):
        return yatest_common.binary_path(os.getenv('YDB_CLI_BINARY'))
    raise RuntimeError('YDB_CLI_BINARY enviroment variable is not specified')


class TpcGeneratorBase(object):
    tables: dict[str, tuple[list[str], dict[int, int]]]
    workload: str

    @classmethod
    def execute_generator(cls, output_path, scale=1, import_args=[], generator_args=[]):
        return yatest_common.execute(
            [
                ydb_bin(),
                '--endpoint', 'grpc://localhost',
                '--database', '/Root/db',
                'workload', cls.workload, '-p', f'/Root/db/{cls.workload}/s{scale}',
                'import', '-f', output_path,
            ]
            + [str(arg) for arg in import_args]
            + ['generator', '--scale', str(scale)]
            + [str(arg) for arg in generator_args],
            wait=False,
        )

    @staticmethod
    def canonical_result(output_result, tmp_path):
        with open(tmp_path, 'w') as f:
            f.write(output_result)
        return yatest_common.canonical_file(tmp_path, local=True, universal_lines=True)

    @staticmethod
    def calc_hashes(files: str | list[str], keys: list[str]):
        if not isinstance(files, list):
            files = [files]
        rows: dict[str, str] = {}
        for file_path in files:
            if not os.path.exists(file_path):
                continue
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f, delimiter='|', quotechar=None, quoting=csv.QUOTE_NONE)
                fields = sorted([name for name in reader.fieldnames])
                for row in reader:

                    def _str(field_names):
                        return '|'.join([row[field] for field in field_names])

                    rows[_str(keys)] = _str(fields)
        m = hashlib.md5()
        for _, row in sorted(rows.items()):
            m.update(row.encode())
        return len(rows), m.hexdigest()

    @classmethod
    def scale_hash(cls, paths: str | list[str], scale=1):
        if not isinstance(paths, list):
            paths = [paths]
        tables = list(sorted(cls.tables.items()))
        result = [''] * 2 * len(tables)
        threads = []

        def _calc_hash(result: list[str], index: int):
            fname, table = tables[index]
            count, md5 = cls.calc_hashes([os.path.join(path, fname) for path in paths], table[0])
            result[index * 2] = f'{fname} count: {count}'
            result[index * 2 + 1] = f'{fname} md5: {md5}'

        for index in range(len(tables)):
            threads.append(Thread(target=_calc_hash, args=(result, index)))
            threads[-1].start()
        for t in threads:
            t.join()
        return '\n'.join(result)

    @classmethod
    def setup_class(cls):
        set_canondata_root(f'ydb/tests/functional/{cls.workload}/canondata')

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self._tmp_path = tmp_path

    def tmp_path(self, *paths):
        return os.path.join(self._tmp_path, *paths)

    def get_cannonical(self, paths, execs):
        for exe in execs:
            exe.wait(check_exit_code=True)
        return self.canonical_result(self.scale_hash(paths), self.tmp_path('s1.hash'))

    def test_s1(self):
        out_fpath = self.tmp_path('s1')
        return self.get_cannonical(
            paths=[out_fpath],
            execs=[self.execute_generator(out_fpath)]
        )

    def test_s1_parts(self):
        parts_count = 10
        paths = []
        execs = []
        for part_index in range(parts_count):
            paths.append(self.tmp_path(f's1.{part_index}_{parts_count}'))
            execs.append(
                self.execute_generator(
                    output_path=paths[-1],
                    generator_args=['--proccess-count', parts_count, '--proccess-index', part_index]
                )
            )
        return self.get_cannonical(paths=paths, execs=execs)

    def test_s1_state(self):
        state_path = self.tmp_path('state.json')
        with open(state_path, 'w') as f:
            json.dump({
                'sources': {
                    k: {'position': int(v[1][1] * random.uniform(0.25, 0.75))}
                    for k, v in self.tables.items()
                }
            }, f)
        paths = [self.tmp_path(path) for path in ['s1.1', 's1.2']]
        execs = [
            self.execute_generator(output_path=paths[0]),
            self.execute_generator(
                output_path=paths[1],
                generator_args=['--state', state_path],
            ),
        ]
        return self.get_cannonical(paths=paths, execs=execs)


class TestTpchGenerator(TpcGeneratorBase):
    workload = 'tpch'
    tables = {
        'customer': (['c_custkey'], {1: 150000}),
        'lineitem': (['l_linenumber', 'l_orderkey'], {1: 6001215}),
        'nation': (['n_nationkey'], {1: 25}),
        'orders': (['o_orderkey'], {1: 1500000}),
        'part': (['p_partkey'], {1: 200000}),
        'partsupp': (['ps_partkey', 'ps_suppkey'], {1: 800000}),
        'region': (['r_regionkey'], {1: 5}),
        'supplier': (['s_suppkey'], {1: 10000})
    }


class TestTpcdsGenerator(TpcGeneratorBase):
    workload = 'tpcds'
    tables = {
        'call_center': (['cc_call_center_sk'], {1: 6}),
        'catalog_page': (['cp_catalog_page_sk'], {1: 11718}),
        'catalog_returns': (['cr_item_sk', 'cr_order_number'], {1: 144067}),
        'catalog_sales': (['cs_item_sk', 'cs_order_number'], {1: 1441548}),
        'customer': (['c_customer_sk'], {1: 100000}),
        'customer_address': (['ca_address_sk'], {1: 50000}),
        'customer_demographics': (['cd_demo_sk'], {1: 1920800}),
        'date_dim': (['d_date_sk'], {1: 73049}),
        'household_demographics': (['hd_demo_sk'], {1: 7200}),
        'income_band': (['ib_income_band_sk'], {1: 20}),
        'inventory': (['inv_date_sk', 'inv_item_sk', 'inv_warehouse_sk'], {1: 11745000}),
        'item': (['i_item_sk'], {1: 18000}),
        'promotion': (['p_promo_sk'], {1: 300}),
        'reason': (['r_reason_sk'], {1: 35}),
        'ship_mode': (['sm_ship_mode_sk'], {1: 20}),
        'store': (['s_store_sk'], {1: 12}),
        'store_returns': (['sr_item_sk', 'sr_ticket_number'], {1: 287514}),
        'store_sales': (['ss_item_sk', 'ss_ticket_number'], {1: 2880404}),
        'time_dim': (['t_time_sk'], {1: 86400}),
        'warehouse': (['w_warehouse_sk'], {1: 5}),
        'web_page': (['wp_web_page_sk'], {1: 60}),
        'web_returns': (['wr_item_sk', 'wr_order_number'], {1: 71763}),
        'web_sales': (['ws_item_sk', 'ws_order_number'], {1: 719384}),
        'web_site': (['web_site_sk'], {1: 30}),
    }
