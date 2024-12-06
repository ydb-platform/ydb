# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import logging
import hashlib
import pytest
import json
import random

import yatest

from ydb.tests.oss.canonical import set_canondata_root
from threading import Thread

logger = logging.getLogger(__name__)


def ydb_bin():
    if os.getenv('YDB_CLI_BINARY'):
        return yatest.common.binary_path(os.getenv('YDB_CLI_BINARY'))
    raise RuntimeError('YDB_CLI_BINARY enviroment variable is not specified')


class TpcGeneratorBase(object):
    tables: dict[str, dict[int, int]]
    workload: str

    @classmethod
    def execute_generator(cls, output_path, scale=1, import_args=[], generator_args=[]):
        return yatest.common.execute(
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
        return yatest.common.canonical_file(tmp_path, local=True, universal_lines=True)

    @staticmethod
    def calc_hashes(files: str | list[str]):
        if not isinstance(files, list):
            files = [files]
        rows: set[str] = set()
        for file_path in files:
            if not os.path.exists(file_path):
                continue
            first_line = True
            with open(file_path, 'r') as f:
                for line in f:
                    if first_line:
                        first_line = False
                    else:
                        rows.add(line)
        m = hashlib.md5()
        for row in sorted(rows):
            m.update(row.encode())
        return len(rows), m.hexdigest()

    @classmethod
    def scale_hash(cls, paths: str | list[str]):
        if not isinstance(paths, list):
            paths = [paths]
        tables = list(sorted(cls.tables.items()))
        result = [''] * 2 * len(tables)
        threads = []

        def _calc_hash(result: list[str], index: int):
            fname, _ = tables[index]
            count, md5 = cls.calc_hashes([os.path.join(path, fname) for path in paths])
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
                    k: {'position': int(v[1] * random.uniform(0.25, 0.75))}
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

    def test_s1_state_and_parts(self):
        state = [self.tmp_path(f'state_{i}.json') for i in range(2)]
        paths = [self.tmp_path(f's1.{i}') for i in range(4)]
        execs = [
            self.execute_generator(
                output_path=paths[0],
                generator_args=['--state', state[0], '--proccess-count', 4, '--proccess-index', 0],
                import_args=['-t', 1]
            ),
            self.execute_generator(
                output_path=paths[1],
                generator_args=['--state', state[1], '--proccess-count', 4, '--proccess-index', 2],
                import_args=['-t', 1]
            )
        ]
        for e in execs:
            e.wait(check_exit_code=True)
        execs += [
            self.execute_generator(
                output_path=paths[2],
                generator_args=['--state', state[0], '--proccess-count', 2, '--proccess-index', 0],
            ),
            self.execute_generator(
                output_path=paths[3],
                generator_args=['--state', state[1], '--proccess-count', 2, '--proccess-index', 1],
            )
        ]
        for e in execs:
            e.wait(check_exit_code=True)
        counts = {}
        for p in paths:
            for table_name, _ in self.tables.items():
                fpath = os.path.join(p, table_name)
                if os.path.exists(fpath):
                    with open(fpath, 'r') as f:
                        counts[table_name] = counts.get(table_name, 0) + len(f.readlines()) - 1
        for table_name, _ in self.tables.items():
            c = counts.get(table_name, 0)
            e = self.tables[table_name][1]
            if c - e > 5 * len(paths):              # some lines can be rebuild on continue building
                pytest.fail(f'Too many lines was generated for table `{table_name}`, fact: {c}, expected: {e}')

        return self.get_cannonical(paths=paths, execs=execs)


class TestTpchGenerator(TpcGeneratorBase):
    workload = 'tpch'
    tables = {
        'customer': {1: 150000},
        'lineitem': {1: 6001215},
        'nation': {1: 25},
        'orders': {1: 1500000},
        'part': {1: 200000},
        'partsupp': {1: 800000},
        'region': {1: 5},
        'supplier': {1: 10000}
    }


class TestTpcdsGenerator(TpcGeneratorBase):
    workload = 'tpcds'
    tables = {
        'call_center': {1: 6},
        'catalog_page': {1: 11718},
        'catalog_returns': {1: 144067},
        'catalog_sales': {1: 1441548},
        'customer': {1: 100000},
        'customer_address': {1: 50000},
        'customer_demographics': {1: 1920800},
        'date_dim': {1: 73049},
        'household_demographics': {1: 7200},
        'income_band': {1: 20},
        'inventory': {1: 11745000},
        'item': {1: 18000},
        'promotion': {1: 300},
        'reason': {1: 35},
        'ship_mode': {1: 20},
        'store':  {1: 12},
        'store_returns': {1: 287514},
        'store_sales': {1: 2880404},
        'time_dim': {1: 86400},
        'warehouse': {1: 5},
        'web_page': {1: 60},
        'web_returns': {1: 71763},
        'web_sales': {1: 719384},
        'web_site':  {1: 30}
    }
