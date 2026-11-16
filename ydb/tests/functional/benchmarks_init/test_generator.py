# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import deque

from itertools import dropwhile, islice, zip_longest

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
        parts_count = 4
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

    def test_s1_state(self):
        # Checkpoints count source rows. One store_sales source row is a ticket
        # that generates its sales rows and the associated store_returns rows.
        # Multiples of six are valid TPC-DS resume boundaries.
        positions = {'store_sales': 120_000, 'date_dim': 36_000}
        state_path = self.tmp_path('state.json')
        with open(state_path, 'w') as f:
            json.dump({'sources': {table: {'position': pos} for table, pos in positions.items()}}, f)

        full_path = self.tmp_path('full')
        resumed_path = self.tmp_path('resumed')
        tables = ['--tables', 'store_sales,date_dim']
        # Single-threaded output lets us compare the complete tail in order,
        # including duplicates, without sorting or retaining the rows in memory.
        full = self.execute_generator(full_path, import_args=['-t', 1], generator_args=tables)
        resumed = self.execute_generator(resumed_path, import_args=['-t', 1], generator_args=tables + ['--state', state_path])
        full.wait(check_exit_code=True)
        resumed.wait(check_exit_code=True)

        for table, ticket_field in (
            ('store_sales', 'ss_ticket_number'),
            ('store_returns', 'sr_ticket_number'),
            ('date_dim', None),
        ):
            with open(os.path.join(full_path, table)) as reference, open(os.path.join(resumed_path, table)) as actual:
                header = next(reference)
                assert next(actual) == header, f'{table}: header changed after resume'
                if ticket_field is None:
                    expected = islice(reference, positions[table], None)
                else:
                    ticket_index = header.rstrip('\n').split('|').index(ticket_field)
                    expected = dropwhile(
                        lambda row: int(row.split('|', ticket_index + 1)[ticket_index]) <= positions['store_sales'],
                        reference,
                    )
                row_count = 0
                for row_count, (expected_row, actual_row) in enumerate(zip_longest(expected, actual), 1):
                    assert actual_row == expected_row, f'{table}: resumed row {row_count} differs'
                assert row_count > 0, f'{table}: empty resumed tail'
                logger.info('%s: verified %s resumed rows', table, row_count)

    def test_s1_state_and_parts(self):
        tables = 'store_sales,date_dim,inventory'
        checkpoints = [self.tmp_path(f'state_{i}.json') for i in range(2)]
        paths = [self.tmp_path(f'part_{i}') for i in range(4)]

        def generate(path, checkpoint, part_count, part_index, selected_tables=tables, scale=1):
            return self.execute_generator(
                path,
                scale=scale,
                import_args=['-t', 1],
                generator_args=['--tables', selected_tables,
                    '--state', checkpoint,
                    '--proccess-count', part_count,
                    '--proccess-index', part_index,
                ],
            )

        initial = [
            generate(paths[0], checkpoints[0], 4, 0, 'inventory'),
            generate(paths[2], checkpoints[1], 4, 2, 'inventory'),
        ]
        for exe in initial:
            exe.wait(check_exit_code=True)

        # Native SF1 assigns these smaller tables to part 0. Write a
        # fractional-scale prefix into the same checkpoint to exercise a
        # non-aligned resume in store_sales as well as repartitioning inventory.
        prefix_path = self.tmp_path('small_table_prefix')
        prefix = generate(prefix_path, checkpoints[0], 4, 0, 'store_sales,date_dim', scale=0.50001)
        prefix.wait(check_exit_code=True)
        with open(checkpoints[0]) as f:
            assert json.load(f)['sources']['store_sales']['position'] == 120_002

        inventory_positions = []
        for checkpoint in checkpoints:
            with open(checkpoint) as f:
                inventory_positions.append(json.load(f)['sources']['inventory']['position'])
        assert all(position > 0 for position in inventory_positions), inventory_positions
        logger.info('Resuming inventory checkpoints %s with the part count changed from 4 to 2', inventory_positions)

        resumed = [
            generate(paths[1], checkpoints[0], 2, 0),
            generate(paths[3], checkpoints[1], 2, 1),
        ]
        reference_path = self.tmp_path('reference')
        reference = self.execute_generator(reference_path, import_args=['-t', 1], generator_args=['--tables', tables])
        for exe in [*resumed, reference]:
            exe.wait(check_exit_code=True)

        for table in ('store_sales', 'store_returns', 'date_dim', 'inventory'):
            boundary_limit = 5 * len(paths)
            recent_rows = deque(maxlen=boundary_limit)
            row_count = 0
            duplicates = []
            part_counts = []
            with open(os.path.join(reference_path, table)) as reference_rows:
                header = next(reference_rows)
                table_paths = paths if table == 'inventory' else [prefix_path, *paths[1:]]
                for path in table_paths:
                    part_path = os.path.join(path, table)
                    overlap = 0
                    part_count = 0
                    if os.path.exists(part_path):
                        with open(part_path) as rows:
                            assert next(rows) == header, f'{table}: header differs in {path}'
                            at_boundary = True
                            for row in rows:
                                if at_boundary and row in recent_rows:
                                    overlap += 1
                                    assert overlap <= boundary_limit, f'{table}: too many boundary repeats in {path}'
                                    continue
                                at_boundary = False
                                row_count += 1
                                part_count += 1
                                assert row == next(reference_rows, None), f'{table}: row {row_count} differs in {path}'
                                recent_rows.append(row)
                    duplicates.append(overlap)
                    part_counts.append(part_count)
                assert next(reference_rows, None) is None, f'{table}: incomplete result'
            assert row_count == self.tables[table][1], f'{table}: unexpected row count {row_count}'
            assert sum(duplicates) <= boundary_limit, f'{table}: too many boundary repeats {duplicates}'
            if table == 'inventory':
                assert all(part_counts), f'inventory: empty initial or resumed part: {part_counts}'
            else:
                assert part_counts[2:] == [0, 0], f'{table}: rows in a nonzero part'
                assert not any(duplicates[i] for i in (0, 2, 3)), f'{table}: repeats outside the resume boundary'
            if table == 'store_sales':
                assert duplicates[1] > 0, 'store_sales resume did not exercise boundary repeats'
            logger.info('%s: verified rows by part: %s; boundary repeats: %s', table, part_counts, duplicates)
