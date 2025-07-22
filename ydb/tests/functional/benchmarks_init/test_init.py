# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import logging
import pytest

import yatest

from ydb.tests.oss.canonical import set_canondata_root

logger = logging.getLogger(__name__)


def ydb_bin():
    if os.getenv('YDB_CLI_BINARY'):
        return yatest.common.binary_path(os.getenv('YDB_CLI_BINARY'))
    raise RuntimeError('YDB_CLI_BINARY enviroment variable is not specified')


class InitBase(object):
    workload: str

    @classmethod
    def execute_init(cls, scale=1, args=[]):
        return yatest.common.execute(
            [
                ydb_bin(),
                '--endpoint', 'grpc://localhost',
                '--database', '/Root/db',
                'workload', cls.workload, '-p', f'/Root/db/{cls.workload}/s{scale}',
                'init', '--dry-run', '--datetime-types=dt64'
            ]
            + [str(arg) for arg in args]
        ).stdout.decode('utf8')

    @staticmethod
    def canonical_result(output_result, tmp_path):
        with open(tmp_path, 'w') as f:
            f.write(output_result)
        return yatest.common.canonical_file(tmp_path, local=True, universal_lines=True)

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

    def test_s1_column(self):
        return self.canonical_result(self.execute_init(scale=1, args=['--store', 'column']), self.tmp_path('s1_column'))

    def test_s1_row(self):
        return self.canonical_result(self.execute_init(scale=1, args=['--store', 'row']), self.tmp_path('s1_row'))

    def test_s1_s3(self):
        return self.canonical_result(self.execute_init(scale=1, args=[
            '--store', 'external-s3',
            '--external-s3-endpoint', 'https://storage.yandexcloud.net/tpc',
            '--external-s3-prefix',
            'h/s1/parquet'
        ]), self.tmp_path('s1_s3'))


class TpcInitBase(InitBase):
    def test_s1_column_decimal(self):
        return self.canonical_result(self.execute_init(scale=1, args=['--store', 'column', '--float-mode', 'decimal']), self.tmp_path('s1_column_decimal'))

    def test_s1_column_decimal_ydb(self):
        return self.canonical_result(self.execute_init(scale=1, args=['--store', 'column', '--float-mode', 'decimal_ydb']), self.tmp_path('s1_column_decimal_ydb'))

    def test_s100_column(self):
        return self.canonical_result(self.execute_init(scale=1, args=['--store', 'column', '--scale', '100']), self.tmp_path('s100_column'))


class TestTpchInit(TpcInitBase):
    workload = 'tpch'


class TestTpcdsInit(TpcInitBase):
    workload = 'tpcds'


class TestClickbenchInit(InitBase):
    workload = 'clickbench'
