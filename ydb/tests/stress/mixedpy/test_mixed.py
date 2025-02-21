# -*- coding: utf-8 -*-
import os
import sys

import pytest

import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class TestYdbMixedWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator())
        cls.cluster.start()

    @classmethod
    def get_command_prefix(cls, subcmds: list[str]) -> list[str]:
        return [
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '--verbose',
            '--endpoint', 'grpc://localhost:%d' % cls.cluster.nodes[1].grpc_port,
            '--database=/Root',
            'workload', 'mixed'
        ] + subcmds

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @classmethod
    def get_cols_count_command_params(cls) -> list[str]:
        return [
            '--int-cols', '5',
            '--str-cols', '5',
        ]

    @classmethod
    def get_bulk_upsert_cmd(cls, duration: str):
        return cls.get_command_prefix(subcmds=['run', 'bulk_upsert'] + cls.get_cols_count_command_params()) + [
            '-s', duration,
            '-t', '40',
            '--rows', '1000',
            '--window', '20',
            '--len', '1000',
        ]

    @classmethod
    def get_select_cmd(cls, duration: str):
        return cls.get_command_prefix(subcmds=['run', 'select']) + [
            '-s', duration,
            '-t', '5',
            '--rows', '100',
        ]

    @classmethod
    def get_update_cmd(cls, duration: str):
        return cls.get_command_prefix(subcmds=['run', 'upsert'] + cls.get_cols_count_command_params()) + [
            '-s', duration,
            '-t', '5',
            '--rows', '100',
            '--len', '1000',
        ]

    @classmethod
    def print_txs(cls, step: str, filename: str):
        found = False
        with open(filename, 'r') as f:
            for line in f.readlines():
                if found:
                    print('{} txs/sec: {}'.format(step, line.split()[1]))
                    break
                else:
                    words = line.split()
                    if len(words) > 0 and words[0] == 'Txs':
                        found = True

    @pytest.mark.parametrize('store_type', ['row', 'column'])
    def test(self, store_type):
        duration = '120'
        yatest.common.execute(
            self.get_command_prefix(subcmds=['clean']))

        yatest.common.execute(
            self.get_command_prefix(subcmds=['init'] + self.get_cols_count_command_params()) + [
                '--store', store_type,
            ])

        with open('upsert_out', 'w+') as out:
            yatest.common.execute(self.get_bulk_upsert_cmd(duration), stdout=out, stderr=sys.stderr)

        with open('upsert1_out', 'w+') as out:
            bulk_upsert = yatest.common.execute(self.get_bulk_upsert_cmd(duration), wait=False, stdout=out, stderr=sys.stderr)
        select = yatest.common.execute(self.get_select_cmd(duration), wait=False)
        bulk_upsert.wait()
        select.wait()

        with open('upsert2_out', 'w+') as out:
            bulk_upsert = yatest.common.execute(self.get_bulk_upsert_cmd(duration), wait=False, stdout=out, stderr=sys.stderr)
        select = yatest.common.execute(self.get_select_cmd(duration), wait=False)
        update = yatest.common.execute(self.get_update_cmd(duration), wait=False)
        bulk_upsert.wait()
        select.wait()
        update.wait()
        self.print_txs('Upsert', 'upsert_out')
        self.print_txs('Select', 'upsert1_out')
        self.print_txs('Update', 'upsert2_out')
