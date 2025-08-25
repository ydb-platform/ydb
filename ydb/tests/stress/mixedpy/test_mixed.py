# -*- coding: utf-8 -*-
import os
import sys
import pytest
import yatest

from ydb.tests.olap.lib.utils import get_external_param

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def get_command_prefix(self, subcmds: list[str]) -> list[str]:
        return [
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
            'workload', 'mixed'
        ] + subcmds

    @classmethod
    def get_cols_count_command_params(cls) -> list[str]:
        return [
            '--int-cols', '5',
            '--str-cols', '5',
        ]

    def get_bulk_upsert_cmd(self, duration: str):
        return self.get_command_prefix(subcmds=['run', 'bulk_upsert'] + self.get_cols_count_command_params()) + [
            '-s', duration,
            '-t', '40',
            '--rows', '1000',
            '--window', '20',
            '--len', '1000',
        ]

    def get_select_cmd(self, duration: str):
        return self.get_command_prefix(subcmds=['run', 'select']) + [
            '-s', duration,
            '-t', '5',
            '--rows', '100',
        ]

    def get_update_cmd(self, duration: str):
        return self.get_command_prefix(subcmds=['run', 'upsert'] + self.get_cols_count_command_params()) + [
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
        duration = get_external_param('duration', '120')
        self.endpoint = get_external_param('endpoint', self.endpoint)
        self.database = get_external_param('database', self.database)
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
