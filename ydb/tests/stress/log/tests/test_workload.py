# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbLogWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                'disabled_on_scheme_shard': False,
            })

    def get_command_prefix(self, subcmds: list[str], path: str) -> list[str]:
        return [
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '--verbose',
            '--endpoint', self.endpoint,
            '--database={}'.format(self.database),
            'workload', 'log'
        ] + subcmds + [
            '--path', path
        ]

    @classmethod
    def get_insert_command_params(cls) -> list[str]:
        return [
            '--int-cols', '2',
            '--str-cols', '5',
            '--key-cols', '4',
            '--len', '200',
        ]

    @pytest.mark.parametrize('store_type', ['row', 'column'])
    def test(self, store_type):
        upload_commands = [
            # import command
            self.get_command_prefix(subcmds=['import', '--bulk-size', '1000', '-t', '1', 'generator'], path=store_type) + self.get_insert_command_params() + ['--rows', '100000'],
            # bulk upsert workload
            self.get_command_prefix(subcmds=['run', 'bulk_upsert'], path=store_type) + self.get_insert_command_params() + ['--seconds', '10', '--threads', '10'],

            # upsert workload
            self.get_command_prefix(subcmds=['run', 'upsert'], path=store_type) + self.get_insert_command_params() + ['--seconds', '10', '--threads', '10'],

            # insert workload
            self.get_command_prefix(subcmds=['run', 'insert'], path=store_type) + self.get_insert_command_params() + ['--seconds', '10', '--threads', '10'],
        ]

        # init
        yatest.common.execute(
            self.get_command_prefix(subcmds=['init'], path=store_type) + self.get_insert_command_params() + [
                '--store', store_type,
                '--min-partitions', '100',
                '--partition-size', '10',
                '--auto-partition', '0',
            ],
        )

        select = yatest.common.execute(
            self.get_command_prefix(subcmds=['run', 'select'], path=store_type) + [
                '--client-timeout', '10000',
                '--threads', '10',
                '--seconds', str(10 * len(upload_commands)),
            ], wait=False)

        for command in upload_commands:
            yatest.common.execute(command, wait=True)

        select.wait()
