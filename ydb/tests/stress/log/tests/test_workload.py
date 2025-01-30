# -*- coding: utf-8 -*-
import os

import pytest

import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class TestYdbLogWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator())
        cls.cluster.start()

    @classmethod
    def get_command_prefix(cls, subcmds: list[str], path: str) -> list[str]:
        return [
            yatest.common.binary_path(os.getenv('YDB_CLI_BINARY')),
            '--verbose',
            '--endpoint', 'grpc://localhost:%d' % cls.cluster.nodes[1].grpc_port,
            '--database=/Root',
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

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.mark.parametrize('store_type', ['row', 'column'])
    def test(self, store_type):
        commands = [
            # init
            self.get_command_prefix(subcmds=['init'], path=store_type) + self.get_insert_command_params() + [
                '--store', store_type,
                '--min-partitions', '100',
                '--partition-size', '10',
                '--auto-partition', '0',
            ],

            # bulk upsert workload
            self.get_command_prefix(subcmds=['run', 'bulk_upsert'], path=store_type) + self.get_insert_command_params(),

            # upsert workload
            self.get_command_prefix(subcmds=['run', 'upsert'], path=store_type) + self.get_insert_command_params(),

            # insert workload
            self.get_command_prefix(subcmds=['run', 'insert'], path=store_type) + self.get_insert_command_params(),

            # select workload
            self.get_command_prefix(subcmds=['run', 'select'], path=store_type) + [
                '--client-timeout', '10000'
            ]
        ]
        for command in commands:
            yatest.common.execute(command, wait=True)
