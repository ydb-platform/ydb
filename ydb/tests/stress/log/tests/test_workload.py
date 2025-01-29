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
    def get_init_command_prefix(cls) -> list[str]:
        return [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload", "log", "init",
            "--min-partitions", "100",
            "--partition-size", "10",
            "--auto-partition", "0"
        ]

    @classmethod
    def get_run_command_prefix(cls, run_type: str) -> list[str]:
        return [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload", "log", "run", run_type,
            "--seconds", "10",
            "--threads", "10",
            "--client-timeout", "10000"
        ]

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test(self, store_type):
        commands = [
            # init
            self.get_init_command_prefix() + [
                "--path", store_type,
                "--store", store_type,
            ],

            # bulk upsert workload
            self.get_run_command_prefix(run_type='bulk_upsert') + [
                "--path", store_type,
                "--len", "200"
            ],

            # upsert workload
            self.get_run_command_prefix(run_type='upsert') + [
                "--path", store_type,
                "--len", "200"
            ],

            # insert workload
            self.get_run_command_prefix(run_type='insert') + [
                "--path", store_type,
                "--len", "200"
            ],

            # select workload
            self.get_run_command_prefix(run_type='select') + [
                "--path", store_type,
            ]
        ]
        for command in commands:
            res = yatest.common.execute(command, wait=True)
            err: str = res.stderr.decode('UTF-8')
            for line in err.splitlines():
                strip_line = line.strip()
                assert not strip_line or strip_line.find('Using access token from YDB_TOKEN env variable') >= 0, f'Command {command} has errors: {"\n".join(err.splitlines()[:50])}'
