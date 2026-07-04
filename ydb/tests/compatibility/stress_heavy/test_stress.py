# -*- coding: utf-8 -*-
import os

import pytest

import yatest

from ydb.tests.library.compatibility.fixtures import MixedClusterFixture


class TestStress(MixedClusterFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                'disabled_on_scheme_shard': False,
            },
        )

    def get_command_prefix_log(self, subcmds: list[str], path: str) -> list[str]:
        return (
            [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "--endpoint",
                "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database=/Root",
                "workload",
                "log",
            ]
            + subcmds
            + ["--path", path]
        )

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_log(self, store_type):
        timeout_scale = 60

        upload_commands = [
            # bulk upsert workload
            self.get_command_prefix_log(subcmds=["run", "bulk-upsert"], path=store_type)
            + ["--seconds", str(timeout_scale), "--threads", "10", "--rows", "2000"],
            # upsert workload
            self.get_command_prefix_log(subcmds=["run", "upsert"], path=store_type)
            + ["--seconds", str(timeout_scale), "--threads", "10"],
            # insert workload
            self.get_command_prefix_log(subcmds=["run", "insert"], path=store_type)
            + ["--seconds", str(timeout_scale), "--threads", "10"],
        ]
        # init
        yatest.common.execute(
            self.get_command_prefix_log(subcmds=["init"], path=store_type)
            + [
                "--store",
                store_type,
                "--min-partitions",
                "100",
                "--partition-size",
                "10",
                "--auto-partition",
                "0",
                "--ttl",
                "10",
            ],
        )

        yatest.common.execute(
            self.get_command_prefix_log(subcmds=["import", "--bulk-size", "1000", "-t", "1", "generator"], path=store_type),
            wait=True,
        )
        select = yatest.common.execute(
            self.get_command_prefix_log(subcmds=["run", "select"], path=store_type)
            + [
                "--client-timeout",
                "10000",
                "--threads",
                "10",
                "--seconds",
                str(timeout_scale * len(upload_commands)),
            ],
            wait=False,
        )

        for i, command in enumerate(upload_commands):
            yatest.common.execute(
                command,
                wait=True,
            )

        select.wait()

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_kv(self, store_type):
        init_command_prefix = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "kv",
            "init",
            "--min-partitions",
            "10",
            "--partition-size",
            "10",
            "--auto-partition",
            "0",
            "--init-upserts",
            "0",
            "--cols",
            "5",
            "--int-cols",
            "2",
            "--key-cols",
            "3",
        ]

        run_command_prefix = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "kv",
            "run",
            "mixed",
            "--seconds",
            "180",
            "--threads",
            "10",
            "--cols",
            "5",
            "--len",
            "200",
            "--int-cols",
            "2",
            "--key-cols",
            "3",
        ]

        init_command = init_command_prefix
        init_command.extend(
            [
                "--path",
                store_type,
                "--store",
                store_type,
            ]
        )
        run_command = run_command_prefix
        run_command.extend(
            [
                "--path",
                store_type,
            ]
        )
        yatest.common.execute(init_command, wait=True)
        yatest.common.execute(run_command, wait=True)
