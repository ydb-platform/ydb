# -*- coding: utf-8 -*-
import os
import time

import pytest

import yatest

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture


class TestRolling(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1, because of enable_column_store flag")

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_column_store": True,
            },

            column_shard_config={
                "disabled_on_scheme_shard": False,
            },
        )

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
            "10000000",  # infinity
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
        run = yatest.common.execute(run_command, wait=False)

        for _ in self.roll():
            time.sleep(5)

        run.kill()
