# -*- coding: utf-8 -*-
import os

import pytest

import yatest

from ydb.tests.stress.simple_queue.workload import Workload
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture


class TestStress(MixedClusterFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        output_path = yatest.common.test_output_path()
        self.output_f = open(os.path.join(output_path, "out.log"), "w")
        yield from self.setup_cluster(
            # uncomment for 64 datetime in tpc-h/tpc-ds
            # extra_feature_flags={"enable_table_datetime64": True},

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
            self.get_command_prefix_log(subcmds=["run", "bulk_upsert"], path=store_type)
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
            stdout=self.output_f,
            stderr=self.output_f,
        )

        yatest.common.execute(
            self.get_command_prefix_log(subcmds=["import", "--bulk-size", "1000", "-t", "1", "generator"], path=store_type),
            stdout=self.output_f,
            stderr=self.output_f,
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
            stdout=self.output_f,
            stderr=self.output_f,
        )

        for i, command in enumerate(upload_commands):
            yatest.common.execute(
                command,
                wait=True,
                stdout=self.output_f,
                stderr=self.output_f,
            )

        select.wait()

    @pytest.mark.skip(reason="Too huge logs")
    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_simple_queue(self, store_type: str):
        with Workload(f"grpc://localhost:{self.cluster.nodes[1].grpc_port}", "/Root", 180, store_type) as workload:
            workload.start()

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
        yatest.common.execute(init_command, wait=True, stdout=self.output_f, stderr=self.output_f)
        yatest.common.execute(run_command, wait=True, stdout=self.output_f, stderr=self.output_f)

    @pytest.mark.parametrize("store_type, date64", [
        pytest.param("row",    False, id="row"),
        pytest.param("column", False, id="column"),
        pytest.param("column", True,  id="column-date64")
    ])
    def test_tpch1(self, store_type, date64):
        if date64 and min(self.versions) < (25, 1):
            pytest.skip("date64 is not supported in 24-4")

        if date64:
            date_args = ["--datetime-types=dt64"]
        else:
            date_args = ["--datetime-types=dt32"]

        init_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "init",
            "--store={}".format(store_type),
            "--partition-size=25",
        ] + date_args
        import_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "import",
            "generator",
            "--scale=0.2",
        ]
        run_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "run",
            "--scale=0.2",
            "--check-canonical",
            "--retries",
            "5",  # in row tables we have to retry query by design
        ]

        yatest.common.execute(init_command, wait=True, stdout=self.output_f, stderr=self.output_f)
        yatest.common.execute(import_command, wait=True, stdout=self.output_f, stderr=self.output_f)
        yatest.common.execute(run_command, wait=True, stdout=self.output_f, stderr=self.output_f)

    @pytest.mark.skip(reason="Not stabilized yet")
    @pytest.mark.parametrize("store_type, date64", [
        pytest.param("row",    False, id="row"),
        pytest.param("column", False, id="column"),
        pytest.param("column", True,  id="column-date64")
    ])
    def test_tpcds1(self, store_type, date64):
        if date64 and min(self.versions) < (25, 1):
            pytest.skip("date64 is not supported in 24-4")

        if date64:
            date_args = ["--datetime-types=dt64"]
        else:
            date_args = ["--datetime-types=dt32"]

        init_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "tpcds",
            "-p",
            "tpcds",
            "init",
            "--store={}".format(store_type),
            "--partition-size=25",
        ] + date_args
        import_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "tpcds",
            "-p",
            "tpcds",
            "import",
            "generator",
            "--scale=1",
        ]
        run_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload",
            "tpcds",
            "-p",
            "tpcds",
            "run",
            "--scale=1",
            "--check-canonical",
            "--exclude",
            # not working for row tables
            "5,7,14,18,22,23,24,26,27,31,33,39,46,51,54,56,58,60,61,64,66,67,68,72,75,77,78,79,80,93",
            "--retries",
            "5",  # in row tables we have to retry query by design
        ]

        yatest.common.execute(init_command, wait=True, stdout=self.output_f, stderr=self.output_f)
        yatest.common.execute(import_command, wait=True, stdout=self.output_f, stderr=self.output_f)
        yatest.common.execute(run_command, wait=True, stdout=self.output_f, stderr=self.output_f)
