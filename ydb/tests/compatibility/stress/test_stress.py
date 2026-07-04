# -*- coding: utf-8 -*-
import os

import pytest

import yatest

from ydb.tests.stress.simple_queue.workload import Workload
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture


class TestStress(MixedClusterFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        yield from self.setup_cluster(
            # uncomment for 64 datetime in tpc-h/tpc-ds
            # extra_feature_flags=["enable_table_datetime64"],
            column_shard_config={
                'disabled_on_scheme_shard': False,
            },
        )

    @pytest.mark.skip(reason="Too huge logs")
    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_simple_queue(self, store_type: str):
        with Workload(f"grpc://localhost:{self.cluster.nodes[1].grpc_port}", "/Root", 180, store_type) as workload:
            workload.start()

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

        yatest.common.execute(init_command, wait=True)
        yatest.common.execute(import_command, wait=True)
        yatest.common.execute(run_command, wait=True)

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

        yatest.common.execute(init_command, wait=True)
        yatest.common.execute(import_command, wait=True)
        yatest.common.execute(run_command, wait=True)
