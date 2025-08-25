# -*- coding: utf-8 -*-
import os

import pytest
import yatest


from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test(self, store_type):
        init_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint", self.endpoint,
            "--database={}".format(self.database),
            "workload", "kv", "init",
            "--min-partitions", "1",
            "--partition-size", "10",
            "--auto-partition", "0",
            "--init-upserts", "0",
            "--cols", "5",
            "--int-cols", "2",
            "--key-cols", "3",
        ]

        run_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload", "kv", "run", "mixed",
            "--seconds", "100",
            "--threads", "10",
            "--cols", "5",
            "--len", "200",
            "--int-cols", "2",
            "--key-cols", "3"
        ]

        init_command.extend([
            "--path", store_type,
            "--store", store_type,
        ])
        run_command.extend([
            "--path", store_type,
        ])
        yatest.common.execute(init_command, wait=True)
        yatest.common.execute(run_command, wait=True)
