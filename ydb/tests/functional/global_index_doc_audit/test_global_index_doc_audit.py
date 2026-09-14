# -*- coding: utf-8 -*-
import os
import re
import time

import yatest.common

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

import ydb


class TestGlobalIndexDocumentationAudit:
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database="/Root",
                endpoint=cls.cluster.nodes[1].endpoint,
            )
        )
        cls.driver.wait(timeout=60)

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    @classmethod
    def execute_cli(cls, args):
        binary = yatest.common.binary_path(os.environ["YDB_CLI_BINARY"])
        result = yatest.common.execute(
            [
                binary,
                "--endpoint", f"grpc://{cls.cluster.nodes[1].host}:{cls.cluster.nodes[1].grpc_port}",
                "--database", "/Root",
            ] + args,
            check_exit_code=False,
        )
        stdout = result.std_out.decode("utf-8") if result.std_out else ""
        stderr = result.std_err.decode("utf-8") if result.std_err else ""
        assert result.exit_code == 0, stdout + stderr
        return stdout + stderr

    def test_successful_build_on_empty_partitions_has_no_error_issues(self):
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(
                """
                    CREATE TABLE `empty_partitioned` (
                        id Uint64 NOT NULL,
                        value Uint64,
                        PRIMARY KEY (id)
                    ) WITH (UNIFORM_PARTITIONS = 4);
                """
            )

        output = self.execute_cli([
            "table", "index", "add", "global-sync", "empty_partitioned",
            "--index-name", "idx_value", "--columns", "value",
        ])
        operation_id = re.search(r"ydb://buildindex/\d+\?id=\d+", output)
        assert operation_id, output

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            output = self.execute_cli(["operation", "get", operation_id.group(0)])
            if "true" in output and "SUCCESS" in output:
                break
            time.sleep(0.1)
        else:
            raise AssertionError(f"Index build did not finish:\n{output}")

        assert "Shard or requested range is empty" not in output, output
