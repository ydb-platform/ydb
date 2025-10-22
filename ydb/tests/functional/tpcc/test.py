# -*- coding: utf-8 -*-
import os
import json
from json import encoder
import yatest.common
from hamcrest import assert_that, is_
encoder.FLOAT_REPR = lambda o: format(o, '{:e}')


def ydb_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
    raise RuntimeError("YDB_CLI_BINARY enviroment variable is not specified")


def run_cli(argv):
    return yatest.common.execute(
        [
            ydb_bin(),
            "--endpoint",
            "grpc://" + os.getenv("YDB_ENDPOINT"),
            "--database",
            "/" + os.getenv("YDB_DATABASE"),
        ] + argv
    )


def test_run_benchmark():
    ret = run_cli(["workload", "tpcc", "init", "-w", "10"])
    assert_that(ret.exit_code, is_(0))

    ret = run_cli(["workload", "tpcc", "import", "-w", "10"])
    assert_that(ret.exit_code, is_(0))

    # this actually checks that import has imported the data
    ret = run_cli(["workload", "tpcc", "check", "-w", "10", "--just-imported"])
    assert_that(ret.exit_code, is_(0))

    ret = run_cli(["workload", "tpcc", "run", "-w", "10", "-f", "Json", "--warmup", "10s", "--time", "2m"])
    assert_that(ret.exit_code, is_(0))

    run_result = json.loads(ret.stdout.decode("utf-8"))
    efficiency = run_result["summary"]["efficiency"]

    # This value is somewhat arbitrary: large enough to reveal issues,
    # but not so high that it becomes flaky due to environment or short runs.
    assert_that(efficiency > 60)

    # Above we verified that at least 60% of transactions succeeded;
    # here we ensure that those transactions did not break data consistency.
    ret = run_cli(["workload", "tpcc", "check", "-w", "10"])
    assert_that(ret.exit_code, is_(0))
