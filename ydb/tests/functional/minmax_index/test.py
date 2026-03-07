# -*- coding: utf-8 -*-
import os
import json
from json import encoder
import yatest.common
from hamcrest import assert_that, is_
encoder.FLOAT_REPR = lambda o: format(o, '{:e}')


WH_COUNT = "2"
RUN_TIME = "65s"


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


def test_log_compaction_init():
    string_cols = 1000
    int_cols = 1000
    rows = 10000
    ret = run_cli([
        "workload","log", "init", "--clear", "--store", "column","--str-cols", str(string_cols), "--int-cols", str(int_cols), "--key-cols", "5","--len", "15",  "--ttl", "60", "--null-percent", "90"])
    assert_that(ret.exit_code, is_(0))

    total_columns = int(int_cols) + int(string_cols)
    assert_that(total_columns % 20, is_(0))
    calls = total_columns // 20
    for i in range(calls):
        stmt = ""
        for j in range(20):
            col_num = i * (total_columns // 20) + j
            stmt += f"ALTER OBJECT `/Root/testdb/log_writer_test` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_c{col_num}_minmax, TYPE=MINMAX, FEATURES=`{{\"column_name\" : \"c{col_num}\"}}`);\n"
        ret = run_cli(
            ["sql", "-s", stmt]
        )
        assert_that(ret.exit_code, is_(0), f"Failed to add minmax index to columns {i} of {calls}")

    ret = run_cli([
        "workload","log", "import", "--bulk-size", "100", "generator", "--key-cols", "5","--len", "15", "--str-cols", str(string_cols), "--int-cols", str(int_cols), "--rows", str(rows)]
    )
    assert_that(ret.exit_code, is_(0))