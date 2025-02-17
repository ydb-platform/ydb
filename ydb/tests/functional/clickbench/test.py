# -*- coding: utf-8 -*-
import os
from ydb.tests.oss.ydb_sdk_import import ydb
import json
from json import encoder
import yatest.common
import pytest
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


def get_queries(filename):
    arcadia_root = yatest.common.source_path('')
    path = os.path.join(arcadia_root, yatest.common.test_source_path(''), filename)
    with open(path, "r") as r:
        data = r.read()
    for query in data.split('\n'):
        if not query:
            continue

        yield query


def format_row(row):
    for col in row:
        if isinstance(row[col], float):
            row[col] = "{:e}".format(row[col])
    return row


def execute_scan_query(driver, yql_text, table_path):
    yql_text = yql_text.replace("$data", table_path)
    success = False
    retries = 10
    while retries > 0 and not success:
        retries -= 1

        if yql_text.startswith('--'):
            return []

        it = driver.table_client.scan_query(yql_text)
        result = []
        while True:
            try:
                response = next(it)
                for row in response.result_set.rows:
                    result.append(format_row(row))

            except StopIteration:
                return result

            except Exception:
                if retries == 0:
                    raise

                break


def remove_optimizer_estimates(query_plan):
    if 'Plans' in query_plan:
        for p in query_plan['Plans']:
            remove_optimizer_estimates(p)
    if 'Operators' in query_plan:
        for op in query_plan['Operators']:
            for key in ['A-Cpu', 'A-Rows', 'E-Cost', 'E-Rows', 'E-Size']:
                if key in op:
                    del op[key]


def sanitize_plan(query_plan):
    if 'queries' not in query_plan:
        return
    for q in query_plan['queries']:
        if 'SimplifiedPlan' in q:
            del q['SimplifiedPlan']
        if 'Plan' in q:
            remove_optimizer_estimates(q['Plan'])


def explain_scan_query(driver, yql_text, table_path):
    yql_text = yql_text.replace("$data", table_path)
    client = ydb.ScriptingClient(driver)
    result = client.explain_yql(
        yql_text,
        ydb.ExplainYqlScriptSettings().with_mode(ydb.ExplainYqlScriptSettings.MODE_EXPLAIN)
    )
    res = json.loads(result.plan)
    sanitize_plan(res)
    print(json.dumps(res))
    return res


def save_canonical_data(data, fname):
    path = os.path.join(yatest.common.output_path(), fname)
    with open(path, "w") as w:
        w.write(
            json.dumps(
                data,
                indent=4,
                sort_keys=True,
            )
        )

    return yatest.common.canonical_file(
        local=True,
        universal_lines=True,
        path=path,
    )


@pytest.mark.parametrize("store", ["row", "column"])
@pytest.mark.parametrize("executer", ["scan", "generic"])
def test_run_benchmark(store, executer):
    path = "clickbench/benchmark/{}/hits".format(store)
    ret = run_cli(["workload", "clickbench", "--path", path, "init", "--store", store, "--datetime"])
    assert_that(ret.exit_code, is_(0))

    ret = run_cli(
        [
            "workload", "clickbench", "--path", path, "import", "files",
            "--input", yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")
        ]
    )
    assert_that(ret.exit_code, is_(0))

    # just validating that benchmark can be executed successfully on this data.
    out_fpath = os.path.join(yatest.common.output_path(), 'click_bench.{}.results'.format(store))
    ret = run_cli(["workload", "clickbench", "--path", path, "run", "--output", out_fpath, "--executer", executer])
    assert_that(ret.exit_code, is_(0))


@pytest.mark.parametrize("store", ["row", "column"])
def test_run_determentistic(store):
    path = "clickbench/determentistic/{}/hits".format(store)
    ret = run_cli(["workload", "clickbench", "--path", path, "init", "--store", store, "--datetime"])
    assert_that(ret.exit_code, is_(0))
    ret = run_cli(
        [
            "workload", "clickbench", "--path", path, "import", "files",
            "--input", yatest.common.source_path("ydb/tests/functional/clickbench/data/hits.csv")
        ]
    )
    assert_that(ret.exit_code, is_(0))

    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/" + os.getenv("YDB_DATABASE"),
            endpoint=os.getenv("YDB_ENDPOINT"),
        )
    )

    driver.wait(5)

    final_results = {}
    for query_id, query in enumerate(get_queries("data/queries-deterministic.sql")):
        results_to_canonize = execute_scan_query(driver, query, "`/local/clickbench/determentistic/{}/hits`".format(store))
        key = "queries-deterministic-results-%s" % str(query_id)
        final_results[key] = save_canonical_data(results_to_canonize, key)
    return final_results


@pytest.mark.parametrize("store", ["row", "column"])
def test_plans(store):
    ret = run_cli(
        ["workload", "clickbench", "--path", "clickbench/plans/{}/hits".format(store), "init", "--store", store, "--datetime"]
    )
    assert_that(ret.exit_code, is_(0))

    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/" + os.getenv("YDB_DATABASE"),
            endpoint=os.getenv("YDB_ENDPOINT"),
        )
    )

    driver.wait(5)

    final_results = {}

    for query_id, query in enumerate(get_queries("data/queries-original.sql")):
        plan = explain_scan_query(driver, query, "`/local/clickbench/plans/{}/hits`".format(store))
        key = "queries-original-plan-{}-{}".format(store, str(query_id))
        final_results[key] = save_canonical_data(plan, key)

    return final_results
