import os
import pytest
import re
import yatest.common

from test_utils import pytest_generate_tests_for_run, get_case_file, get_config
from ydb.tests.fq.tools.kqprun import KqpRun
from yql_utils import get_supported_providers

NEGATIVE_TEMPLATE = ".sqlx"
DATA_PATH = yatest.common.source_path("ydb/tests/fq/streaming_optimize/suites")


@pytest.fixture
def kqp_run(request) -> KqpRun:
    result = KqpRun(
        config_file=os.path.join("ydb/tests/fq/streaming_optimize/cfg", "app_config.conf"),
        scheme_file=os.path.join("ydb/tests/fq/streaming_optimize/cfg", "scheme.sql"),
        path_prefix=f"{request.function.__name__}_",
    )
    result.add_topic("test_topic_input", [])
    result.add_topic("test_topic_input2", [])
    result.add_topic("test_topic_output", [])
    result.add_topic("test_topic_output2", [])
    return result


def sanitize_issues(s):
    # 2022-08-13T16:11:21Z -> ISOTIME
    # 2022-08-13T16:11:21.549879Z -> ISOTIME
    s = re.sub(r"2\d{3}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d+)?Z", "ISOTIME", s)
    # (yexception) ... ->
    s = re.sub(r"\(yexception\).*", "", s)
    # library/cpp/json/json_reader.cpp:420 -> library/cpp/json/json_reader.cpp:xxx
    s = re.sub(r"cpp:\d+", "cpp:xxx", s)
    # Remove DWARF warnings from sanitizer symbolization
    s = re.sub(r"warning: address range table[^\n]*\n?", "", s)
    # Remove LSan suppression summary
    s = re.sub(r"-+\nSuppressions used:\n.*?-+\n?", "", s, flags=re.DOTALL)
    # Collapse multiple consecutive blank lines into one
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s


def pytest_generate_tests(metafunc):
    pytest_generate_tests_for_run(metafunc, NEGATIVE_TEMPLATE, data_path=DATA_PATH)


def test(suite, case, cfg, tmpdir, kqp_run):
    program_sql = get_case_file(DATA_PATH, suite, case, exts=NEGATIVE_TEMPLATE)
    out_dir = tmpdir.mkdir(suite).mkdir(case).dirname
    with open(program_sql, encoding="utf-8") as f:
        sql_query = f.read()

    config = get_config(suite, case, cfg, data_path=DATA_PATH)
    providers = get_supported_providers(config)
    assert not ("pq" in providers and "pq-shared" in providers), \
        f"providers pq and pq-shared are mutually exclusive, on file: {program_sql}"
    shared_reading = str("pq-shared" in providers).upper()
    kqp_run.replace_scheme(lambda scheme: scheme
                           .replace("${PQ_SHARED_READING}", shared_reading)
                           .replace("${KQPRUN_ENDPOINT}", f"localhost:{kqp_run.grpc_port}"))

    kqp_run.add_query(sql_query)
    result = kqp_run.yql_exec(
        check_error=False,
        action="explain",
        var_templates=["SOLOMON_HTTP_ENDPOINT", "SOLOMON_GRPC_ENDPOINT"],
    )

    assert result.execution_result.exit_code != 0, \
        f"execute finished without error, on file: {program_sql}, query:\n{sql_query}"
    sql_stderr = result.std_err.strip()
    assert sql_stderr, \
        f"exit code is {result.execution_result.exit_code}, but error is empty, on file: {program_sql}, query:\n{sql_query}"

    err_file_path = os.path.join(out_dir, "err_file.out")
    with open(err_file_path, "wb") as err_file:
        err_file.write(sanitize_issues(sql_stderr.decode()).encode())

    return [yatest.common.canonical_file(err_file_path)]
