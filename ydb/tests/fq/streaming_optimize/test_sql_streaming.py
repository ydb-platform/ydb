import os
import pytest
import yatest.common

from test_utils import pytest_generate_tests_for_run, get_case_file, get_config
from ydb.tests.fq.tools.kqprun import KqpRun
from yql_utils import get_supported_providers, yql_binary_path

ASTDIFF_PATH = yql_binary_path('yql/essentials/tools/astdiff/astdiff')
DATA_PATH = yatest.common.source_path('ydb/tests/fq/streaming_optimize/suites')


@pytest.fixture
def kqp_run(request) -> KqpRun:
    result = KqpRun(
        config_file=os.path.join('ydb/tests/fq/streaming_optimize/cfg', 'app_config.conf'),
        scheme_file=os.path.join('ydb/tests/fq/streaming_optimize/cfg', 'scheme.sql'),
        path_prefix=f"{request.function.__name__}_",
    )
    result.add_topic("test_topic_input", [])
    result.add_topic("test_topic_input2", [])
    result.add_topic("test_topic_output", [])
    result.add_topic("test_topic_output2", [])
    return result


def pytest_generate_tests(metafunc):
    return pytest_generate_tests_for_run(metafunc, data_path=DATA_PATH)


def test(suite, case, cfg, kqp_run):
    program_sql = get_case_file(DATA_PATH, suite, case)
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
        action="explain",
        var_templates=["SOLOMON_HTTP_ENDPOINT", "SOLOMON_GRPC_ENDPOINT"],
    )

    return {
        "Ast": yatest.common.canonical_file(result.opt_file, local=True, diff_tool=ASTDIFF_PATH),
        "Plan": yatest.common.canonical_file(result.plan_file, local=True)
    }
