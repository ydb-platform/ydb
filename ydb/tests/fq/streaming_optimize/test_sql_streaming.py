import os
import codecs
import yatest.common
import pytest

from test_utils import pytest_generate_tests_for_run
from ydb.tests.fq.tools.fqrun import FqRun
from yql_utils import yql_binary_path

ASTDIFF_PATH = yql_binary_path('yql/essentials/tools/astdiff/astdiff')
DATA_PATH = yatest.common.source_path('ydb/tests/fq/streaming_optimize')


@pytest.fixture
def fq_run(request) -> FqRun:
    result = FqRun(
        config_file=os.path.join('ydb/tests/fq/streaming_optimize/cfg', 'app_config.conf'),
        path_prefix=f"{request.function.__name__}_"
    )
    result.replace_config(lambda config: config.replace("${SOLOMON_ENDPOINT}", os.getenv("SOLOMON_HTTP_ENDPOINT")))
    result.add_topic("test_topic_input", [])
    result.add_topic("test_topic_input2", [])
    result.add_topic("test_topic_output", [])
    result.add_topic("test_topic_output2", [])
    return result


def pytest_generate_tests(metafunc):
    return pytest_generate_tests_for_run(metafunc, suites=["suites"], data_path=DATA_PATH)


def test(suite, case, cfg, fq_run):
    program_sql = os.path.join(DATA_PATH, suite, f"{case}.sql")
    with codecs.open(program_sql, encoding="utf-8") as f:
        sql_query = f.read()

    fq_run.add_query(sql_query)
    result = fq_run.yql_exec(action="explain")

    return {
        "Ast": yatest.common.canonical_file(result.opt_file, local=True, diff_tool=ASTDIFF_PATH),
        "Plan": yatest.common.canonical_file(result.plan_file, local=True)
    }
