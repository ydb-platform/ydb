import pytest
import os
import re
import codecs
import yatest.common

from ydb.tests.fq.tools.kqprun import KqpRun

from yql_utils import (
    get_supported_providers,
    is_xfail,
    log,
    normalize_source_code_path)

from test_utils import (
    get_config,
    pytest_generate_tests_for_run)

DATA_PATH = yatest.common.source_path('ydb/library/yql/tests/sql/suites')


def sanitize_issues(s):
    # 2022-08-13T16:11:21Z -> ISOTIME
    # 2022-08-13T16:11:21.549879Z -> ISOTIME
    s = re.sub(r"2\d{3}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d+)?Z", "ISOTIME", s)
    # (yexception) ... ->
    s = re.sub(r"\(yexception\).*", "", s)
    # library/cpp/json/json_reader.cpp:420 -> library/cpp/json/json_reader.cpp:xxx
    return re.sub(r"cpp:\d+", "cpp:xxx", s)


def validate_sql(sql_query):
    # Unsupported constructions
    if 'define subquery' in sql_query.lower():
        pytest.skip('Using of system \'kikimr\' is not allowed in SUBQUERY')


def pytest_generate_tests(metafunc):
    return pytest_generate_tests_for_run(metafunc, suites=['solomon'], data_path=DATA_PATH)


def test(suite, case, cfg, solomon):
    config = get_config(suite, case, cfg, data_path=DATA_PATH)

    prov = 'solomon'

    log(get_supported_providers(config))

    if prov not in get_supported_providers(config):
        pytest.skip('%s provider is not supported here' % prov.upper())

    log('===' + suite)
    log('===' + case + '-' + cfg)

    xfail = is_xfail(config)
    program_sql = os.path.join(DATA_PATH, suite, '%s.sql' % case)
    with codecs.open(program_sql, encoding='utf-8') as f:
        sql_query = f.read()
        f.close()

    validate_sql(sql_query)

    kqprun = KqpRun(config_file=os.path.join('ydb/tests/fq/solomon/cfg', 'kqprun_config.conf'),
                    scheme_file=os.path.join('ydb/tests/fq/solomon/cfg', 'kqprun_scheme.sql'))
    yqlrun_res = kqprun.yql_exec(
        yql_program=sql_query,
        var_templates=['SOLOMON_ENDPOINT', 'SOLOMON_PORT'],
        verbose=True,
        check_error=not xfail
    )

    if xfail:
        assert yqlrun_res.execution_result.exit_code != 0
        return [normalize_source_code_path(sanitize_issues(yqlrun_res.std_err.decode('utf-8')))]

    return [yatest.common.canonical_file(yqlrun_res.results_file, local=True)]
