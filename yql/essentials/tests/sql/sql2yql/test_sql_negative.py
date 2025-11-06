import os
import pytest
import yatest.common
from yql_utils import get_supported_providers, get_param, is_xsqlfail

from test_utils import pytest_generate_tests_for_run, get_config, SQLRUN_PATH, SQL_FLAGS, get_case_file

DATA_PATH = yatest.common.source_path('yql/essentials/tests/sql/suites')


def pytest_generate_tests(metafunc):
    pytest_generate_tests_for_run(metafunc, template=['.sql', '.yql', '.sqlx'], data_path=DATA_PATH)


def run_sql2yql(program_sql, out_dir, err_file_path):
    def out_file(name):
        return os.path.join(out_dir, name)

    # translate sql to yql
    program_yql = out_file('program.yql')

    cmd_sql = [
        SQLRUN_PATH,
        '--yql',
        '--output=' + program_yql,
        '--syntax-version=1',
        '/dev/stdin',
    ]

    if SQL_FLAGS:
        cmd_sql.append('--flags=%s' % ','.join(SQL_FLAGS))

    with open(program_sql) as f:
        sql_res = yatest.common.process.execute(cmd_sql, check_exit_code=False, stdin=f, env={'YQL_DETERMINISTIC_MODE': '1'})

    if sql_res.exit_code:
        sql_stderr = sql_res.std_err.strip()
        assert sql_stderr
        with open(err_file_path, 'wb') as err_file:
            err_file.write(sql_stderr)

    return sql_res


def test(suite, case, cfg, tmpdir):
    config = get_config(suite, case, cfg, DATA_PATH)

    if 'yt' not in get_supported_providers(config):
        pytest.skip('YT provider is not supported here')

    if get_param('TARGET_PLATFORM'):
        if "yson" in case:
            pytest.skip('yson is not supported on non-default target platform')

    program_sql = get_case_file(DATA_PATH, suite, case, ['.sql', '.yql', '.sqlx'])

    if not is_xsqlfail(config, program_sql):
        pytest.skip('only xsqlfail is supported in this mode')

    out_dir = tmpdir.mkdir(suite).mkdir(case).dirname
    files = []

    err_file_path = os.path.join(out_dir, 'err_file.out')
    res = run_sql2yql(program_sql, out_dir, err_file_path)

    with open(program_sql) as f:
        program_sql_content = f.read()

    assert res.exit_code != 0, 'execute finished without error, on file: %s, query:\n%s' % \
                               (program_sql, program_sql_content)
    assert os.path.getsize(err_file_path) > 0, 'exit code is %d, but error is empty, on file: %s, query:\n%s' % \
                                               (res.exit_code, program_sql, program_sql_content)

    files.append(err_file_path)

    return [yatest.common.canonical_file(file_name) for file_name in files]
