import os

import yatest.common

from utils import pytest_generate_tests_by_template, DATA_PATH, SQLRUN_PATH, SQL_FLAGS


def pytest_generate_tests(metafunc):
    return pytest_generate_tests_by_template('.sql', metafunc)


def get_sql2yql_cmd(suite, case, case_file, out_dir, ansi_lexer, test_format, test_double_format):
    cmd = [
        SQLRUN_PATH,
        case_file,
        '--syntax-version=1'
    ]

    if ansi_lexer:
        cmd.append('--ansi-lexer')
    if test_format:
        cmd.append('--test-format')
        cmd.append('--format-output=%s' % os.path.join(out_dir, 'formatted.sql'))
        if test_double_format:
            cmd.append('--test-double-format')
    else:
        cmd.append('--yql')
        cmd.append('--output=%s' % os.path.join(out_dir, 'sql.yql'))
    if suite == 'kikimr':
        cmd.append('--cluster=plato@kikimr')
    if suite == 'rtmr':
        cmd.append('--cluster=plato@rtmr')
        if case.startswith('solomon'):
            cmd.append('--cluster=local_solomon@solomon')

    if suite == 'rtmr_ydb':
        cmd.append('--cluster=plato@rtmr')
        cmd.append('--cluster=local@kikimr')
    if suite == 'streaming':
        cmd.append('--cluster=pq@pq')
        cmd.append('--cluster=solomon@solomon')
    if suite == 'solomon':
        cmd.append('--cluster=local_solomon@solomon')

    if SQL_FLAGS:
        cmd.append('--flags=%s' % ','.join(SQL_FLAGS))

    return cmd


def test(suite, case, tmpdir):
    files = []
    # case can contain slash because of nested suites
    out_dir = tmpdir.mkdir(suite).mkdir(case.replace('/', '_')).dirname
    case_file = os.path.join(DATA_PATH, suite, '%s.sql' % case)
    with open(case_file, 'r') as f:
        content = f.read()
        ansi_lexer = 'ansi_lexer' in content
    cmd = get_sql2yql_cmd(suite, case, case_file, out_dir, ansi_lexer, test_format=False, test_double_format=False)
    yatest.common.process.execute(cmd, env={'YQL_DETERMINISTIC_MODE': '1'})
    files.append(os.path.join(out_dir, 'sql.yql'))

    return [yatest.common.canonical_file(file_name) for file_name in files]
