import os
import yatest.common
import pytest

from test_sql2yql import get_sql2yql_cmd
from test_utils import pytest_generate_sql_tests, get_case_file, get_config
from yql_utils import is_xsqlfail

DATA_PATH = yatest.common.source_path('yql/essentials/tests/sql/suites')


def pytest_generate_tests(metafunc):
    return pytest_generate_sql_tests(metafunc, data_path=DATA_PATH)


def _get_cfg_path(suite, case, data_path):
    cfg_path = os.path.join(data_path, suite, case)
    if os.path.exists(cfg_path + '.cfg'):
        return ""
    else:
        return "default.txt"


def test(suite, case, tmpdir):
    config = get_config(suite, case, _get_cfg_path(suite, case, DATA_PATH), DATA_PATH)

    files = []
    # case can contain slash because of nested suites
    out_dir = tmpdir.mkdir(suite).mkdir(case.replace('/', '_')).dirname
    case_file = get_case_file(DATA_PATH, suite, case)
    if is_xsqlfail(config, case_file):
        pytest.skip('xfail is not supported in this mode')

    ansi_lexer = False
    with open(case_file, 'r') as f:
        content = f.read()
        ansi_lexer = 'ansi_lexer' in content
        test_double_format = 'skip double format' not in content
        if 'syntax_pg' in content:
            pytest.skip('syntax_pg')
    cmd = get_sql2yql_cmd(suite, case, case_file, out_dir=out_dir, ansi_lexer=ansi_lexer, test_format=True, test_double_format=test_double_format)
    yatest.common.process.execute(cmd, env={'YQL_DETERMINISTIC_MODE': '1'})
    files.append(os.path.join(out_dir, 'formatted.sql'))

    return [yatest.common.canonical_file(file_name, local=True) for file_name in files]
