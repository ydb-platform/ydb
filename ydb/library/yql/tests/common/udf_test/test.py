import os
import os.path
import glob
import codecs
import shutil

import pytest

import yql_utils
from yqlrun import YQLRun

import yatest.common

project_path = yatest.common.context.project_path
SOURCE_PATH = yql_utils.yql_source_path((project_path + '/cases').replace('\\', '/'))
DATA_PATH = yatest.common.output_path('cases')
ASTDIFF_PATH = yql_utils.yql_binary_path(os.getenv('YQL_ASTDIFF_PATH') or 'ydb/library/yql/tools/astdiff/astdiff')


def pytest_generate_tests(metafunc):
    if os.path.exists(SOURCE_PATH):
        shutil.copytree(SOURCE_PATH, DATA_PATH)
        cases = sorted([os.path.basename(sql_query)[:-4] for sql_query in glob.glob(DATA_PATH + '/*.sql')])

    else:
        cases = []
    metafunc.parametrize(['case'], [(case, ) for case in cases])


def test(case):
    program_file = os.path.join(DATA_PATH, case + '.sql')

    with codecs.open(program_file, encoding='utf-8') as f:
        program = f.readlines()

    header = program[0]
    canonize_ast = False

    if header.startswith('--ignore'):
        pytest.skip(header)
    elif header.startswith('--sanitizer ignore') and yatest.common.context.sanitize is not None:
        pytest.skip(header)
    elif header.startswith('--sanitizer ignore address') and yatest.common.context.sanitize == 'address':
        pytest.skip(header)
    elif header.startswith('--sanitizer ignore memory') and yatest.common.context.sanitize == 'memory':
        pytest.skip(header)
    elif header.startswith('--sanitizer ignore thread') and yatest.common.context.sanitize == 'thread':
        pytest.skip(header)
    elif header.startswith('--sanitizer ignore undefined') and yatest.common.context.sanitize == 'undefined':
        pytest.skip(header)
    elif header.startswith('--canonize ast'):
        canonize_ast = True

    program = '\n'.join(['use plato;'] + program)

    cfg = yql_utils.get_program_cfg(None, case, DATA_PATH)
    files = {}
    diff_tool = None
    for item in cfg:
        if item[0] == 'file':
            files[item[1]] = item[2]
        if item[0] == 'diff_tool':
            diff_tool = item[1:]

    in_tables = yql_utils.get_input_tables(None, cfg, DATA_PATH, def_attr=yql_utils.KSV_ATTR)

    udfs_dir = yql_utils.get_udfs_path([
        yatest.common.build_path(os.path.join(yatest.common.context.project_path, ".."))
    ])

    xfail = yql_utils.is_xfail(cfg)
    if yql_utils.get_param('TARGET_PLATFORM') and xfail:
        pytest.skip('xfail is not supported on non-default target platform')

    extra_env = dict(os.environ)
    extra_env["YQL_UDF_RESOLVER"] = "1"
    extra_env["YQL_ARCADIA_BINARY_PATH"] = os.path.expandvars(yatest.common.build_path('.'))
    extra_env["YQL_ARCADIA_SOURCE_PATH"] = os.path.expandvars(yatest.common.source_path('.'))
    extra_env["Y_NO_AVX_IN_DOT_PRODUCT"] = "1"

    # this breaks tests using V0 syntax
    if "YA_TEST_RUNNER" in extra_env:
        del extra_env["YA_TEST_RUNNER"]

    yqlrun_res = YQLRun(udfs_dir=udfs_dir, prov='yt', use_sql2yql=False, cfg_dir=os.getenv('YQL_CONFIG_DIR') or 'ydb/library/yql/cfg/udf_test').yql_exec(
        program=program,
        run_sql=True,
        tables=in_tables,
        files=files,
        check_error=not xfail,
        extra_env=extra_env,
        require_udf_resolver=True,
        scan_udfs=False
    )

    if xfail:
        assert yqlrun_res.execution_result.exit_code != 0

    results_path = os.path.join(yql_utils.yql_output_path(), case + '.results.txt')
    with open(results_path, 'w') as f:
        f.write(yqlrun_res.results)

    to_canonize = [yqlrun_res.std_err] if xfail else [yatest.common.canonical_file(yqlrun_res.results_file, local=True, diff_tool=diff_tool)]

    if canonize_ast:
        to_canonize += [yatest.common.canonical_file(yqlrun_res.opt_file, local=True, diff_tool=ASTDIFF_PATH)]

    return to_canonize
