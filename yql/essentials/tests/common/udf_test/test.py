import os

import yql_utils

from udf_test_common import (
    DATA_PATH,
    discover_cases,
    make_test,
    facade_runner,
    canonize_results,
)

CFG_DIR = os.getenv('YQL_CONFIG_DIR') or 'yql/essentials/cfg/udf_test'
RUNNER_FACTORY = facade_runner(prov='yt', cfg_dir=CFG_DIR)


def pytest_generate_tests(metafunc):
    metafunc.parametrize(['case'], [(case,) for case in discover_cases()])


def test(case):
    spec = make_test(case)

    program_text = '\n'.join(['use plato;'] + spec.program)
    in_tables = yql_utils.get_input_tables(None, spec.cfg, DATA_PATH, def_attr=yql_utils.KSV_ATTR)

    res = RUNNER_FACTORY(spec.langver).yql_exec(
        program=program_text,
        run_sql=True,
        tables=in_tables,
        files=spec.files,
        check_error=not spec.xfail,
        extra_env=spec.extra_env,
        require_udf_resolver=True,
        scan_udfs=spec.scan_udfs,
    )

    if spec.xfail:
        assert res.execution_result.exit_code != 0

    return canonize_results(case, res, spec.xfail, spec.canonize_ast, spec.diff_tool)
