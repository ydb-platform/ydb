import os

import pytest

import yql_utils
import test_file_common

from udf_test_common import (
    discover_cases,
    make_test,
    facade_runner,
    canonize_opt,
    canonize_results,
)

MINIRUN_PATH = yql_utils.yql_binary_path(os.getenv('YQL_MINIRUN_PATH') or 'yql/essentials/tools/minirun/minirun')
CFG_DIR = os.getenv('YQL_CONFIG_DIR') or 'yql/essentials/cfg/tests'
RUNNER_FACTORY = facade_runner(prov='pure', cfg_dir=CFG_DIR, binary=MINIRUN_PATH)


def pytest_generate_tests(metafunc):
    params = []
    for case in discover_cases():
        for flavour in ('Results', 'Blocks', 'Peephole'):
            params.append((case, flavour))

    metafunc.parametrize(['case', 'mode'], params)


def test(case, mode):
    spec = make_test(case)

    if mode != 'Results':
        if not yql_utils.is_forceblocks(spec.cfg) or yql_utils.is_skip_forceblocks(spec.cfg):
            pytest.skip('no block execution requested')
        if spec.xfail:
            pytest.skip('xfail is not supported in this mode')

    program_text = '\n'.join(spec.program)
    exec_args = dict(
        program=program_text,
        run_sql=True,
        files=spec.files,
        extra_env=spec.extra_env,
        require_udf_resolver=True,
        scan_udfs=spec.scan_udfs,
    )

    if mode == 'Results':
        runner = RUNNER_FACTORY(spec.langver)
        scalar_res = runner.yql_exec(check_error=not spec.xfail, **exec_args)

        if spec.xfail:
            assert scalar_res.execution_result.exit_code != 0
        return canonize_results(case, scalar_res, spec.xfail, spec.canonize_ast, spec.diff_tool)
    elif mode == 'Blocks':
        blocks_cfg = test_file_common.get_gateways_config({}, None, force_blocks=True)

        scalar_runner = RUNNER_FACTORY(spec.langver)
        blocks_runner = RUNNER_FACTORY(spec.langver, gateway_config=blocks_cfg)

        scalar_res = scalar_runner.yql_exec(check_error=True, **exec_args)
        blocks_res = blocks_runner.yql_exec(check_error=True, **exec_args)

        assert os.path.exists(blocks_res.results_file)
        scalar_yson = yql_utils.normalize_result(yql_utils.stable_result_file(scalar_res), False)
        blocks_yson = yql_utils.normalize_result(yql_utils.stable_result_file(blocks_res), False)
        assert blocks_yson == scalar_yson, 'RESULTS_DIFFER\nBlocks:\n %s\n\nScalar:\n %s\n' % (blocks_yson, scalar_yson)
    elif mode == 'Peephole':
        blocks_cfg = test_file_common.get_gateways_config({}, None, force_blocks=True)
        blocks_runner = RUNNER_FACTORY(spec.langver, gateway_config=blocks_cfg, extra_args=['--peephole'])
        blocks_peephole = blocks_runner.yql_exec(check_error=True, **exec_args)
        return canonize_opt(blocks_peephole)
