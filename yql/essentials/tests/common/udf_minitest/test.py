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


def udf_bridge_path():
    # Resolved lazily (not at module scope like MINIRUN_PATH) since only test
    # projects that opt in via 'udf_bridge' in their .cfg need the udf_bridge
    # binary at all -- most YQL_UDF_MINITEST projects don't DEPENDS() on it.
    return yql_utils.yql_binary_path('yql/essentials/tools/udf_bridge/udf_bridge')


def pytest_generate_tests(metafunc):
    params = []
    for case in discover_cases():
        for flavour in ('Results', 'LLVM', 'Blocks', 'Peephole'):
            params.append((case, flavour))

    metafunc.parametrize(['case', 'mode'], params)


def cross_validate(expected_runner, actual_runner, expected_name, actual_name, exec_args):
    expected_result = expected_runner.yql_exec(check_error=True, **exec_args)
    actual_result = actual_runner.yql_exec(check_error=True, **exec_args)

    assert os.path.exists(actual_result.results_file)
    expected_yson = yql_utils.normalize_result(yql_utils.stable_result_file(expected_result), False)
    actual_yson = yql_utils.normalize_result(yql_utils.stable_result_file(actual_result), False)
    assert actual_yson == expected_yson, 'RESULTS_DIFFER\n%s:\n %s\n\n%s:\n %s\n' % (
        actual_name,
        actual_yson,
        expected_name,
        expected_yson,
    )


def make_canon_runner(runner_factory, spec, extra_args=None):
    canon_cfg = test_file_common.get_gateways_config({}, None, allow_llvm=False)
    if extra_args is None:
        extra_args = []
    return runner_factory(spec.langver, gateway_config=canon_cfg, extra_args=extra_args)


def test(case, mode):
    spec = make_test(case)

    if mode != 'Results' and spec.xfail:
        pytest.skip('xfail is not supported in this mode')
    if mode in ('Blocks', 'Peephole'):
        if not yql_utils.is_forceblocks(spec.cfg) or yql_utils.is_skip_forceblocks(spec.cfg):
            pytest.skip('no block execution requested')

    program_text = '\n'.join(spec.program)
    exec_args = dict(
        program=program_text,
        run_sql=True,
        files=spec.files,
        extra_env=spec.extra_env,
        require_udf_resolver=True,
        scan_udfs=spec.scan_udfs,
    )

    RUNNER_FACTORY = facade_runner(
        prov='pure',
        cfg_dir=CFG_DIR,
        binary=MINIRUN_PATH,
        secure_params=spec.secure_params,
        patch_cfg_file=spec.patch_cfg_file,
    )

    if mode == 'Results':
        extra_args = ['--udf-bridge', udf_bridge_path()] if spec.udf_bridge else []
        canon_runner = make_canon_runner(RUNNER_FACTORY, spec, extra_args)
        canon_results = canon_runner.yql_exec(check_error=not spec.xfail, **exec_args)

        if spec.xfail:
            assert canon_results.execution_result.exit_code != 0
        return canonize_results(case, canon_results, spec.xfail, spec.canonize_ast, spec.diff_tool)
    elif mode == 'LLVM':
        canon_runner = make_canon_runner(RUNNER_FACTORY, spec)
        llvm_runner = RUNNER_FACTORY(spec.langver)

        cross_validate(canon_runner, llvm_runner, 'Results', 'LLVM', exec_args)
    elif mode == 'Blocks':
        canon_runner = make_canon_runner(RUNNER_FACTORY, spec)

        blocks_cfg = test_file_common.get_gateways_config({}, None, force_blocks=True)
        blocks_runner = RUNNER_FACTORY(spec.langver, gateway_config=blocks_cfg)

        cross_validate(canon_runner, blocks_runner, 'Results', 'Blocks', exec_args)
    elif mode == 'Peephole':
        blocks_cfg = test_file_common.get_gateways_config({}, None, force_blocks=True)
        blocks_runner = RUNNER_FACTORY(spec.langver, gateway_config=blocks_cfg, extra_args=['--peephole'])
        blocks_peephole = blocks_runner.yql_exec(check_error=True, **exec_args)
        return canonize_opt(blocks_peephole)
