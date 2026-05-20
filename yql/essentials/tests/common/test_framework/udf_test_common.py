import codecs
import collections
import glob
import os
import shutil

import pytest

import yql_utils
import yqlrun

import yatest.common

DATA_PATH = yatest.common.output_path('cases')
ASTDIFF_PATH = yql_utils.yql_binary_path(os.getenv('YQL_ASTDIFF_PATH') or 'yql/essentials/tools/astdiff/astdiff')


def discover_cases():
    project_path = yatest.common.context.project_path
    source_path = yql_utils.yql_source_path((project_path + '/cases').replace('\\', '/'))
    if os.path.exists(source_path):
        shutil.copytree(source_path, DATA_PATH)
        return sorted([os.path.basename(f)[:-4] for f in glob.glob(os.path.join(DATA_PATH, '*.sql'))])
    return []


TSpec = collections.namedtuple(
    'TSpec', ['program', 'canonize_ast', 'cfg', 'xfail', 'langver', 'files', 'diff_tool', 'scan_udfs', 'extra_env']
)


def make_test(case):
    # 1. Read SQL program and process first-line directives (--ignore, --sanitizer ignore, --canonize ast).
    program_file = os.path.join(DATA_PATH, case + '.sql')
    with codecs.open(program_file, encoding='utf-8') as f:
        program = f.readlines()

    header = program[0]
    sanitizers = ['address', 'memory', 'thread', 'undefined']
    if header.startswith('--ignore'):
        pytest.skip(header)
    if header.startswith('--sanitizer ignore'):
        current = yatest.common.context.sanitize
        if current is not None:
            specific = [s for s in sanitizers if header.startswith('--sanitizer ignore ' + s)]
            if not specific or current == specific[0]:
                pytest.skip(header)
    canonize_ast = header.startswith('--canonize ast')

    # 2. Parse .cfg file for xfail, language version, and other settings.
    cfg = yql_utils.get_program_cfg(None, case, DATA_PATH)
    xfail = yql_utils.is_xfail(cfg)
    if yql_utils.get_param('TARGET_PLATFORM') and xfail:
        pytest.skip('xfail is not supported on non-default target platform')
    langver = yql_utils.get_langver(cfg) or "unknown"

    # 3. Extract per-test cfg directives: attached files, diff tool, UDF scan control.
    project_path = yatest.common.context.project_path
    files = {}
    diff_tool = None
    scan_udfs = os.getenv('YQL_UDF_NO_SCAN') != 'yes'
    # check for purecalc-only UDFs (TODO: replace with YQL_UDF_NO_SCAN macro)
    if project_path.startswith('robot/rthub/yql/udfs') or project_path.startswith('robot/zora'):
        scan_udfs = False
    for item in cfg:
        if item[0] == 'file':
            files[item[1]] = item[2]
        elif item[0] == 'diff_tool':
            diff_tool = item[1:]
        elif item[0] == 'scan_udfs':
            scan_udfs = True
        elif item[0] == 'disable_scan_udfs':
            scan_udfs = False

    # 4. Build the environment for the YQL runner process.
    extra_env = dict(os.environ)
    extra_env["YQL_UDF_RESOLVER"] = "1"
    extra_env["YQL_ARCADIA_BINARY_PATH"] = os.path.expandvars(yatest.common.build_path('.'))
    extra_env["YQL_ARCADIA_SOURCE_PATH"] = os.path.expandvars(yatest.common.source_path('.'))
    extra_env["Y_NO_AVX_IN_DOT_PRODUCT"] = "1"
    extra_env.update(yql_utils.get_envs(cfg))
    # this breaks tests using V0 syntax
    if "YA_TEST_RUNNER" in extra_env:
        del extra_env["YA_TEST_RUNNER"]

    return TSpec(
        program=program,
        canonize_ast=canonize_ast,
        cfg=cfg,
        xfail=xfail,
        langver=langver,
        files=files,
        diff_tool=diff_tool,
        scan_udfs=scan_udfs,
        extra_env=extra_env,
    )


def facade_runner(prov, cfg_dir, binary=None):
    """Returns a factory that creates YQLRun instances with the given fixed parameters."""

    def make(langver, gateway_config=None, extra_args=[]):
        project_path = yatest.common.context.project_path
        udfs_list = [yatest.common.build_path(os.path.join(project_path, ".."))]
        env_udfs_list = yql_utils.get_param("EXTRA_UDF_DIRS")
        if env_udfs_list:
            for udf_path in env_udfs_list.strip().split(":"):
                udfs_list.append(yatest.common.build_path(udf_path))
        udfs_dir = yql_utils.get_udfs_path(udfs_list)

        return yqlrun.YQLRun(
            udfs_dir=udfs_dir,
            prov=prov,
            binary=binary,
            use_sql2yql=False,
            cfg_dir=cfg_dir,
            gateway_config=gateway_config,
            extra_args=extra_args,
            langver=langver,
        )

    return make


def canonize_opt(res):
    return [yatest.common.canonical_file(res.opt_file, local=True, diff_tool=ASTDIFF_PATH)]


def canonize_results(case, res, xfail, canonize_ast, diff_tool):
    with open(os.path.join(yql_utils.yql_output_path(), case + '.results.txt'), 'w') as f:
        f.write(res.results)

    to_canonize = (
        [res.std_err] if xfail else [yatest.common.canonical_file(res.results_file, local=True, diff_tool=diff_tool)]
    )
    if canonize_ast:
        to_canonize += canonize_opt(res)
    return to_canonize
