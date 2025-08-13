import codecs
import os
import pytest
import re
import yql_utils

import yatest.common
from yql_utils import execute, get_tables, get_files, get_http_files, \
    KSV_ATTR, yql_binary_path, is_xfail, is_canonize_peephole, is_peephole_use_blocks, is_canonize_lineage, \
    is_skip_forceblocks, get_param, normalize_source_code_path, replace_vals, get_gateway_cfg_suffix, \
    do_custom_query_check, stable_result_file, stable_table_file, is_with_final_result_issues, \
    normalize_result, get_langver
from yqlrun import YQLRun

from test_utils import get_config, get_parameters_json, get_case_file
from test_file_common import run_file, run_file_no_cache, get_gateways_config, get_sql_query

DEFAULT_LANG_VER = '2025.01'
DATA_PATH = yatest.common.source_path('yql/essentials/tests/sql/suites')
ASTDIFF_PATH = yql_binary_path('yql/essentials/tools/astdiff/astdiff')
MINIRUN_PATH = yql_binary_path('yql/essentials/tools/minirun/minirun')


def mode_expander(lst):
    res = []
    for (suite, case, cfg) in lst:
        res.append((suite, case, cfg, 'Results'))
        res.append((suite, case, cfg, 'Debug'))
        res.append((suite, case, cfg, 'RunOnOpt'))
        res.append((suite, case, cfg, 'LLVM'))
        if suite == 'blocks':
            res.append((suite, case, cfg, 'Blocks'))
            res.append((suite, case, cfg, 'Peephole'))
    return res


def run_test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    if get_gateway_cfg_suffix() != '' and what not in ('Results','LLVM','Blocks'):
        pytest.skip('non-trivial gateways.conf')

    config = get_config(suite, case, cfg, data_path = DATA_PATH)

    langver = get_langver(config)
    if langver is None:
        langver = DEFAULT_LANG_VER

    xfail = is_xfail(config)
    if xfail and what != 'Results':
        pytest.skip('xfail is not supported in this mode')

    program_sql = get_case_file(DATA_PATH, suite, case, {'.sql', '.yql'})
    with codecs.open(program_sql, encoding='utf-8') as program_file_descr:
        sql_query = program_file_descr.read()

    extra_final_args = []
    if is_with_final_result_issues(config):
        extra_final_args += ['--with-final-issues']
    (res, tables_res) = run_file('pure', suite, case, cfg, config, yql_http_file_server, MINIRUN_PATH,
                                 extra_args=extra_final_args, allow_llvm=False, data_path=DATA_PATH,
                                 langver=langver)

    to_canonize = []
    assert xfail or os.path.exists(res.results_file)
    assert not tables_res

    if what == 'Peephole':
        canonize_peephole = is_canonize_peephole(config)
        if not canonize_peephole:
            canonize_peephole = re.search(r"canonize peephole", sql_query)
            if not canonize_peephole:
                pytest.skip('no peephole canonization requested')

        force_blocks = is_peephole_use_blocks(config)
        (res, tables_res) = run_file_no_cache('pure', suite, case, cfg, config, yql_http_file_server,
                                              force_blocks=force_blocks, extra_args=['--peephole'],
                                              data_path=DATA_PATH, yqlrun_binary=MINIRUN_PATH,
                                              langver=langver)
        return [yatest.common.canonical_file(res.opt_file, diff_tool=ASTDIFF_PATH)]

    if what == 'Results':
        if xfail:
            return None

        if do_custom_query_check(res, sql_query):
            return None

        stable_result_file(res)
        to_canonize.append(yatest.common.canonical_file(res.results_file))
        if res.std_err:
            to_canonize.append(normalize_source_code_path(res.std_err))

    if what == 'Debug':
        to_canonize = [yatest.common.canonical_file(res.opt_file, diff_tool=ASTDIFF_PATH)]

    if what == 'RunOnOpt' or what == 'LLVM' or what == 'Blocks':
        is_on_opt = (what == 'RunOnOpt')
        is_llvm = (what == 'LLVM')
        is_blocks = (what == 'Blocks')
        files = get_files(suite, config, DATA_PATH)
        http_files = get_http_files(suite, config, DATA_PATH)
        http_files_urls = yql_http_file_server.register_files({}, http_files)
        parameters = get_parameters_json(suite, config, DATA_PATH)
        query_sql = get_sql_query('pure', suite, case, config, DATA_PATH) if not is_on_opt else None

        yqlrun = YQLRun(
            prov='pure',
            keep_temp=False,
            gateway_config=get_gateways_config(http_files, yql_http_file_server, allow_llvm=is_llvm, force_blocks=is_blocks),
            udfs_dir=yql_binary_path('yql/essentials/tests/common/test_framework/udfs_deps'),
            binary=MINIRUN_PATH,
            langver=langver
        )

        opt_res, opt_tables_res = execute(
            yqlrun,
            program=res.opt if is_on_opt else query_sql,
            run_sql=not is_on_opt,
            files=files,
            urls=http_files_urls,
            check_error=True,
            verbose=True,
            parameters=parameters)

        assert os.path.exists(opt_res.results_file)
        assert not opt_tables_res

        base_res_yson = normalize_result(stable_result_file(res), False)
        opt_res_yson = normalize_result(stable_result_file(opt_res), False)

        # Compare results
        assert opt_res_yson == base_res_yson, 'RESULTS_DIFFER for mode {}\n'.format(what) + \
            'Result:\n %(opt_res_yson)s\n\n' \
            'Base result:\n %(base_res_yson)s\n' % locals()

        return None

    return to_canonize
