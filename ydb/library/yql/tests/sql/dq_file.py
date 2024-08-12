import codecs
import os
import pytest
import re

import yatest.common
from yql_utils import get_supported_providers, yql_binary_path, is_xfail, is_skip_forceblocks, get_param, \
    normalize_source_code_path, dump_table_yson, get_gateway_cfg_suffix, do_custom_query_check, normalize_result, \
    stable_result_file, stable_table_file, is_with_final_result_issues

from utils import get_config, DATA_PATH
from file_common import run_file, run_file_no_cache

ASTDIFF_PATH = yql_binary_path('ydb/library/yql/tools/astdiff/astdiff')
DQRUN_PATH = yql_binary_path('ydb/library/yql/tools/dqrun/dqrun')


def run_test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    if get_param('SQL_FLAGS'):
        if what == 'Debug' or what == 'Plan':
            pytest.skip('SKIP')
    if get_gateway_cfg_suffix() != '' and what != 'Results':
        pytest.skip('non-trivial gateways.conf')

    config = get_config(suite, case, cfg)

    xfail = is_xfail(config)
    if xfail and what != 'Results':
        pytest.skip('SKIP')

    force_blocks = what == 'ForceBlocks'
    if force_blocks and is_skip_forceblocks(config):
        pytest.skip('skip force blocks requested')

    extra_args=["--emulate-yt"]
    if what == 'Analyze':
        extra_args += ["--analyze-query", "--optimize"]
    if is_with_final_result_issues(config):
        extra_args += ["--with-final-issues"]

    if what == 'Analyze':
        (res, tables_res) = run_file_no_cache('dq', suite, case, cfg, config, yql_http_file_server, DQRUN_PATH, extra_args=extra_args)
    else:
        (res, tables_res) = run_file('dq', suite, case, cfg, config, yql_http_file_server, DQRUN_PATH, extra_args=extra_args)

    to_canonize = []

    if what == 'Results' or force_blocks:
        if not xfail:
            program_sql = os.path.join(DATA_PATH, suite, '%s.sql' % case)
            with codecs.open(program_sql, encoding='utf-8') as program_file_descr:
                sql_query = program_file_descr.read()

            if force_blocks and re.search(r"skip force_blocks", sql_query):
                pytest.skip('skip force blocks requested')

            if 'ytfile can not' in sql_query or 'yt' not in get_supported_providers(config):
                if force_blocks:
                    pytest.skip('ForceBlocks skipped - provider disabled')
                if do_custom_query_check(res, sql_query):
                    return None
                if os.path.exists(res.results_file):
                    stable_result_file(res)
                    to_canonize.append(yatest.common.canonical_file(res.results_file))
                for table in tables_res:
                    if os.path.exists(tables_res[table].file):
                        stable_table_file(tables_res[table])
                        to_canonize.append(yatest.common.canonical_file(tables_res[table].file))
                        to_canonize.append(yatest.common.canonical_file(tables_res[table].yqlrun_file + ".attr"))
            else:
                # yqlrun run
                if force_blocks:
                    yqlrun_res, yqlrun_tables_res = run_file_no_cache('dq', suite, case, cfg, config, yql_http_file_server, DQRUN_PATH, \
                                                                      extra_args=["--emulate-yt"], force_blocks=True)
                    dq_result_name = 'Scalar'
                    yqlrun_result_name = 'Block'
                else:
                    yqlrun_res, yqlrun_tables_res = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server)
                    dq_result_name = 'DQFILE'
                    yqlrun_result_name = 'YQLRUN'

                if do_custom_query_check(yqlrun_res, sql_query):
                    return None
                
                if os.path.exists(yqlrun_res.results_file):
                    assert os.path.exists(res.results_file)
                    dq_res_yson = normalize_result(stable_result_file(res), False)
                    yqlrun_res_yson = normalize_result(stable_result_file(yqlrun_res), False)

                    # Compare results
                    assert dq_res_yson == yqlrun_res_yson, 'RESULTS_DIFFER\n' \
                        '%(dq_result_name)s result:\n %(dq_res_yson)s\n\n' \
                        '%(yqlrun_result_name)s result:\n %(yqlrun_res_yson)s\n' % locals()

                for table in yqlrun_tables_res:
                    assert table in tables_res

                    if os.path.exists(yqlrun_tables_res[table].file):
                        assert os.path.exists(tables_res[table].file)
                        yqlrun_table_yson = dump_table_yson(stable_table_file(yqlrun_tables_res[table]), False)
                        dq_table_yson = dump_table_yson(stable_table_file(tables_res[table]), False)

                        assert yqlrun_table_yson == dq_table_yson, \
                            'OUT_TABLE_DIFFER: %(table)s\n' \
                            '%(dq_result_name)s table:\n %(dq_table_yson)s\n\n' \
                            '%(yqlrun_result_name)s table:\n %(yqlrun_table_yson)s\n' % locals()

                if force_blocks:
                    return None

        if res.std_err:
            to_canonize.append(normalize_source_code_path(res.std_err))

    if what == 'Plan':
        to_canonize = [yatest.common.canonical_file(res.plan_file)]

    if what == 'Debug':
        with open(res.opt_file + "_patched", 'w') as f:
            f.write(re.sub(r"""("?_logical_id"?) '\d+""", r"""\1 '0""", res.opt).encode('utf-8'))
        to_canonize = [yatest.common.canonical_file(res.opt_file + "_patched", diff_tool=ASTDIFF_PATH)]

    if what == 'Analyze':
        to_canonize = [yatest.common.canonical_file(res.plan_file)]
        if res.std_err:
            to_canonize.append(normalize_source_code_path(res.std_err))

    return to_canonize
