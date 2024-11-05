import codecs
import os
import pytest
import re
import yql_utils

import yatest.common
from yql_utils import execute, get_tables, get_files, get_http_files, \
    KSV_ATTR, yql_binary_path, is_xfail, is_canonize_peephole, is_peephole_use_blocks, is_canonize_lineage, \
    is_skip_forceblocks, get_param, normalize_source_code_path, replace_vals, get_gateway_cfg_suffix, \
    do_custom_query_check, stable_result_file, stable_table_file, is_with_final_result_issues
from yqlrun import YQLRun

from utils import get_config, get_parameters_json, DATA_PATH
from file_common import run_file, run_file_no_cache, get_gateways_config

ASTDIFF_PATH = yql_binary_path('ydb/library/yql/tools/astdiff/astdiff')


def run_test(suite, case, cfg, tmpdir, what, yql_http_file_server):
    if get_param('SQL_FLAGS'):
        if what == 'Debug' or what == 'Plan' or what == 'Peephole' or what == 'Lineage':
            pytest.skip('SKIP')
    if get_gateway_cfg_suffix() != '' and what != 'Results':
        pytest.skip('non-trivial gateways.conf')

    config = get_config(suite, case, cfg)

    xfail = is_xfail(config)

    program_sql = os.path.join(DATA_PATH, suite, '%s.sql' % case)
    with codecs.open(program_sql, encoding='utf-8') as program_file_descr:
        sql_query = program_file_descr.read()

    if what == 'Peephole':
        if xfail:
            pytest.skip('xfail is not supported in this mode')
        canonize_peephole = is_canonize_peephole(config)
        if not canonize_peephole:
            canonize_peephole = re.search(r"canonize peephole", sql_query)
            if not canonize_peephole:
                pytest.skip('no peephole canonization requested')

        force_blocks = is_peephole_use_blocks(config)
        (res, tables_res) = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, force_blocks=force_blocks, extra_args=['--peephole'])
        return [yatest.common.canonical_file(res.opt_file, diff_tool=ASTDIFF_PATH)]

    if what == 'Lineage':
        if xfail:
            pytest.skip('xfail is not supported in this mode')
        canonize_lineage = is_canonize_lineage(config)
        if not canonize_lineage:
            pytest.skip('no lineage canonization requested')

        (res, tables_res) = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, extra_args=['--lineage'])
        return [yatest.common.canonical_file(res.results_file)]

    extra_final_args = []
    if is_with_final_result_issues(config):
        extra_final_args += ['--with-final-issues']
    (res, tables_res) = run_file('yt', suite, case, cfg, config, yql_http_file_server, extra_args=extra_final_args)

    to_canonize = []

    if what == 'Results':
        if not xfail:
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
        if res.std_err:
            to_canonize.append(normalize_source_code_path(res.std_err))

    if what == 'Plan' and not xfail:
        to_canonize = [yatest.common.canonical_file(res.plan_file)]

    if what == 'Debug' and not xfail:
        to_canonize = [yatest.common.canonical_file(res.opt_file, diff_tool=ASTDIFF_PATH)]

    if what == 'RunOnOpt':
        if xfail:
            pytest.skip('xfail is not supported in this mode')

        in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR)
        files = get_files(suite, config, DATA_PATH)
        http_files = get_http_files(suite, config, DATA_PATH)
        http_files_urls = yql_http_file_server.register_files({}, http_files)
        parameters = get_parameters_json(suite, config)

        yqlrun = YQLRun(
            prov='yt',
            keep_temp=False,
            gateway_config=get_gateways_config(http_files, yql_http_file_server),
            udfs_dir=yql_binary_path('ydb/library/yql/tests/common/test_framework/udfs_deps')
        )

        opt_res, opt_tables_res = execute(
            yqlrun,
            program=res.opt,
            input_tables=in_tables,
            output_tables=out_tables,
            files=files,
            urls=http_files_urls,
            check_error=True,
            verbose=True,
            parameters=parameters)

        if os.path.exists(res.results_file):
            assert res.results == opt_res.results
        for table in tables_res:
            if os.path.exists(tables_res[table].file):
                assert tables_res[table].content == opt_tables_res[table].content

        check_plan = True
        check_ast = False  # Temporary disable
        if re.search(r"ignore runonopt ast diff", sql_query):
            check_ast = False
        if re.search(r"ignore runonopt plan diff", sql_query):
            check_plan = False

        if check_plan:
            assert res.plan == opt_res.plan
        if check_ast:
            yatest.common.process.execute([ASTDIFF_PATH, res.opt_file, opt_res.opt_file], check_exit_code=True)

        return None

    if what == 'ForceBlocks':
        if xfail:
            pytest.skip('xfail is not supported in this mode')

        skip_forceblocks = is_skip_forceblocks(config) or re.search(r"skip force_blocks", sql_query)
        if skip_forceblocks:
            pytest.skip('no force_blocks test requested')

        blocks_res, blocks_tables_res = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server, force_blocks=True)

        if do_custom_query_check(blocks_res, sql_query):
            return None

        if os.path.exists(res.results_file):
            assert os.path.exists(blocks_res.results_file)
            scalar_res = stable_result_file(res)
            block_res = stable_result_file(blocks_res)
            assert scalar_res == block_res, 'RESULTS_DIFFER\nBlocks result:\n %s\n\nScalar result:\n %s\n' % (block_res, scalar_res)

        for table in tables_res:
            if os.path.exists(tables_res[table].file):
                assert os.path.exists(blocks_tables_res[table].file)
                scalar_content = stable_table_file(tables_res[table])
                block_content = stable_table_file(blocks_tables_res[table])
                assert scalar_content == block_content, \
                       'RESULTS_DIFFER FOR TABLE %s\nBlocks result:\n %s\n\nScalar result:\n %s\n' % \
                       (table, block_content, scalar_content)

        return None

    return to_canonize
