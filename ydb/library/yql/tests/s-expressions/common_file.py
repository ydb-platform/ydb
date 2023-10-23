import codecs
import cyson
import os
import re
import pytest
import yatest.common
import ydb.library.yql.providers.common.proto.gateways_config_pb2 as gateways_config_pb2

from common_test_tools import ASTDIFF_PATH, DATA_PATH, get_program_cfg, get_supported_providers, cut_unstable, get_program
from yql_utils import is_xfail, is_skip_forceblocks, get_param, get_tables, get_files, KSV_ATTR, execute, yql_binary_path, \
                      normalize_source_code_path, get_gateway_cfg_suffix
from yqlrun import YQLRun
from google.protobuf import text_format


def get_block_gateways_config():
    config_message = gateways_config_pb2.TGatewaysConfig()
    config_message.SqlCore.TranslationFlags.extend(['EmitAggApply'])
    flags = config_message.YqlCore.Flags.add()
    flags.Name = 'UseBlocks'
    config = text_format.MessageToString(config_message)
    return config


def yqlrun_yt_results(provider, prepare, suite, case, config):
    if (suite, case) not in yqlrun_yt_results.cache:
        if provider not in get_supported_providers(config):
            pytest.skip('%s provider is not supported here' % provider)

        prepare = prepare or (lambda s: s)
        program = prepare(get_program(suite, case))
        if 'ScriptUdf' in program:
            pytest.skip('ScriptUdf')

        xfail = is_xfail(config)

        in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR)
        files = get_files(suite, config, DATA_PATH)

        for table in in_tables:
            if cyson.loads(table.attr).get("type") == "document":
                content = table.content
            else:
                content = table.attr
            if 'Python' in content or 'Javascript' in content:
                pytest.skip('ScriptUdf')

        yqlrun = YQLRun(prov=provider, keep_temp=True, udfs_dir=yql_binary_path('ydb/library/yql/tests/common/test_framework/udfs_deps'))
        res, tables_res = execute(
            yqlrun,
            program=program,
            input_tables=in_tables,
            output_tables=out_tables,
            files=files,
            check_error=not xfail,
            verbose=True)
        tmp_tables = [os.path.join(yqlrun.res_dir, f) for f in sorted(filter(lambda f: f.endswith(".tmp") or f.endswith(".attr"), os.listdir(yqlrun.res_dir)))]

        if xfail:
            assert res.execution_result.exit_code != 0

        yqlrun_yt_results.cache[(suite, case)] = res, tables_res, tmp_tables

    return yqlrun_yt_results.cache[(suite, case)]


yqlrun_yt_results.cache = {}


def run_test(provider, prepare, suite, case, tmpdir, what):
    if get_param('TARGET_PLATFORM'):
        if "ArcPython" in case:
            pytest.skip('ArcPython is not supported on non-default target platform')

    config = get_program_cfg(suite, case)

    (res, tables_res, tmp_tables) = yqlrun_yt_results(provider, prepare, suite, case, config)
    xfail = is_xfail(config)
    to_canonize = []
    if get_gateway_cfg_suffix() != '' and what != 'Results':
        pytest.skip('non-default gateways.conf')
    if what == 'Plan' and not xfail:
        to_canonize = [yatest.common.canonical_file(res.plan_file)]

    if what == 'Results':
        if not xfail:
            if os.path.exists(res.results_file):
                stable_res = cut_unstable(res.results)
                if stable_res != res.results:
                    with open(res.results_file, 'w') as f:
                        f.write(stable_res)
                to_canonize.append(yatest.common.canonical_file(res.results_file))
            for table in tables_res:
                if os.path.exists(tables_res[table].file):
                    to_canonize.append(yatest.common.canonical_file(tables_res[table].file))
        if res.std_err:
            to_canonize.append(normalize_source_code_path(res.std_err))

    if what == 'Debug' and not xfail:
        to_canonize = [yatest.common.canonical_file(res.opt_file, diff_tool=ASTDIFF_PATH)]
        for file in tmp_tables:
            to_canonize.append(yatest.common.canonical_file(file))

    if what == 'RunOnOpt':
        if xfail:
            pytest.skip('xfail is not supported in this mode')

        in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR)
        files = get_files(suite, config, DATA_PATH)

        opt_res, opt_tables_res = execute(
            YQLRun(prov=provider, keep_temp=False, udfs_dir=yql_binary_path('ydb/library/yql/tests/common/test_framework/udfs_deps')),
            program=res.opt,
            input_tables=in_tables,
            output_tables=out_tables,
            files=files,
            check_error=True,
            verbose=True)

        check_result = True
        check_plan = True
        check_ast = False  # Temporary disable
        with open(os.path.join(DATA_PATH, suite, case + '.yql')) as prog:
            program_content = prog.read()
            if "# canonize yson here" in program_content:
                check_result = False
            if "# ignore runonopt ast diff" in program_content:
                check_ast = False
            if "# ignore runonopt plan diff" in program_content:
                check_plan = False

        if check_result:
            if os.path.exists(res.results_file):
                assert res.results == opt_res.results
            for table in tables_res:
                if os.path.exists(tables_res[table].file):
                    assert tables_res[table].content == opt_tables_res[table].content

        if check_plan:
            assert res.plan == opt_res.plan

        if check_ast:
            yatest.common.process.execute([ASTDIFF_PATH, res.opt_file, opt_res.opt_file], check_exit_code=True)

        return None

    if what == 'ForceBlocks':
        if xfail:
            pytest.skip('xfail is not supported in this mode')
        if is_skip_forceblocks(config):
            pytest.skip('no force_blocks test requested')

        prepare = prepare or (lambda s: s)
        program = prepare(get_program(suite, case))

        if re.search(r"skip force_blocks", program):
            pytest.skip('no force_blocks test requested')

        in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR)
        files = get_files(suite, config, DATA_PATH)
        gateway_config = get_block_gateways_config()

        blocks_res, blocks_tables_res = execute(
            YQLRun(prov=provider, keep_temp=False, udfs_dir=yql_binary_path('ydb/library/yql/tests/common/test_framework/udfs_deps'), gateway_config=gateway_config),
            program=program,
            input_tables=in_tables,
            output_tables=out_tables,
            files=files,
            check_error=True,
            verbose=True)

        if os.path.exists(res.results_file):
            assert res.results == blocks_res.results, 'RESULTS_DIFFER\nBlocks result:\n %s\n\nScalar result:\n %s\n' % (blocks_res.results, res.results)
        for table in tables_res:
            if os.path.exists(tables_res[table].file):
                assert tables_res[table].content == blocks_tables_res[table].content, \
                       'RESULTS_DIFFER FOR TABLE %s\nBlocks result:\n %s\n\nScalar result:\n %s\n' % \
                       (table, blocks_tables_res[table].content, tables_res[table].content)

        return None

    return to_canonize
