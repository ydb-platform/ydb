import codecs
import os
import pytest
import re
import yql_utils
import cyson

import yatest.common
import ydb.library.yql.providers.common.proto.gateways_config_pb2 as gateways_config_pb2

from google.protobuf import text_format
from yql_utils import execute_sql, get_supported_providers, get_tables, get_files, get_http_files, \
    get_pragmas, log, KSV_ATTR, is_xfail, get_param, YQLExecResult, yql_binary_path
from yqlrun import YQLRun

from utils import get_config, get_parameters_json, DATA_PATH, replace_vars


def get_gateways_config(http_files, yql_http_file_server, force_blocks=False, is_hybrid=False):
    config = None

    if http_files or force_blocks or is_hybrid:
        config_message = gateways_config_pb2.TGatewaysConfig()
        if http_files:
            schema = config_message.Fs.CustomSchemes.add()
            schema.Pattern = 'http_test://(.*)'
            schema.TargetUrl = yql_http_file_server.compose_http_link('$1')
        if force_blocks:
            config_message.SqlCore.TranslationFlags.extend(['EmitAggApply'])
            flags = config_message.YqlCore.Flags.add()
            flags.Name = 'UseBlocks'
        if is_hybrid:
            activate_hybrid = config_message.Yt.DefaultSettings.add()
            activate_hybrid.Name = "HybridDqExecution"
            activate_hybrid.Value = "1"
            deactivate_dq = config_message.Dq.DefaultSettings.add()
            deactivate_dq.Name = "AnalyzeQuery"
            deactivate_dq.Value = "0"
        enabled_spilling_nodes = config_message.Dq.DefaultSettings.add()
        enabled_spilling_nodes.Name = "EnableSpillingNodes"
        enabled_spilling_nodes.Value = "All"

        config = text_format.MessageToString(config_message)

    return config


def is_hybrid(provider):
    return provider == 'hybrid'


def check_provider(provider, config):
    if provider not in get_supported_providers(config):
        pytest.skip('%s provider is not supported here' % provider)


def get_sql_query(provider, suite, case, config):
    pragmas = get_pragmas(config)

    if get_param('TARGET_PLATFORM'):
        if "yson" in case or "regexp" in case or "match" in case:
            pytest.skip('yson/match/regexp is not supported on non-default target platform')

    if get_param('TARGET_PLATFORM') and is_xfail(config):
        pytest.skip('xfail is not supported on non-default target platform')

    program_sql = os.path.join(DATA_PATH, suite, '%s.sql' % case)

    with codecs.open(program_sql, encoding='utf-8') as program_file_descr:
        sql_query = program_file_descr.read()
        if get_param('TARGET_PLATFORM'):
            if "Yson::" in sql_query:
                pytest.skip('yson udf is not supported on non-default target platform')
        if (provider + 'file can not' in sql_query) or (is_hybrid(provider) and ('ytfile can not' in sql_query)):
            pytest.skip(provider + ' can not execute this')

    pragmas.append(sql_query)
    sql_query = ';\n'.join(pragmas)
    if 'Python' in sql_query or 'Javascript' in sql_query:
        pytest.skip('ScriptUdf')

    # assert 'UseBlocks' not in sql_query, 'UseBlocks should not be used directly, only via ForceBlocks'
    
    return sql_query

def run_file_no_cache(provider, suite, case, cfg, config, yql_http_file_server, yqlrun_binary=None, extra_args=[], force_blocks=False):
    check_provider(provider, config)

    sql_query = get_sql_query(provider, suite, case, config)
    sql_query = replace_vars(sql_query, "yqlrun_var")

    xfail = is_xfail(config)

    in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR)
    files = get_files(suite, config, DATA_PATH)
    http_files = get_http_files(suite, config, DATA_PATH)
    http_files_urls = yql_http_file_server.register_files({}, http_files)

    for table in in_tables:
        if cyson.loads(table.attr).get("type") == "document":
            content = table.content
        else:
            content = table.attr
        if 'Python' in content or 'Javascript' in content:
            pytest.skip('ScriptUdf')

    parameters = get_parameters_json(suite, config)

    yqlrun = YQLRun(
        prov=provider,
        keep_temp=not re.search(r"yt\.ReleaseTempData", sql_query),
        binary=yqlrun_binary,
        gateway_config=get_gateways_config(http_files, yql_http_file_server, force_blocks=force_blocks, is_hybrid=is_hybrid(provider)),
        extra_args=extra_args,
        udfs_dir=yql_binary_path('ydb/library/yql/tests/common/test_framework/udfs_deps')
    )

    res, tables_res = execute_sql(
        yqlrun,
        program=sql_query,
        input_tables=in_tables,
        output_tables=out_tables,
        files=files,
        urls=http_files_urls,
        check_error=not xfail,
        verbose=True,
        parameters=parameters)

    fixed_stderr = res.std_err
    if xfail:
        assert res.execution_result.exit_code != 0
        custom_error = re.search(r"/\* custom error:(.*)\*/", sql_query)
        if custom_error:
            err_string = custom_error.group(1)
            assert res.std_err.find(err_string) != -1
            fixed_stderr = None

    fixed_result = YQLExecResult(res.std_out,
                                 fixed_stderr,
                                 res.results,
                                 res.results_file,
                                 res.opt,
                                 res.opt_file,
                                 res.plan,
                                 res.plan_file,
                                 res.program,
                                 res.execution_result,
                                 None)

    return fixed_result, tables_res


def run_file(provider, suite, case, cfg, config, yql_http_file_server, yqlrun_binary=None, extra_args=[], force_blocks=False):
    if (suite, case, cfg) not in run_file.cache:
        run_file.cache[(suite, case, cfg)] = run_file_no_cache(provider, suite, case, cfg, config, yql_http_file_server, yqlrun_binary, extra_args, force_blocks=force_blocks)

    return run_file.cache[(suite, case, cfg)]


run_file.cache = {}
