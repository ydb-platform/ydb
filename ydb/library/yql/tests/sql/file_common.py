import codecs
import os
import pytest
import re
import yql_utils
import yt.yson

import yatest.common
import ydb.library.yql.providers.common.proto.gateways_config_pb2 as gateways_config_pb2

from google.protobuf import text_format
from yql_utils import execute_sql, get_supported_providers, get_tables, get_files, get_http_files, \
    get_pragmas, log, KSV_ATTR, is_xfail, get_param, YQLExecResult
from yqlrun import YQLRun

from utils import get_config, get_parameters_json, DATA_PATH


def get_gateways_config(http_files, yql_http_file_server, force_blocks=False):
    config = None

    if http_files or force_blocks:
        config_message = gateways_config_pb2.TGatewaysConfig()
        if http_files:
            schema = config_message.Fs.CustomSchemes.add()
            schema.Pattern = 'http_test://(.*)'
            schema.TargetUrl = yql_http_file_server.compose_http_link('$1')
        if force_blocks:
            config_message.SqlCore.TranslationFlags.extend(['EmitAggApply'])
            flags = config_message.YqlCore.Flags.add()
            flags.Name = 'UseBlocks'
        config = text_format.MessageToString(config_message)

    return config


def run_file_no_cache(provider, suite, case, cfg, config, yql_http_file_server, yqlrun_binary=None, extra_args=[], force_blocks=False):
    if provider not in get_supported_providers(config):
        pytest.skip('%s provider is not supported here' % provider)

    pragmas = get_pragmas(config)

    if get_param('TARGET_PLATFORM'):
        if "yson" in case or "regexp" in case or "match" in case:
            pytest.skip('yson/match/regexp is not supported on non-default target platform')

    xfail = is_xfail(config)
    if get_param('TARGET_PLATFORM') and xfail:
        pytest.skip('xfail is not supported on non-default target platform')

    in_tables, out_tables = get_tables(suite, config, DATA_PATH, def_attr=KSV_ATTR)
    files = get_files(suite, config, DATA_PATH)
    http_files = get_http_files(suite, config, DATA_PATH)
    http_files_urls = yql_http_file_server.register_files({}, http_files)

    program_sql = os.path.join(DATA_PATH, suite, '%s.sql' % case)

    with codecs.open(program_sql, encoding='utf-8') as program_file_descr:
        sql_query = program_file_descr.read()
        if get_param('TARGET_PLATFORM'):
            if "Yson::" in sql_query:
                pytest.skip('yson udf is not supported on non-default target platform')
        if provider + 'file can not' in sql_query:
            pytest.skip(provider + ' can not execute this')
            assert False

    pragmas.append(sql_query)
    sql_query = ';\n'.join(pragmas)

    parameters = get_parameters_json(suite, config)

    yqlrun = YQLRun(
        prov=provider,
        keep_temp=not re.search(r"yt\.ReleaseTempData", sql_query),
        binary=yqlrun_binary,
        gateway_config=get_gateways_config(http_files, yql_http_file_server, force_blocks=force_blocks),
        extra_args=extra_args,
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
