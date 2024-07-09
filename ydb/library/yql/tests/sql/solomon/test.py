import pytest
import os
import re
import codecs
import yatest.common

from google.protobuf import text_format
import ydb.library.yql.providers.common.proto.gateways_config_pb2 as gateways_config_pb2
from yqlrun import YQLRun

from yql_utils import (
    get_files,
    get_supported_providers,
    is_xfail,
    log,
    normalize_source_code_path,
    yql_binary_path)

from utils import (
    get_config,
    pytest_generate_tests_for_run)

ASTDIFF_PATH = yql_binary_path('ydb/library/yql/tools/astdiff/astdiff')
DQRUN_PATH = yql_binary_path('ydb/library/yql/tools/dqrun/dqrun')
DATA_PATH = yatest.common.source_path('ydb/library/yql/tests/sql/suites')


def read_file(path):
    with open(path, "r") as f:
        return f.read()


def sanitize_issues(s):
    # 2022-08-13T16:11:21Z -> ISOTIME
    # 2022-08-13T16:11:21.549879Z -> ISOTIME
    s = re.sub(r"2\d{3}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d+)?Z", "ISOTIME", s)
    # library/cpp/json/json_reader.cpp:420 -> library/cpp/json/json_reader.cpp:xxx
    return re.sub(r"cpp:\d+", "cpp:xxx", s)


def pytest_generate_tests(metafunc):
    return pytest_generate_tests_for_run(metafunc, suites=['solomon'], data_path=DATA_PATH)


def compose_gateways_config(solomon_endpoint):
    config_message = gateways_config_pb2.TGatewaysConfig()
    solomon_cluster = config_message.Solomon.ClusterMapping.add()
    solomon_cluster.Name = "local_solomon"
    solomon_cluster.Cluster = solomon_endpoint

    return text_format.MessageToString(config_message)


def test(suite, case, cfg, solomon):
    config = get_config(suite, case, cfg, data_path=DATA_PATH)

    prov = 'solomon'

    log(get_supported_providers(config))

    if prov not in get_supported_providers(config):
        pytest.skip('%s provider is not supported here' % prov.upper())

    log('===' + suite)
    log('===' + case + '-' + cfg)

    xfail = is_xfail(config)
    program_sql = os.path.join(DATA_PATH, suite, '%s.sql' % case)
    sql_query = codecs.open(program_sql, encoding='utf-8').read()

    files = get_files(suite, config, DATA_PATH)

    yqlrun = YQLRun(prov=prov, gateway_config=compose_gateways_config(solomon.endpoint), binary=DQRUN_PATH,
                    extra_args=["--emulate-yt"])
    yqlrun_res = yqlrun.yql_exec(
        program=sql_query,
        run_sql=True,
        files=files,
        check_error=not xfail,
        scan_udfs=False
    )

    if xfail:
        assert yqlrun_res.execution_result.exit_code != 0
        return [normalize_source_code_path(sanitize_issues(yqlrun_res.std_err))]

    return [yatest.common.canonical_file(yqlrun_res.results_file, local=True),
            yatest.common.canonical_file(yqlrun_res.opt_file, local=True, diff_tool=ASTDIFF_PATH),
            yatest.common.canonical_file(yqlrun_res.plan_file, local=True)]
