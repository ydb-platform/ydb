import codecs
import os
import pytest
import re

import yatest.common

from yql_utils import replace_vals, yql_binary_path, is_xfail, get_param, \
    get_gateway_cfg_suffix, normalize_result

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

    (res, tables_res) = run_file('hybrid', suite, case, cfg, config, yql_http_file_server, DQRUN_PATH, extra_args=["--emulate-yt", "--no-force-dq"])

    to_canonize = []

    if what == 'Results':

        program_sql = os.path.join(DATA_PATH, suite, '%s.sql' % case)
        with codecs.open(program_sql, encoding='utf-8') as program_file_descr:
            sql_query = program_file_descr.read()

        # yqlrun run
        yqlrun_res, yqlrun_tables_res = run_file_no_cache('yt', suite, case, cfg, config, yql_http_file_server)
        hybrid_result_name = 'HYBRIDFILE'
        yqlrun_result_name = 'YQLRUN'

        sort = not 'order' in sql_query.lower()
        hybrid_res_yson = normalize_result(res.results, sort)
        yqlrun_res_yson = normalize_result(yqlrun_res.results, sort)

        # Compare results
        assert hybrid_res_yson == yqlrun_res_yson, 'RESULTS_DIFFER\n' \
            '%(hybrid_result_name)s result:\n %(hybrid_res_yson)s\n\n' \
            '%(yqlrun_result_name)s result:\n %(yqlrun_res_yson)s\n' % locals()
        return
    
    if what == 'Plan':
        to_canonize = [yatest.common.canonical_file(res.plan_file)]

    if what == 'Debug':
        with open(res.opt_file + "_patched", 'w') as f:
            f.write(re.sub(r"""("?_logical_id"?) '\d+""", r"""\1 '0""", res.opt).encode('utf-8'))
        to_canonize = [yatest.common.canonical_file(res.opt_file + "_patched", diff_tool=ASTDIFF_PATH)]

    return to_canonize
