import json
import os
import pytest
import re
import yatest.common

from yql_utils import get_param as yql_get_param
from google.protobuf import text_format
import ydb.library.yql.providers.common.proto.gateways_config_pb2 as gateways_config_pb2

DATA_PATH = yatest.common.source_path('ydb/library/yql/tests/sql/suites')
try:
    SQLRUN_PATH = yatest.common.binary_path('ydb/library/yql/tools/sql2yql/sql2yql')
except BaseException:
    SQLRUN_PATH = None

try:
    YQLRUN_PATH = yatest.common.binary_path('ydb/library/yql/tools/yqlrun/yqlrun')
except BaseException:
    YQLRUN_PATH = None

def get_sql_flags():
    gateway_config = gateways_config_pb2.TGatewaysConfig()

    with open(yatest.common.source_path('ydb/library/yql/cfg/tests/gateways.conf')) as f:
        text_format.Merge(f.read(), gateway_config)

    if yql_get_param('SQL_FLAGS'):
        flags = yql_get_param('SQL_FLAGS').split(',')
        gateway_config.SqlCore.TranslationFlags.extend(flags)
    return gateway_config.SqlCore.TranslationFlags

try:
    SQL_FLAGS = get_sql_flags()
except BaseException:
    SQL_FLAGS = None

def recursive_glob(root, begin_template=None, end_template=None):
    for parent, dirs, files in os.walk(root):
        for filename in files:
            if begin_template is not None and not filename.startswith(begin_template):
                continue
            if end_template is not None and not filename.endswith(end_template):
                continue
            path = os.path.join(parent, filename)
            yield os.path.relpath(path, root)


def pytest_generate_tests_by_template(template, metafunc):
    argvalues = []

    suites = [name for name in os.listdir(DATA_PATH) if os.path.isdir(os.path.join(DATA_PATH, name))]
    for suite in suites:
        for case in sorted([sql_query_path[:-len(template)]
                            for sql_query_path in recursive_glob(os.path.join(DATA_PATH, suite), end_template=template)]):
            argvalues.append((suite, case))

    metafunc.parametrize(['suite', 'case'], argvalues)


def pytest_generate_tests_for_run(metafunc, template='.sql', suites=None, currentPart=0, partsCount=1, data_path=None):
    if data_path is None:
        data_path = DATA_PATH
    argvalues = []

    if not suites:
        suites = sorted([name for name in os.listdir(data_path) if os.path.isdir(os.path.join(data_path, name))])

    for suite in suites:
        suite_dir = os.path.join(data_path, suite)
        # .sql's
        for case in sorted([sql_query_path[:-len(template)]
                            for sql_query_path in recursive_glob(suite_dir, end_template=template)]):
            case_program = case + template
            with open(os.path.join(suite_dir, case_program)) as f:
                if 'do not execute' in f.read():
                    continue

            # .cfg's
            configs = [
                cfg_file.replace(case + '-', '').replace('.cfg', '')
                for cfg_file in recursive_glob(suite_dir, begin_template=case + '-', end_template='.cfg')
            ]
            if os.path.exists(suite_dir + '/' + case + '.cfg'):
                configs.append('')
            for cfg in sorted(configs):
                if hash((suite, case, cfg)) % partsCount == currentPart:
                    argvalues.append((suite, case, cfg))
            if not configs and hash((suite, case, 'default.txt')) % partsCount == currentPart:
                argvalues.append((suite, case, 'default.txt'))

    metafunc.parametrize(
        ['suite', 'case', 'cfg'],
        argvalues,
    )


def pytest_generate_tests_for_part(metafunc, currentPart, partsCount):
    return pytest_generate_tests_for_run(metafunc, currentPart=currentPart, partsCount=partsCount)


def get_cfg_file(cfg, case):
    if cfg:
        return (case + '-' + cfg + '.cfg') if cfg != 'default.txt' else 'default.cfg'
    else:
        return case + '.cfg'


def validate_cfg(result):
    for r in result:
        assert r[0] in (
            "in",
            "out",
            "udf",
            "providers",
            "res",
            "canonize_peephole",
            "canonize_lineage",
            "peephole_use_blocks",
            "with_final_result_issues",
            "xfail",
            "pragma",
            "canonize_yt",
            "file",
            "http_file",
            "yt_file",
            "os",
            "param",
            ), "Unknown command in .cfg: %s" % (r[0])


def get_config(suite, case, cfg, data_path=None):
    if data_path is None:
        data_path = DATA_PATH
    result = []
    try:
        default_cfg = get_cfg_file('default.txt', case)
        inherit = ['canonize_peephole', 'canonize_lineage', 'peephole_use_blocks']
        with open(os.path.join(data_path, suite, default_cfg)) as cfg_file_content:
            result = [line.strip().split() for line in cfg_file_content.readlines() if line.strip() and line.strip().split()[0]]
        validate_cfg(result)
        result = [r for r in result if r[0] in inherit]
    except IOError:
        pass
    cfg_file = get_cfg_file(cfg, case)
    with open(os.path.join(data_path, suite, cfg_file)) as cfg_file_content:
        result = [line.strip().split() for line in cfg_file_content.readlines() if line.strip()] + result
    validate_cfg(result)
    return result


def load_json_file_strip_comments(path):
    with open(path) as file:
        return '\n'.join([line for line in file.readlines() if not line.startswith('#')])


def get_parameters_files(suite, config):
    result = []
    for line in config:
        if len(line) != 3 or not line[0] == "param":
            continue

        result.append((line[1], os.path.join(DATA_PATH, suite, line[2])))

    return result


def get_parameters_json(suite, config):
    parameters_files = get_parameters_files(suite, config)
    data = {}
    for p in parameters_files:
        value_json = json.loads(load_json_file_strip_comments(p[1]))
        data[p[0]] = {'Data': value_json}

    return data


def output_dir(name):
    output_dir = yatest.common.output_path(name)
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    return output_dir


def run_sql_on_mr(name, query, kikimr):
    out_dir = output_dir(name)
    opt_file = os.path.join(out_dir, 'opt.yql')
    results_file = os.path.join(out_dir, 'results.yson')

    try:
        kikimr(
            'yql-exec -d 1 -P %s --sql --run --optimize -i /dev/stdin --oexpr %s --oresults %s' % (
                kikimr.yql_pool_id,
                opt_file,
                results_file
            ),
            stdin=query
        )
    except yatest.common.ExecutionError as e:
        runyqljob_result = e.execution_result
        assert 0, 'yql-exec finished with error: \n\n%s \n\non program: \n\n%s' % (
            runyqljob_result.std_err,
            query
        )
    return opt_file, results_file


def normalize_table(csv, fields_order=None):
    '''
    :param csv: table content
    :param fields_order: normal order of fields (default: 'key', 'subkey', 'value')
    :return: normalized table content
    '''
    if not csv.strip():
        return ''

    headers = csv.splitlines()[0].strip().split(';')

    if fields_order is None:
        if len(set(headers)) < len(headers):
            # we have duplicates in case of joining tables, let's just cut headers and return as is
            return '\n'.join(csv.splitlines()[1:])

        fields_order = headers

    normalized = ''

    if any(field not in headers for field in fields_order):
        fields_order = sorted(headers)

    translator = {
        field: headers.index(field) for field in fields_order
    }

    def normalize_cell(s):
        if s == 't':
            return 'true'
        if s == 'f':
            return 'false'

        if '.' in s:
            try:
                f = float(s)
                return str(str(int(f)) if f.is_integer() else f)
            except ValueError:
                return s
        else:
            return s

    for line in csv.splitlines()[1:]:
        line = line.strip().split(';')
        normalized_cells = [normalize_cell(line[translator[field]]) for field in fields_order]
        normalized += '\n' + ';'.join(normalized_cells)

    return normalized.strip()

def replace_vars(sql_query, var_tag):
    """
    Sql can contain comment like /* yt_local_var: VAR_NAME=VAR_VALUE */
    it will replace VAR_NAME with VAR_VALUE within sql query
    """
    vars = re.findall(r"\/\* {}: (.*)=(.*) \*\/".format(var_tag), sql_query)
    for var_name, var_value in vars:
        sql_query = re.sub(re.escape(var_name.strip()), var_value.strip(), sql_query)
    return sql_query
