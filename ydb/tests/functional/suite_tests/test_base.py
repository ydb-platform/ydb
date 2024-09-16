# -*- coding: utf-8 -*-
import itertools
import json
import abc
import collections
import os
import random
import string
import logging
import time
import six
import enum
from concurrent import futures

from hamcrest import assert_that, is_, equal_to, raises, none
import ydb.tests.library.common.yatest_common as yatest_common
from yatest.common import source_path, test_source_path

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.oss.canonical import set_canondata_root

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def mute_sdk_loggers():
    loggers = (
        'ydb.connection',
        'ydb.table.SessionPool',
        'urllib3.connectionpool',
        'ydb.resolver.DiscoveryEndpointsResolver'
    )

    for logger_name in loggers:
        li = logging.getLogger(logger_name)
        li.setLevel(logging.CRITICAL)


mute_sdk_loggers()


@enum.unique
class StatementTypes(enum.Enum):
    Skipped = 'statement skipped'
    Ok = 'statement ok'
    Error = 'statement error'
    Query = 'statement query'
    StreamQuery = 'statement stream query'
    ImportTableData = 'statement import table data'


def get_statement_type(line):
    for s_type in list(StatementTypes):
        if s_type.value in line.lower():
            return s_type
    raise RuntimeError("Can't find statement type for line %s" % line)


def get_token(length=10):
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


def get_source_path(*args):
    arcadia_root = source_path('')
    return os.path.join(arcadia_root, test_source_path(os.path.join(*args)))


def is_empty_line(line):
    if line.split():
        return False
    return True


def get_lines(suite_path):
    with open(suite_path) as reader:
        for line_idx, line in enumerate(reader.readlines()):
            yield line_idx, line.rstrip()


def get_test_suites(directory):
    resources_root = get_source_path(directory)
    directories = [resources_root]
    suites = []
    while len(directories) > 0:
        current_directory = directories.pop()
        for candidate in map(lambda x: os.path.join(current_directory, x), os.listdir(current_directory)):
            if os.path.isfile(candidate) and candidate.endswith(".test"):
                suite_relative_path = os.path.relpath(candidate, resources_root)
                suites.append(('results', suite_relative_path,))
                suites.append(('plan', suite_relative_path))

            if os.path.isdir(candidate):
                directories.append(candidate)

    return suites


def get_single_statement(lines):
    statement_lines = []
    for line_idx, line in lines:
        if is_empty_line(line):
            statement = "\n".join(statement_lines)
            return statement
        statement_lines.append(line)
    return "\n".join(statement_lines)


class ParsedStatement(collections.namedtuple('ParsedStatement', ["at_line", "s_type", "suite_name", "text"])):
    def get_fields(self):
        return self._fields

    def __str__(self):
        result = ["", "Parsed Statement"]
        for field in self.get_fields():
            value = str(getattr(self, field))
            if field != 'text':
                result.append(' ' * 4 + '%s: %s,' % (field, value))
            else:
                result.append(' ' * 4 + '%s:' % field)
                result.extend([' ' * 8 + row for row in value.split('\n')])
        return "\n".join(result)


def get_statements(suite_path, suite_name):
    lines = get_lines(suite_path)
    for line_idx, line in lines:
        if is_empty_line(line) or not is_statement_definition(line):
            # empty line or junk lines
            continue
        text = get_single_statement(lines)
        yield ParsedStatement(
            line_idx,
            get_statement_type(line),
            suite_name,
            text)


def is_side_effect(statement_line):
    return statement_line.startswith('side effect: ')


def parse_side_effect(se_line):
    pieces = se_line.split(':')
    if len(pieces) < 3:
        raise RuntimeError("Invalid side effect description: %s" % se_line)
    se_type = pieces[1].strip()
    se_description = ':'.join(pieces[2:])
    se_description = se_description.strip()

    return se_type, se_description


def get_statement_and_side_effects(statement_text):
    statement_lines = statement_text.split('\n')
    side_effects = {}
    filtered = []
    for statement_line in statement_lines:
        if not is_side_effect(statement_line):
            filtered.append(statement_line)
            continue

        se_type, se_description = parse_side_effect(statement_line)

        side_effects[se_type] = se_description

    return '\n'.join(filtered), side_effects


def is_statement_definition(line):
    return line.startswith("statement")


def format_yql_statement(lines_or_statement, table_path_prefix):
    if not isinstance(lines_or_statement, list):
        lines_or_statement = [lines_or_statement]
    statement = "\n".join(
        ['pragma TablePathPrefix = "%s";' % table_path_prefix, "pragma SimpleColumns;"] + lines_or_statement)
    return statement


def safe_execute(method, *info):
    try:
        return method()
    except Exception as e:
        collected = "\n".join([str(e)] + [str(el) for el in info])
        raise RuntimeError(
            "Failed to execute. Collected info: %s" % collected
        )


def wrap_rows(rows):
    for row in rows:
        for key, value in six.iteritems(row):
            if isinstance(value, bytes):
                row[key] = value.decode('utf-8')
    return rows


def write_canonical_response(response, file):
    output_path = os.path.join(yatest_common.output_path(), file)
    with open(output_path, 'w') as w:
        w.write(json.dumps(response, indent=4, sort_keys=True))
    return yatest_common.canonical_file(
        local=True,
        universal_lines=True,
        path=output_path
    )


def format_as_table(data):
    nrows = len(data)

    if nrows == 0:
        return "(0 rows)\n"

    keys = data[0].keys()
    widths = [len(k) + 2 for k in keys]
    for row in data:
        for i, val in enumerate(row.values()):
            widths[i] = max(widths[i], len(str(val)) + 2)

    col_names = ['{: ^{w}}'.format(str(k), w=widths[i]) for i, k in enumerate(keys)]

    table = ""
    table += "|".join(col_names)
    table += "\n"
    table += "+".join(['-'*w for w in widths])
    table += "\n"

    for row in data:
        vals = ['{: >{w}}'.format(str(val) + ' ', w=widths[i]) for i, val in enumerate(row.values())]
        table += "|".join(vals)
        table += "\n"

    table += "(%d row%s)\n" % (nrows, 's' if nrows != 1 else 0)

    return table


@six.add_metaclass(abc.ABCMeta)
class BaseSuiteRunner(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(
                load_udfs=True,
                use_in_memory_pdisks=True,
                disable_iterator_reads=True,
                disable_iterator_lookups=True,
                extra_feature_flags=["enable_resource_pools"],
                # additional_log_configs={'KQP_YQL': 7}
            )
        )
        cls.cluster.start()
        cls.table_path_prefix = None
        cls.table_path_prefix_ne = None
        cls.driver = ydb.Driver(ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)))
        cls.pool = ydb.SessionPool(cls.driver)
        cls.driver.wait()
        cls.query_id = itertools.count(start=1)
        cls.files = {}
        cls.plan = False
        set_canondata_root('ydb/tests/functional/suite_tests/canondata')

    @classmethod
    def teardown_class(cls):
        cls.pool.stop()
        cls.driver.stop()
        cls.cluster.stop()

    def run_sql_suite(self, kind, *path_pieces):
        self.files = {}
        self.plan = (kind == 'plan')
        self.query_id = itertools.count(start=1)
        self.table_path_prefix = "/Root/%s" % '_'.join(list(path_pieces) + [kind])
        self.table_path_prefix_ne = self.table_path_prefix + "_ne"
        for parsed_statement in get_statements(get_source_path(*path_pieces), os.path.join(*path_pieces)):
            self.assert_statement(parsed_statement)
        return self.files

    def assert_statement_import_table_data(self, statement):
        # insert into {tableName} () VALUES {dataPath}
        base_path = statement.text.split()[-1]
        full_path = get_source_path(base_path)
        lines = get_lines(full_path)
        entries = ["(%s)" % ",".join(line.split()) for _, line in lines]
        batch_size = 1
        future_results = []
        tp = futures.ThreadPoolExecutor(20)
        for start_point in range(0, len(entries), batch_size):
            end_point = min(start_point + batch_size, len(entries))
            cmd = statement.text.replace(base_path, ",".join(entries[start_point:end_point]))
            future_results.append(
                tp.submit(
                    self.execute_query,
                    cmd,
                )
            )

        for future in future_results:
            safe_execute(lambda: future.result(), statement)

    def assert_statement(self, parsed_statement):
        start_time = time.time()
        from_type = {
            StatementTypes.Ok: self.assert_statement_ok,
            StatementTypes.Query: self.assert_statement_query,
            StatementTypes.StreamQuery: self.assert_statement_stream_query,
            StatementTypes.Error: (lambda x: x),
            StatementTypes.ImportTableData: self.assert_statement_import_table_data,
            StatementTypes.Skipped: lambda x: x
        }
        assert_method = from_type.get(parsed_statement.s_type)
        assert_method(parsed_statement)
        end_time = time.time()
        logger.info("Executed statement at line %d, suite from %s, in %0.3f seconds" % (
            parsed_statement.at_line, parsed_statement.suite_name, end_time - start_time))

    def assert_statement_ok(self, statement):
        actual = safe_execute(lambda: self.execute_ydb_ok(statement.text), statement)
        assert_that(
            actual,
            is_(none()),
            str(statement),
        )

    def assert_statement_error(self, statement):
        # not supported yet
        statement_text, side_effects = get_statement_and_side_effects(statement.text)
        assert_that(
            lambda: self.execute_query(statement_text),
            raises(
                ydb.Error
            )
        )

    def get_query_and_output(self, statement_text):
        return statement_text, None

    @staticmethod
    def pretty_json(j):
        return json.dumps(j, indent=4, sort_keys=True)

    def remove_optimizer_estimates(self, query_plan):
        if 'Plans' in query_plan:
            for p in query_plan['Plans']:
                self.remove_optimizer_estimates(p)
        if 'Operators' in query_plan:
            for op in query_plan['Operators']:
                for key in ['A-Cpu', 'A-Rows', 'E-Cost', 'E-Rows', 'E-Size']:
                    if key in op:
                        del op[key]

    def assert_statement_query(self, statement):
        def get_actual_and_expected():
            query, expected = self.get_query_and_output(statement.text)
            actual = self.execute_query(query)
            return actual, expected

        query_id = next(self.query_id)
        query_name = "query_%d" % query_id
        if self.plan:
            query_plan = json.loads(self.explain(statement.text))
            if 'SimplifiedPlan' in query_plan:
                del query_plan['SimplifiedPlan']
            if 'Plan' in query_plan:
                self.remove_optimizer_estimates(query_plan['Plan'])
            self.files[query_name + '.plan'] = write_canonical_response(
                query_plan,
                query_name + '.plan',
            )

            return

        actual_output, expected_output = safe_execute(get_actual_and_expected, statement, query_name)

        if len(actual_output) > 0:
            self.files[query_name] = write_canonical_response(
                wrap_rows(actual_output[0].rows), query_name)

            if expected_output:
                assert_that(
                    actual_output[0].rows,
                    equal_to(expected_output),
                    str(
                        statement
                    )
                )

    def execute_scan_query(self, yql_text):
        success = False
        retries = 10
        while retries > 0 and not success:
            retries -= 1

            it = self.driver.table_client.scan_query(yql_text)
            result = []
            while True:
                try:
                    response = next(it)
                    for row in response.result_set.rows:
                        result.append(row)

                except StopIteration:
                    return result

                except Exception:
                    if retries == 0:
                        raise

    def assert_statement_stream_query(self, statement):
        if self.plan:
            return

        yql_text = format_yql_statement(statement.text, self.table_path_prefix)
        yql_text = "--!syntax_v1\n" + yql_text + "\n\n"
        result = self.execute_scan_query(yql_text)
        file_name = statement.suite_name.split('/')[1] + '.out'
        output_path = os.path.join(yatest_common.output_path(), file_name)
        with open(output_path, 'a+') as w:
            w.write(yql_text)
            w.write(format_as_table(result))
            w.write("\n\n")

        self.files[file_name] = yatest_common.canonical_file(
            path=output_path,
            local=True,
            universal_lines=True,
        )

    def is_probably_scheme(self, yql_text):
        lwr = yql_text.lower()
        return 'create table' in lwr or 'drop table' in lwr

    def execute_ydb_ok(self, statement_text):
        if self.is_probably_scheme(statement_text):
            self.execute_scheme(statement_text)
        else:
            self.execute_query(statement_text)

    def explain(self, query):
        yql_text = format_yql_statement(query, self.table_path_prefix)
        return self.pool.retry_operation_sync(lambda s: s.explain(yql_text)).query_plan

    def execute_scheme(self, statement_text):
        yql_text = format_yql_statement(statement_text, self.table_path_prefix)
        self.pool.retry_operation_sync(lambda s: s.execute_scheme(yql_text))
        yql_text = format_yql_statement(statement_text, self.table_path_prefix_ne)
        self.pool.retry_operation_sync(lambda s: s.execute_scheme(yql_text))
        return None

    def execute_query(self, statement_text):
        yql_text = format_yql_statement(statement_text, self.table_path_prefix)
        result = self.pool.retry_operation_sync(lambda s: s.transaction().execute(yql_text, commit_tx=True))

        if len(result) == 1:
            scan_query_result = self.execute_scan_query(yql_text)
            for i in range(len(result)):
                self.execute_assert(
                    result[i].rows,
                    scan_query_result,
                    "Results are not same",
                )

        return result
