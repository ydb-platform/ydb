# -*- coding: utf-8 -*-
import itertools
import json
import abc
import os
import random
import string
import logging
import time
import six
import enum
from concurrent import futures

from hamcrest import assert_that, equal_to, raises
import yatest
from yatest.common import source_path, test_source_path

from ydb.tests.library.harness.kikimr_runner import KiKiMR
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


class StatementDefinition:
    @enum.unique
    class Type(enum.Enum):
        Skipped = 'statement skipped'
        Ok = 'statement ok'
        Error = 'statement error'
        Query = 'statement query'
        StreamQuery = 'statement stream query'
        ImportTableData = 'statement import table data'

    def __init__(self, suite: str, at_line: int, type: Type, text: [str]):
        self.suite_name = suite
        self.at_line = at_line
        self.s_type = type
        self.text = text

    def __str__(self):
        return f'''StatementDefinition:
    suite: {self.suite_name}
    line: {self.at_line}
    type: {self.s_type}
    text:
''' + '\n'.join([f'        {row}' for row in self.text.split('\n')])

    @staticmethod
    def _parse_statement_type(statement_line: str) -> Type:
        for t in list(StatementDefinition.Type):
            if t.value in statement_line.lower():
                return t
        return None

    @staticmethod
    def parse(suite: str, at_line: int, lines: list[str]):
        if not lines or not lines[0]:
            raise RuntimeError(f'Invalid statement in {suite}, at line: {at_line}')
        type = StatementDefinition._parse_statement_type(lines[0])
        if type is None:
            raise RuntimeError(f'Unknown statement type in {suite}, at line: {at_line}')
        lines.pop(0)
        at_line += 1
        statement_lines = []
        for line in lines:
            if line.startswith('side effect: '):  # side effects are not supported yet
                pass
            else:
                statement_lines.append(line)
        return StatementDefinition(suite, at_line, type, "\n".join(statement_lines))


def get_token(length=10):
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


def get_source_path(*args):
    arcadia_root = source_path('')
    return os.path.join(arcadia_root, test_source_path(os.path.join(*args)))


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


def split_by_statement(lines):
    statement_lines = []
    statement_start_line_idx = 0
    for line_idx, line in lines:
        if line:
            if line.startswith("statement "):
                statement_start_line_idx = line_idx
                statement_lines = [line]
            elif statement_lines:
                statement_lines.append(line)
        else:
            if statement_lines:
                yield (statement_start_line_idx, statement_lines)
                statement_lines = []
    if statement_lines:
        yield (statement_start_line_idx, statement_lines)


def get_statements(suite_path, suite_name):
    for statement_start_line_idx, statement_lines in split_by_statement(get_lines(suite_path)):
        yield StatementDefinition.parse(
            suite_name,
            statement_start_line_idx,
            statement_lines,
        )


def patch_yql_statement(lines_or_statement, table_path_prefix):
    if not isinstance(lines_or_statement, list):
        lines_or_statement = [lines_or_statement]
    statement = "\n".join(
        ['pragma TablePathPrefix = "%s";' % table_path_prefix, "pragma SimpleColumns;", "pragma AnsiCurrentRow;"] + lines_or_statement)
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
    output_path = os.path.join(yatest.common.output_path(), file)
    with open(output_path, 'w') as w:
        w.write(json.dumps(response, indent=4, sort_keys=True))
    return yatest.common.canonical_file(
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
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                udfs_path=yatest.common.build_path("yql/udfs"),
                use_in_memory_pdisks=True,
                disable_iterator_reads=True,
                disable_iterator_lookups=True,
                extra_feature_flags=["enable_resource_pools"],
                # additional_log_configs={'KQP_YQL': 7}
            )
        )
        cls.cluster.start()
        cls.table_path_prefix = None
        cls.driver = ydb.Driver(ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)))
        cls.legacy_pool = ydb.SessionPool(cls.driver)  # for explain
        cls.pool = ydb.QuerySessionPool(cls.driver)
        cls.driver.wait()
        cls.query_id = itertools.count(start=1)
        cls.files = {}
        cls.plan = False
        set_canondata_root('ydb/tests/functional/suite_tests/canondata')

    @classmethod
    def teardown_class(cls):
        cls.legacy_pool.stop()
        cls.pool.stop()
        cls.driver.stop()
        cls.cluster.stop()

    def run_sql_suite(self, kind, *path_pieces):
        self.files = {}
        self.plan = (kind == 'plan')
        self.query_id = itertools.count(start=1)
        self.table_path_prefix = "/Root/%s" % '_'.join(list(path_pieces) + [kind])
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
            StatementDefinition.Type.Ok: self.assert_statement_ok,
            StatementDefinition.Type.Query: self.assert_statement_query,
            StatementDefinition.Type.StreamQuery: self.assert_statement_stream_query,
            StatementDefinition.Type.Error: (lambda x: x),
            StatementDefinition.Type.ImportTableData: self.assert_statement_import_table_data,
            StatementDefinition.Type.Skipped: lambda x: x
        }
        assert_method = from_type.get(parsed_statement.s_type)
        assert_method(parsed_statement)
        end_time = time.time()
        logger.info("Executed statement at line %d, suite from %s, in %0.3f seconds" % (
            parsed_statement.at_line, parsed_statement.suite_name, end_time - start_time))

    def assert_statement_ok(self, statement):
        actual = safe_execute(lambda: self.execute_query(statement.text))
        assert_that(
            len(actual),
            1,
            str(statement),
        )

    def assert_statement_error(self, statement):
        assert_that(
            lambda: self.execute_query(statement.text),
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

        yql_text = patch_yql_statement(statement.text, self.table_path_prefix)
        yql_text = "--!syntax_v1\n" + yql_text + "\n\n"
        result = self.execute_scan_query(yql_text)
        file_name = statement.suite_name.split('/')[1] + '.out'
        output_path = os.path.join(yatest.common.output_path(), file_name)
        with open(output_path, 'a+') as w:
            w.write(yql_text)
            w.write(format_as_table(result))
            w.write("\n\n")

        self.files[file_name] = yatest.common.canonical_file(
            path=output_path,
            local=True,
            universal_lines=True,
        )

    def explain(self, query):
        yql_text = patch_yql_statement(query, self.table_path_prefix)
        # seems explain not working with query service ?
        """
        result_sets = self.pool.execute_with_retries(yql_text, exec_mode=ydb.query.base.QueryExecMode.EXPLAIN)
        first_set = result_sets[0]
        print("***", first_set)
        print("***", first_set.rows)
        return first_set.rows[0]
        """

        return self.legacy_pool.retry_operation_sync(lambda s: s.explain(yql_text)).query_plan

    def execute_query(self, statement_text):
        yql_text = patch_yql_statement(statement_text, self.table_path_prefix)
        result = self.pool.execute_with_retries(yql_text)

        if len(result) == 1:
            scan_query_result = self.execute_scan_query(yql_text)
            for i in range(len(result)):
                self.execute_assert(
                    result[i].rows,
                    scan_query_result,
                    "Results are not same",
                )

        return result
