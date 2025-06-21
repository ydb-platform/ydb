# -*- coding: utf-8 -*-
import itertools
import json
import abc
import os
import random
import string
import logging
import six
import enum
import re
from functools import cmp_to_key
from concurrent import futures

from hamcrest import assert_that, equal_to, is_not, raises
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
        Ok = 'ok'
        Error = 'error'
        Query = 'query'
        StreamQuery = 'stream query'
        ImportTableData = 'import table data'

    class SqlStatementType(enum.Enum):
        Create = "create"
        DropTable = "drop"
        Insert = "insert"
        Uosert = "upsert"
        Replace = "replace"
        Delete = "delete"
        Select = "select"

    @enum.unique
    class TableType(enum.Enum):
        Row = 'row'
        Column = 'column'

    def __init__(self, suite: str, at_line: int, type: Type, text: [str], sql_statement_type: SqlStatementType, table_types: {TableType}):
        self.suite_name = suite
        self.at_line = at_line
        self.s_type = type
        self.text = text
        self.sql_statement_type = sql_statement_type
        self.table_types = table_types

    def __str__(self):
        return f'''StatementDefinition:
    suite: {self.suite_name}
    line: {self.at_line}
    type: {self.s_type}
    sql_stmt_tyoe: {self.sql_statement_type}
    table_types: {', '.join([str(t) for t in self.table_types])}
    text:
''' + '\n'.join([f'        {row}' for row in self.text.split('\n')])

    @staticmethod
    def _parse_statement_type(statement_line: str, suite: str, at_line: int) -> (Type, {TableType}):
        parts = statement_line.split(' ')
        parts.pop(0)  # skip 'statement' word
        table_types = {}
        if parts[0] == 'error':
            type = StatementDefinition.Type.Error
        else:
            if parts[0] == 'skipped':
                parts.pop(0)
            elif parts[0] == 'skipped_cs':
                table_types = {StatementDefinition.TableType.Row}
                parts.pop(0)
            else:
                table_types = {StatementDefinition.TableType.Row, StatementDefinition.TableType.Column}
            type = None
            if table_types:  # ignore rest of the line for skipped tests
                for t in list(StatementDefinition.Type):
                    if t.value == ' '.join(parts):
                        type = t
                if type is None:
                    raise RuntimeError(f'Unknown statement type in {suite}, at line: {at_line}')
        return (type, table_types)

    @staticmethod
    def _parse_sql_statement_type(lines: [str]) -> SqlStatementType:
        for line in lines:
            line = line.lower()
            if line.startswith("pragma"):
                continue
            for t in list(StatementDefinition.SqlStatementType):
                if line.startswith(t.value):
                    return t
        return None

    @staticmethod
    def parse(suite: str, at_line: int, lines: list[str]):
        if not lines or not lines[0]:
            raise RuntimeError(f'Invalid statement in {suite}, at line: {at_line}')
        type, table_types = StatementDefinition._parse_statement_type(lines[0], suite, at_line)
        lines.pop(0)
        at_line += 1
        statement_lines = []
        for line in lines:
            if line.startswith('side effect: '):  # side effects are not supported yet
                pass
            else:
                statement_lines.append(line)
        sql_statement_type = StatementDefinition._parse_sql_statement_type(statement_lines)
        if sql_statement_type is None:
            raise RuntimeError(f'Unknown sql statement type in {suite}, at line: {at_line}')
        return StatementDefinition(suite, at_line, type, "\n".join(statement_lines), sql_statement_type, table_types)


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
        yield (statement_start_line_idx + 1, statement_lines)


def get_statements(suite_path, suite_name):
    for statement_start_line_idx, statement_lines in split_by_statement(get_lines(suite_path)):
        yield StatementDefinition.parse(
            suite_name,
            statement_start_line_idx,
            statement_lines,
        )


def patch_create_table_for_column_table(stmt):
    if stmt[-1] == ";":
        stmt = stmt[:-1]
    return stmt + " WITH (STORE = COLUMN)"


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
                extra_feature_flags=["enable_resource_pools"],
                column_shard_config={
                    'allow_nullable_columns_in_pk': True,
                },
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
        for statement in get_statements(get_source_path(*path_pieces), os.path.join(*path_pieces)):
            self.assert_statement(statement)
        return self.files

    def get_table_prefix(self, table_type: StatementDefinition.TableType) -> str:
        if table_type == StatementDefinition.TableType.Column:
            return f"{self.table_path_prefix}_{str(table_type.value)}"
        return self.table_path_prefix

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
                    statement,
                    cmd,
                )
            )

        for future in future_results:
            safe_execute(lambda: future.result(), statement)

    def assert_statement(self, statement):
        from_type = {
            StatementDefinition.Type.Ok: self.assert_statement_ok,
            StatementDefinition.Type.Query: self.assert_statement_query,
            StatementDefinition.Type.StreamQuery: self.assert_statement_stream_query,
            StatementDefinition.Type.Error: (lambda x: x),
            StatementDefinition.Type.ImportTableData: self.assert_statement_import_table_data,
        }
        if not statement.table_types:
            logger.info(f"Skipped statement {statement.suite_name}:{statement.at_line}")
            return
        assert_method = from_type.get(statement.s_type)
        assert_method(statement)

    def assert_statement_ok(self, statement):
        assert_that(
            lambda: self.execute_query(statement),
            is_not(raises(ydb.Error)),
        )

    def assert_statement_error(self, statement):
        assert_that(
            lambda: self.execute_query(statement),
            raises(
                ydb.Error
            )
        )

    def get_expected_output(self, _):
        return None

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
        query_id = next(self.query_id)
        query_name = "query_%d" % query_id
        print('query_name: {query_name}')
        print('statement: {statement}')
        if self.plan:
            json_deser = self.explain(statement.text)
            json_deser = re.sub(r'precompute_\d+_\d+', 'precompute', json_deser)
            query_plan = json.loads(json_deser)
            if 'SimplifiedPlan' in query_plan:
                del query_plan['SimplifiedPlan']
            if 'Plan' in query_plan:
                self.remove_optimizer_estimates(query_plan['Plan'])
            self.files[query_name + '.plan'] = write_canonical_response(
                query_plan,
                query_name + '.plan',
            )

            return
        expected_output = self.get_expected_output(statement.text)
        actual_output = safe_execute(lambda: self.execute_query(statement), statement, query_name)

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
        def callee():
            it = self.driver.table_client.scan_query(yql_text)
            result = []
            for response in it:
                for row in response.result_set.rows:
                    result.append(row)
            return result
        return ydb.retry_operation_sync(callee)

    def assert_statement_stream_query(self, statement):
        if self.plan:
            return

        yql_text = patch_yql_statement(statement.text, self.get_table_prefix(StatementDefinition.TableType.Row))
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
        yql_text = patch_yql_statement(query, self.get_table_prefix(StatementDefinition.TableType.Row))
        # seems explain not working with query service ?
        """
        result_sets = self.pool.execute_with_retries(yql_text, exec_mode=ydb.query.base.QueryExecMode.EXPLAIN)
        first_set = result_sets[0]
        print("***", first_set)
        print("***", first_set.rows)
        return first_set.rows[0]
        """

        return self.legacy_pool.retry_operation_sync(lambda s: s.explain(yql_text)).query_plan

    def execute_query(self, statement: StatementDefinition, amended_text: str = None):
        text_row = amended_text if amended_text is not None else statement.text
        if statement.sql_statement_type == StatementDefinition.SqlStatementType.Create:
            text_column = patch_create_table_for_column_table(text_row)
        else:
            text_column = text_row
        text_row = patch_yql_statement(text_row, self.get_table_prefix(StatementDefinition.TableType.Row))
        text_column = patch_yql_statement(text_column, self.get_table_prefix(StatementDefinition.TableType.Column))
        result_row = self.pool.execute_with_retries(text_row)
        if StatementDefinition.TableType.Column in statement.table_types:
            result_column = self.pool.execute_with_retries(text_column)

        if statement.sql_statement_type == StatementDefinition.SqlStatementType.Select:
            scan_query_result = self.execute_scan_query(text_row)
            self.execute_assert(
                result_row[0].rows,
                scan_query_result,
                "Scan query and query service produce different results",
            )
            if StatementDefinition.TableType.Column in statement.table_types:
                def flatten_result(result_sets):
                    return [row for result_set in result_sets for row in result_set.rows]

                def compare(lhs, rhs):
                    lhs_keys = sorted(lhs.keys())
                    rhs_keys = sorted(rhs.keys())
                    if lhs_keys != rhs_keys:
                        return -1 if lhs_keys < rhs_keys else 1

                    def cmp_val_none(lhs, rhs):
                        if lhs is None or rhs is None:
                            if lhs == rhs:
                                return 0
                            return -1 if lhs is None else 1
                        if lhs == rhs:
                            return 0
                        return -1 if lhs < rhs else 1

                    for k in lhs_keys:
                        c = cmp_val_none(lhs[k], rhs[k])
                        if c != 0:
                            return c
                    return 0

                sorted_result_row = sorted(flatten_result(result_row), key=cmp_to_key(compare))
                sorted_result_column = sorted(flatten_result(result_column), key=cmp_to_key(compare))

                self.execute_assert(
                    sorted_result_row,
                    sorted_result_column,
                    f"Row table and column table produce different results: \n{sorted_result_row} \n{sorted_result_column}",
                )
        return result_row
