import os
import re
from typing import Optional, List

import pytest
import yatest.common
import library.python.port_manager

import yql_utils


class KqpRun(object):
    def __init__(self, config_file: str, scheme_file: str, udfs_dir: Optional[str] = None, path_prefix: str = ""):
        self.kqprun_binary: str = yql_utils.yql_binary_path('ydb/tests/tools/kqprun/kqprun')

        self.config_file: str = yql_utils.yql_source_path(config_file)
        self.scheme_file: str = yql_utils.yql_source_path(scheme_file)

        self.res_dir: str = yql_utils.get_yql_dir(prefix=f'{path_prefix}kqprun_')
        self.port_manager = library.python.port_manager.PortManager()
        self.grpc_port: Optional[int] = self.port_manager.get_port()

        if udfs_dir is None:
            self.udfs_dir: str = yql_utils.get_udfs_path()
        else:
            self.udfs_dir: str = udfs_dir

        self.tables: List[str] = []
        self.topics: List[str] = []
        self.cancel_on_finish_topics: List[str] = []
        self.queries: List[str] = []

    def __res_file_path(self, name: str) -> str:
        return os.path.join(self.res_dir, name)

    def __normalize_explain_file(self, name: str) -> None:
        if not os.path.exists(name):
            return

        with open(name, 'r') as f:
            content = f.read()

        content = re.sub(r"""('"_logical_id"\s+')\d+""", r"\g<1>0", content)
        content = re.sub(r"""('"_id"\s+'")[0-9a-f-]+(")""", r"\g<1><id>\2", content)
        content = re.sub(r'"[0-9a-f]{1,8}(?:-[0-9a-f]{1,8}){3}"', '"<id>"', content)
        content = re.sub(r'localhost:\d+', 'localhost:<port>', content)

        with open(name, 'w') as f:
            f.write(content)

    def replace_scheme(self, replace) -> None:
        scheme_file = self.scheme_file
        if not os.path.exists(scheme_file):
            scheme_file = yatest.common.source_path(scheme_file)

        with open(scheme_file, 'r') as f:
            scheme = f.read()

        scheme = replace(scheme)
        self.scheme_file = self.__res_file_path('scheme.sql')

        with open(self.scheme_file, 'w') as f:
            f.write(scheme)

    def add_table(self, name: str, content: List[str], attrs: Optional[str] = None):
        table_path = self.__res_file_path(f'table_{len(self.tables)}.yson')
        with open(table_path, 'w') as table:
            for row in content:
                table.write(f'{row}\n')

        if attrs is not None:
            with open(f'{table_path}.attr', 'w') as table_attrs:
                table_attrs.write(attrs)

        self.tables.append(f'yt./Root/plato.{name}@{table_path}')

    def add_topic(self, name: str, content: List[str], partitions_count: int = 1, cancel_on_finish: bool = False):
        topic_path = self.__res_file_path(f'topic_{len(self.topics)}.txt')
        with open(topic_path, 'w') as topic:
            for row in content:
                topic.write(f'{row}\n')

        partition_suffix = f':{partitions_count}' if partitions_count != 1 else ''
        self.topics.append(f'{name}@{topic_path}{partition_suffix}')
        if cancel_on_finish:
            self.cancel_on_finish_topics.append(name)

    def add_query(self, sql: str):
        query_path = self.__res_file_path(f'query_{len(self.queries)}.sql')
        with open(query_path, 'w') as query:
            query.write(sql)

        self.queries.append(query_path)

    def yql_exec(self, verbose: bool = False, check_error: bool = True, var_templates: Optional[List[str]] = None,
                 yql_program: Optional[str] = None, yql_tables: List[yql_utils.Table] = [], user: Optional[str] = None,
                 action: str = "execute") -> yql_utils.YQLExecResult:
        udfs_dir = self.udfs_dir

        config_file = self.config_file
        scheme_file = self.scheme_file

        results_file = self.__res_file_path('results.txt')
        ast_file = self.__res_file_path('ast.txt')
        plan_file = self.__res_file_path('plan.json')
        log_file = self.__res_file_path('log.txt')

        cmd = self.kqprun_binary + ' '

        cmd += (
            '--emulate-yt '
            '--exclude-linked-udfs '
            '--execution-case query '
            f'--script-action {action} '
            f'--app-config={config_file} '
            f'--scheme-query={scheme_file} '
            f'--result-file={results_file} '
            f'--script-ast-file={ast_file} '
            f'--script-plan-file={plan_file} '
            f'--log-file={log_file} '
            f'--udfs-dir={udfs_dir} '
            '--result-format full-proto '
            '--plan-format json '
            '--result-rows-limit 0 '
        )
        if self.grpc_port is not None:
            cmd += f'--grpc={self.grpc_port} '

        if user is not None:
            cmd += f'-U {user} '

        if var_templates is not None:
            for var_template in var_templates:
                cmd += f'--template {var_template} '

        for query in self.queries:
            cmd += f'--script-query={query} '

        if yql_program is not None:
            program_file = yql_utils.prepare_program(yql_program, None, self.res_dir, ext='sql')[1]
            cmd += f'--script-query={program_file} '

        for table in self.tables:
            cmd += f'--emulate-yt={table} '

        for topic in self.topics:
            cmd += f'--emulate-pq={topic} '

        for topic in self.cancel_on_finish_topics:
            cmd += f'--emulate-pq-cancel-on-finish={topic} '

        for table in yql_tables:
            if table.format != 'yson':
                pytest.skip('skip tests containing tables with a non-yson attribute format')
            cmd += f'--emulate-yt=yt./Root/{table.full_name}@{table.yqlrun_file} '

        proc_result = yatest.common.process.execute(cmd.strip().split(), check_exit_code=False, cwd=self.res_dir)
        if proc_result.exit_code != 0 and check_error:
            assert 0, f'Command\n{cmd}\n finished with exit code {proc_result.exit_code}, stderr:\n\n{proc_result.std_err}\n\nlog file:\n{yql_utils.read_res_file(log_file)[1]}'

        self.__normalize_explain_file(ast_file)
        self.__normalize_explain_file(plan_file)

        results, log_results = yql_utils.read_res_file(results_file)
        ast, log_ast = yql_utils.read_res_file(ast_file)
        plan, log_plan = yql_utils.read_res_file(plan_file)
        err, log_err = yql_utils.read_res_file(log_file)

        if verbose:
            yql_utils.log('QUERIES:')
            if yql_program is not None:
                yql_utils.log(yql_program)

            for query in self.queries:
                yql_utils.log(query)

            yql_utils.log('RESULTS:')
            yql_utils.log(log_results)
            yql_utils.log('AST:')
            yql_utils.log(log_ast)
            yql_utils.log('PLAN:')
            yql_utils.log(log_plan)
            yql_utils.log('ERROR:')
            yql_utils.log(log_err)

        return yql_utils.YQLExecResult(
            proc_result.std_out,
            proc_result.std_err,
            results,
            results_file,
            ast,
            ast_file,
            plan,
            plan_file,
            yql_program,
            proc_result,
            None,
        )
