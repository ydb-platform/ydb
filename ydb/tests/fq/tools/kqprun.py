import os
from typing import Optional, List

import pytest
import yatest.common

import yql_utils


class KqpRun(object):
    def __init__(self, config_file: str, scheme_file: str, udfs_dir: Optional[str] = None, path_prefix: str = ""):
        self.kqprun_binary: str = yql_utils.yql_binary_path('ydb/tests/tools/kqprun/kqprun')

        self.config_file: str = yql_utils.yql_source_path(config_file)
        self.scheme_file: str = yql_utils.yql_source_path(scheme_file)

        self.res_dir: str = yql_utils.get_yql_dir(prefix=f'{path_prefix}kqprun_')

        if udfs_dir is None:
            self.udfs_dir: str = yql_utils.get_udfs_path()
        else:
            self.udfs_dir: str = udfs_dir

        self.tables: List[str] = []
        self.queries: List[str] = []

    def __res_file_path(self, name: str) -> str:
        return os.path.join(self.res_dir, name)

    def add_table(self, name: str, content: List[str], attrs: Optional[str] = None):
        table_path = self.__res_file_path(f'table_{len(self.tables)}.yson')
        with open(table_path, 'w') as table:
            for row in content:
                table.write(f'{row}\n')

        if attrs is not None:
            with open(f'{table_path}.attr', 'w') as table_attrs:
                table_attrs.write(attrs)

        self.tables.append(f'yt./Root/plato.{name}@{table_path}')

    def add_query(self, sql: str):
        query_path = self.__res_file_path(f'query_{len(self.queries)}.sql')
        with open(query_path, 'w') as query:
            query.write(sql)

        self.queries.append(query_path)

    def yql_exec(self, verbose: bool = False, check_error: bool = True, var_templates: Optional[List[str]] = None,
                 yql_program: Optional[str] = None, yql_tables: List[yql_utils.Table] = []) -> yql_utils.YQLExecResult:
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

        if var_templates is not None:
            for var_template in var_templates:
                cmd += f'--var-template {var_template} '

        for query in self.queries:
            cmd += f'--script-query={query} '

        if yql_program is not None:
            program_file = yql_utils.prepare_program(yql_program, None, self.res_dir, ext='sql')[1]
            cmd += f'--script-query={program_file} '

        for table in self.tables:
            cmd += f'--table={table} '

        for table in yql_tables:
            if table.format != 'yson':
                pytest.skip('skip tests containing tables with a non-yson attribute format')
            cmd += f'--table=yt./Root/{table.full_name}@{table.yqlrun_file} '

        proc_result = yatest.common.process.execute(cmd.strip().split(), check_exit_code=False, cwd=self.res_dir)
        if proc_result.exit_code != 0 and check_error:
            assert 0, f'Command\n{cmd}\n finished with exit code {proc_result.exit_code}, stderr:\n\n{proc_result.std_err}\n\nlog file:\n{yql_utils.read_res_file(log_file)[1]}'

        results, log_results = yql_utils.read_res_file(results_file)
        ast, log_ast = yql_utils.read_res_file(ast_file)
        plan, log_plan = yql_utils.read_res_file(plan_file)
        err, log_err = yql_utils.read_res_file(log_file)

        if verbose:
            yql_utils.log('QUERIES:')
            if yql_program is not None:
                yql_utils.log(yql_program)

            for query in self.queries:
                yql_utils.log(yql_program)

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
