import os
from typing import Optional, List

import yatest.common

import yql_utils


class FqRun:
    def __init__(self, config_file: str, udfs_dir: Optional[str] = None, path_prefix: str = ""):
        self.fqrun_binary: str = yql_utils.yql_binary_path('ydb/tests/tools/fqrun/fqrun')

        self.config_file: str = yql_utils.yql_source_path(config_file)

        self.res_dir: str = yql_utils.get_yql_dir(prefix=f'{path_prefix}fqrun_')

        if udfs_dir is None:
            self.udfs_dir: str = yql_utils.get_udfs_path()
        else:
            self.udfs_dir: str = udfs_dir

        self.topics: List[str] = []
        self.queries: List[str] = []

    def __res_file_path(self, name: str) -> str:
        return os.path.join(self.res_dir, name)

    def replace_config(self, replace):
        with open(self.config_file, 'r') as f:
            config = f.read()

        config = replace(config)
        self.config_file = self.__res_file_path('fq_config.conf')

        with open(self.config_file, 'w') as f:
            f.write(config)

    def add_topic(self, name: str, content: List[str]):
        topic_path = self.__res_file_path(f'topic_{len(self.topics)}.txt')
        with open(topic_path, 'w') as topic:
            for row in content:
                topic.write(f'{row}\n')

        self.topics.append(f'{name}@{topic_path}')

    def add_query(self, sql: str):
        query_path = self.__res_file_path(f'query_{len(self.queries)}.sql')
        with open(query_path, 'w') as query:
            query.write(sql)

        self.queries.append(query_path)

    def yql_exec(self, verbose: bool = False, check_error: bool = True, action: str = "run") -> yql_utils.YQLExecResult:
        results_file = self.__res_file_path('results.txt')
        ast_file = self.__res_file_path('ast.txt')
        plan_file = self.__res_file_path('plan.json')
        log_file = self.__res_file_path('log.txt')

        cmd = self.fqrun_binary + ' '

        cmd += (
            '--exclude-linked-udfs '
            f'--action={action} '
            f'--cfg={self.config_file} '
            f'--result-file={results_file} '
            f'--ast-file={ast_file} '
            f'--plan-file={plan_file} '
            f'--log-file={log_file} '
            f'--udfs-dir={self.udfs_dir} '
            '--result-format=full-proto '
            '--canonical-output '
        )

        for query in self.queries:
            cmd += f'--query={query} '

        for topic in self.topics:
            cmd += f'--emulate-pq={topic} '

        proc_result = yatest.common.process.execute(cmd.strip().split(), check_exit_code=False, cwd=self.res_dir)
        if proc_result.exit_code != 0 and check_error:
            assert 0, f'Command\n{cmd}\n finished with exit code {proc_result.exit_code}, stderr:\n\n{proc_result.std_err}\n\nlog file:\n{yql_utils.read_res_file(log_file)[1]}'

        results, log_results = yql_utils.read_res_file(results_file)
        ast, log_ast = yql_utils.read_res_file(ast_file)
        plan, log_plan = yql_utils.read_res_file(plan_file)
        err, log_err = yql_utils.read_res_file(log_file)

        if verbose:
            yql_utils.log('QUERIES:')
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
            None,
            proc_result,
            None,
        )
