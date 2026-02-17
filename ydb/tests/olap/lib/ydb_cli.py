from __future__ import annotations
from typing import Any, Optional
import allure
import yatest.common
import json
import os
import re
import subprocess
import logging
import ydb.tests.olap.lib.remote_execution as remote_execution
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from enum import StrEnum, Enum
from types import TracebackType
from time import time, sleep
from hashlib import md5


class WorkloadType(StrEnum):
    Clickbench = 'clickbench'
    TPC_H = 'tpch'
    TPC_DS = 'tpcds'
    EXTERNAL = 'query'


class TxMode(StrEnum):
    SerializableRW = 'serializable-rw'
    SnapshotRW = 'snapshot-rw'


class CheckCanonicalPolicy(Enum):
    NO = 0
    WARNING = 1
    ERROR = 2


class YdbCliHelper:
    @staticmethod
    def get_cli_path() -> str:
        cli = get_external_param('ydb-cli', 'main')
        if cli == 'git':
            return yatest.common.work_path('ydb')
        if cli == 'main':
            return yatest.common.binary_path(os.getenv('YDB_CLI_BINARY'))
        return cli

    @staticmethod
    def get_cli_command(cli_path: str = '') -> list[str]:
        if not cli_path:
            cli_path = YdbCliHelper.get_cli_path()
        result = [
            cli_path,
            '-e', YdbCluster.ydb_endpoint,
            '-d', f'/{YdbCluster.ydb_database}'
        ]
        if YdbCluster.ydb_iam_file:
            result += ['--sa-key-file', YdbCluster.ydb_iam_file]
        return result

    class QueryPlan:
        def __init__(self) -> None:
            self.plan: dict = None
            self.table: str = None
            self.ast: str = None
            self.svg: str = None
            self.stats: str = None

    class Iteration:
        def __init__(self):
            self.final_plan: Optional[YdbCliHelper.QueryPlan] = None
            self.in_progress_plan: Optional[YdbCliHelper.QueryPlan] = None
            self.error_message: Optional[str] = None
            self.time: Optional[float] = None

        def get_error_class(self) -> str:
            msg_to_class = {
                'Deadline Exceeded': 'timeout',
                'Request timeout': 'timeout',
                'Query did not complete within specified timeout': 'timeout',
                'There is diff': 'diff'
            }
            for msg, cl in msg_to_class.items():
                if self.error_message and self.error_message.find(msg) >= 0:
                    return cl
            if self.error_message:
                return 'other'
            return ''

    class WorkloadRunResult:
        def __init__(self):
            self._stats: dict[str, dict[str, Any]] = {}
            self.query_out: Optional[str] = None
            self.stdout: str = ''
            self.stderr: str = ''
            self.error_message: str = ''
            self.warning_message: str = ''
            self.errors: list[str] = []
            self.warnings: list[str] = []
            self.explain = YdbCliHelper.Iteration()
            self.iterations: dict[int, YdbCliHelper.Iteration] = {}
            self.traceback: Optional[TracebackType] = None
            self.start_time = time()

        def merge(self, *others: list[YdbCliHelper.WorkloadRunResult]) -> YdbCliHelper.WorkloadRunResult:
            def not_empty(x):
                return bool(x)

            results = [r for r in filter(lambda x: x is not None, others)]
            self.start_time = min([r.start_time for r in results])
            self.query_out = '\n'.join(filter(not_empty, [r.query_out for r in results]))
            self.stdout = '\n'.join(filter(not_empty, [r.stdout for r in results]))
            self.stderr = '\n'.join(filter(not_empty, [r.stderr for r in results]))
            self.error_message = '\n'.join(filter(not_empty, [r.error_message for r in results]))
            self.warning_message = '\n'.join(filter(not_empty, [r.warning_message for r in results]))
            for r in results:
                self._stats.update(r._stats)
                self.errors.extend(r.errors)
                self.warnings.extend(r.warnings)
                self.explain = r.explain
                if self.traceback is None and r.traceback is not None:
                    self.traceback = r.traceback
                for num, iter in r.iterations.items():
                    while num in self.iterations:
                        num = max(num + 1, len(self.iterations))
                    self.iterations[num] = iter
            return self

        @property
        def success(self) -> bool:
            return len(self.errors) == 0

        def get_stats(self, test: str) -> dict[str, dict[str, Any]]:
            result = self._stats.get(test, {})
            result.update({
                'with_warnings': bool(self.warnings) or bool(self.warning_message),
                'with_errors': bool(self.errors) or bool(self.error_message),
                'errors': self.get_error_stats()
            })
            return result

        def add_stat(self, test: str, signal: str, value: Any) -> None:
            self._stats.setdefault(test, {})
            self._stats[test][signal] = value

        def get_error_stats(self):
            result = {}
            for iter in self.iterations.values():
                cl = iter.get_error_class()
                if cl:
                    result[cl] = True
            if len(result) == 0 and self.error_message:
                result['other'] = True
            if self.warning_message:
                result['warning'] = True
            return result

        def add_error(self, msg: Optional[str]) -> bool:
            if msg:
                self.errors.append(msg)
                if len(self.error_message) > 0:
                    self.error_message += f'\n\n{msg}'
                else:
                    self.error_message = msg
                return True
            return False

        def add_warning(self, msg: Optional[str]):
            if msg:
                self.warnings.append(msg)
                if len(self.warning_message) > 0:
                    self.warning_message += f'\n\n{msg}'
                else:
                    self.warning_message = msg
                return True
            return False

    class WorkloadRunner():
        def __init__(self,
                     workload_type: WorkloadType,
                     db_path: str,
                     query_names: list[str],
                     iterations: int,
                     timeout: float,
                     check_canonical: CheckCanonicalPolicy,
                     query_syntax: str,
                     scale: Optional[int],
                     query_prefix: Optional[str],
                     external_path: str,
                     threads: int,
                     user: str
                     ):
            self.result = YdbCliHelper.WorkloadRunResult()
            self.iterations = iterations
            self.check_canonical = check_canonical
            self.workload_type = workload_type
            self.db_path = db_path
            self.timeout = timeout
            self.query_syntax = query_syntax
            self.scale = scale
            self.query_prefix = query_prefix
            self.external_path = external_path
            self.threads = threads
            self.returncode = None
            self.stderr = None
            self.stdout = None
            if self.workload_type == WorkloadType.EXTERNAL and not self.external_path:
                self.__prefix = md5(','.join(query_names).encode()).hexdigest()
                self.__external_queries = query_names
                self.query_names = [f'Custom{i}' for i in range(len(query_names))]
            else:
                self.__prefix = md5(','.join(query_names).encode()).hexdigest() if len(query_names) != 1 else query_names[0]
                self.__external_queries = []
                self.query_names = query_names
            if user:
                self.__prefix += f'.{user}'
            self.__plan_path = f'{self.__prefix}.plan'
            self.__query_output_path = f'{self.__prefix}.result'
            self.json_path = f'{self.__prefix}.stats.json'
            self.user = user

        def get_plan_path(self, query_name: str, plan_name: Any) -> str:
            return f'{self.__plan_path}.{query_name}.{plan_name}'

        def get_query_output_path(self, query_name: str) -> str:
            return f'{self.__query_output_path}.{query_name}.out'

        def __get_cmd(self) -> list[str]:
            cmd = YdbCliHelper.get_cli_command()
            if self.user:
                cmd += ['--user', self.user, '--no-password']
            cmd += [
                'workload', str(self.workload_type), '--path', YdbCluster.get_tables_path(self.db_path)]
            cmd += ['run']
            if self.external_path:
                cmd += ['--suite-path', self.external_path]
            for q in self.__external_queries:
                cmd += ['--query', q]
            cmd += [
                '--json', self.json_path,
                '--output', self.__query_output_path,
                '--include', ','.join(self.query_names),
                '--iterations', str(self.iterations),
                '--plan', self.__plan_path,
                '--global-timeout', f'{self.timeout}s',
                '--verbose'
            ]
            if self.query_prefix:
                cmd += ['--query-prefix', self.query_prefix]
            if self.check_canonical != CheckCanonicalPolicy.NO:
                cmd.append('--check-canonical')
            if self.query_syntax:
                cmd += ['--syntax', self.query_syntax]
            if self.scale is not None and self.scale > 0:
                cmd += ['--scale', str(self.scale)]
            if self.threads > 0:
                cmd += ['--threads', str(self.threads)]
            return cmd

        def run(self) -> bool:
            try:
                if not self.result.add_error(YdbCluster.wait_ydb_alive(int(os.getenv('WAIT_CLUSTER_ALIVE_TIMEOUT', 20 * 60)), self.db_path)):
                    if os.getenv('SECRET_REQUESTS', '') == '1':
                        with open(f'{self.__prefix}.stdout', "wt") as sout, open(f'{self.__prefix}.stderr', "wt") as serr:
                            cmd = self.__get_cmd()
                            logging.info(f'Run ydb cli: {cmd}')
                            process = subprocess.run(cmd, check=False, text=True, stdout=sout, stderr=serr)
                        with open(f'{self.__prefix}.stdout', "rt") as sout, open(f'{self.__prefix}.stderr', "rt") as serr:
                            self.stderr = serr.read()
                            self.stdout = sout.read()
                    else:
                        process = yatest.common.process.execute(self.__get_cmd(), check_exit_code=False, text=True)
                        self.stderr = process.stderr
                        self.stdout = process.stdout
                    self.returncode = process.returncode
            except BaseException as e:
                self.result.add_error(str(e))
                self.result.traceback = e.__traceback__
            return self.result.success

    class WorkloadResultParser:
        def __init__(self, runner: YdbCliHelper.WorkloadRunner, query_name: str, queries: list[str]):
            self.result = YdbCliHelper.WorkloadRunResult()
            self.result.start_time = runner.result.start_time
            self.__queries = queries
            self.__query_name = query_name
            self.__runner = runner
            self.__process()

        def __init_iter(self, iter_num: int) -> None:
            if iter_num not in self.result.iterations:
                self.result.iterations[iter_num] = YdbCliHelper.Iteration()

        def __parse_stderr(self) -> None:
            self.result.stderr = self.__runner.stderr if self.__runner.stderr else ''
            begin_str = f'{self.__query_name}:'
            end_str = 'Query text:'
            iter_str = 'iteration '
            begin_pos = self.result.stderr.find(begin_str)
            if begin_pos >= 0:
                query_end_pos = -1
                for q in self.__queries:
                    end_pos = self.result.stderr.find(q, begin_pos + len(begin_str))
                    if end_pos >= 0 and (query_end_pos < 0 or query_end_pos > end_pos):
                        query_end_pos = end_pos
                while begin_pos < query_end_pos or query_end_pos < 0:
                    begin_pos = self.result.stderr.find(iter_str, begin_pos)
                    if begin_pos < 0:
                        break
                    begin_pos += len(iter_str)
                    end_pos = self.result.stderr.find('\n', begin_pos)
                    if end_pos < 0:
                        iter = int(self.result.stderr[begin_pos:])
                        begin_pos = len(self.result.stderr) - 1
                    else:
                        iter = int(self.result.stderr[begin_pos:end_pos])
                        begin_pos = end_pos + 1
                    end_pos = self.result.stderr.find(end_str, begin_pos)
                    if query_end_pos >= 0:
                        end_pos = query_end_pos if end_pos < 0 else min(query_end_pos, end_pos)
                    msg = (self.result.stderr[begin_pos:] if end_pos < 0 else self.result.stderr[begin_pos:end_pos]).strip()
                    self.__init_iter(iter)
                    self.result.iterations[iter].error_message = msg
                    self.result.add_error(f'Iteration {iter}: {msg}')

        def __load_plan(self, name: Any) -> YdbCliHelper.QueryPlan:
            result = YdbCliHelper.QueryPlan()
            pp = self.__runner.get_plan_path(self.__query_name, name)
            if (os.path.exists(f'{pp}.json')):
                with open(f'{pp}.json') as f:
                    result.plan = json.load(f)
            if (os.path.exists(f'{pp}.table')):
                with open(f'{pp}.table') as f:
                    result.table = f.read()
            if (os.path.exists(f'{pp}.ast')):
                with open(f'{pp}.ast') as f:
                    result.ast = f.read()
            if (os.path.exists(f'{pp}.svg')):
                with open(f'{pp}.svg') as f:
                    result.svg = f.read()
            if (os.path.exists(f'{pp}.stats')):
                with open(f'{pp}.stats') as f:
                    result.stats = f.read()
            return result

        def __load_plans(self) -> None:
            for i in range(self.__runner.iterations):
                self.__init_iter(i)
                self.result.iterations[i].final_plan = self.__load_plan(i)
                self.result.iterations[i].in_progress_plan = self.__load_plan(f'{i}.in_progress')
            self.result.explain.final_plan = self.__load_plan('explain')

        def __load_stats(self):
            if not os.path.exists(self.__runner.json_path):
                return
            with open(self.__runner.json_path, 'r') as r:
                json_data = r.read()
            for signal in json.loads(json_data):
                self.result.add_stat(signal['labels']['query'], signal['sensor'], signal['value'])
            if self.result.get_stats(f'{self.__query_name}').get("DiffsCount", 0) > 0:
                if self.__runner.check_canonical == CheckCanonicalPolicy.WARNING:
                    self.result.add_warning('There is diff in query results')
                else:
                    self.result.add_error('There is diff in query results')

        def __load_query_out(self) -> None:
            path = self.__runner.get_query_output_path(self.__query_name)
            if (os.path.exists(path)):
                with open(path, 'r') as r:
                    self.result.query_out = r.read()

        def __parse_stdout(self) -> None:
            self.result.stdout = self.__runner.stdout if self.__runner.stdout else ''
            begin_pos = self.result.stdout.find(f'{self.__query_name}')
            if begin_pos < 0:
                return
            max_iter = -1
            for line in self.result.stdout[begin_pos:].splitlines():
                if line == '':
                    continue
                m = re.search(r'iteration ([0-9]*):\s*ok\s*([\.0-9]*)s', line)
                if m is not None:
                    iter = int(m.group(1))
                    if iter <= max_iter:
                        break
                    max_iter = iter
                    self.__init_iter(iter)
                    self.result.iterations[iter].time = float(m.group(2))

        def __process(self) -> YdbCliHelper.WorkloadRunResult:
            self.__parse_stderr()
            self.__parse_stdout()
            self.__load_stats()
            self.__load_query_out()
            self.__load_plans()

    @staticmethod
    def workload_run(workload_type: WorkloadType, path: str, query_names: list[str], iterations: int = 5,
                     timeout: float = 100., check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.NO, query_syntax: str = '',
                     scale: Optional[int] = None, query_prefix=None, external_path='', threads: int = 0,
                     users=['']) -> dict[str, YdbCliHelper.WorkloadRunResult]:
        runners = [YdbCliHelper.WorkloadRunner(
            workload_type,
            path,
            query_names,
            iterations,
            timeout,
            check_canonical,
            query_syntax,
            scale,
            query_prefix=query_prefix,
            external_path=external_path,
            threads=threads,
            user=u,
        ) for u in users]
        extended_query_names = query_names + ["Sum", "Avg", "GAvg"]

        def __get_result(runner: YdbCliHelper.WorkloadRunner):
            if runner.run():
                return {q: YdbCliHelper.WorkloadResultParser(runner, q, extended_query_names).result for q in extended_query_names}
            return {q: runner.result for q in extended_query_names}

        with ThreadPoolExecutor(max_workers=len(runners)) as executor:
            results = as_completed([executor.submit(__get_result, r) for r in runners])
        results_by_q = [r.result() for r in results]
        return {q: YdbCliHelper.WorkloadRunResult().merge(*[r.get(q) for r in results_by_q]) for q in extended_query_names}

    @classmethod
    def get_remote_cli_path(cls, host: str = ''):
        if not host:
            host = YdbCluster.get_client_host()
        return remote_execution.get_remote_tmp_path(host, 'ydb_cli', os.path.basename(cls.get_cli_path()))

    @classmethod
    @allure.step
    def deploy_remote_cli(cls, host: str = ''):
        if not host:
            host = YdbCluster.get_client_host()
        result = remote_execution.deploy_binary(cls.get_cli_path(), host, os.path.dirname(cls.get_remote_cli_path()))
        assert result.get('success', False), f"host: {host}, bin: {cls.get_cli_path()}, path: {result.get('path')}, error: {result.get('error')}"

    @classmethod
    @allure.step
    def clear_tpcc(cls, path: str):
        yatest.common.process.execute(cls.get_cli_command() + ['workload', 'tpcc', '-p', YdbCluster.get_tables_path(path), 'clean'])

    @classmethod
    @allure.step
    def init_tpcc(cls, path: str, warehouses: int):
        yatest.common.process.execute(cls.get_cli_command() + ['workload', 'tpcc', '-p', YdbCluster.get_tables_path(path), 'init', '--warehouses', str(warehouses)])

    @classmethod
    @allure.step
    def import_data_tpcc(cls, path: str, warehouses: int):
        cmd = cls.get_cli_command(cls.get_remote_cli_path()) + ['workload', 'tpcc', '-p', YdbCluster.get_tables_path(path), 'import', '--no-tui', '--warehouses', str(warehouses)]
        with remote_execution.LongRemoteExecution(YdbCluster.get_client_host(), *cmd) as exec:
            while exec.is_running():
                sleep(10)
            assert exec.return_code == 0, f'import fails with code {exec.return_code}\nerrors: {exec.stderr}\noutput: {exec.stdout}'

    @classmethod
    @allure.step
    def run_tpcc(cls, path: str, bench_time: float, warehouses: int = 10, threads: int = 0, warmup: float = 0.,
                 tx_mode: TxMode = TxMode.SerializableRW, users=['']) -> dict[str, YdbCliHelper.WorkloadRunResult]:
        executions = []
        for user in users:
            cmd = cls.get_cli_command(cls.get_remote_cli_path())
            if user:
                cmd += ['--user', user, '--no-password']
            cmd += ['workload', 'tpcc', '--path', YdbCluster.get_tables_path(path), 'run', '--no-tui', '--format', 'Json', '--tx-mode', str(tx_mode), '--highres-histogram']
            if warmup > 0:
                cmd += ['--warmup', f'{warmup}s']
            cmd += ['--time', f'{bench_time}s', '--warehouses', str(warehouses)]
            if threads:
                cmd += ['--threads', str(threads)]

            executions.append((user, remote_execution.LongRemoteExecution(YdbCluster.get_client_host(), *cmd)))

        start_time = time()
        with ExitStack() as stack:
            for _, exec in executions:
                stack.enter_context(exec)
            while any([exec.is_running() for _, exec in executions]):
                sleep(10)

        results = {}
        for user, exec in executions:
            res = YdbCliHelper.WorkloadRunResult()
            res.start_time = start_time
            try:
                res.stdout = exec.stdout
                res.stderr = exec.stderr
                if exec.return_code != 0:
                    res.add_error(f'ydb cli failed with code {exec.return_code}.')
                    ans = {}
                else:
                    ans = json.loads(res.stdout)
                summary = ans.get('summary', {})
                res.add_stat('test', 'tpcc_json', ans)
                res.add_stat('test', 'tpcc_tpmc', summary.get('tpmc', 0))
                res.add_stat('test', 'tpcc_warehouses', summary.get('warehouses', 0))
                res.add_stat('test', 'tpcc_efficiency', summary.get('efficiency', 0))
                res.add_stat('test', 'tpcc_time_seconds', summary.get('time_seconds', 0))
                for tr, stats in ans.get('transactions', {}).items():
                    res.add_stat('test', f'tpcc_{tr}_ok_count', stats.get('ok_count', 0))
                    res.add_stat('test', f'tpcc_{tr}_failed_count', stats.get('failed_count', 0))
                    for p, t in stats.get('percentiles', {}).items():
                        res.add_stat('test', f'tpcc_{tr}_perc_{p.replace(".", "_")}', t)
            except BaseException as e:
                res.add_error(str(e))
            results[user] = res

        return results
