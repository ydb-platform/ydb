from __future__ import annotations
from typing import Any, Optional
import yatest.common
import json
import os
import re
import subprocess
import logging
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from enum import StrEnum, Enum
from types import TracebackType
from time import time
from hashlib import md5


class WorkloadType(StrEnum):
    Clickbench = 'clickbench'
    TPC_H = 'tpch'
    TPC_DS = 'tpcds'
    EXTERNAL = 'query'


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
    def get_cli_command() -> list[str]:
        return [
            YdbCliHelper.get_cli_path(),
            '-e', YdbCluster.ydb_endpoint,
            '-d', f'/{YdbCluster.ydb_database}'
        ]

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
            self.plans: Optional[list[YdbCliHelper.QueryPlan]] = None
            self.explain = YdbCliHelper.Iteration()
            self.iterations: dict[int, YdbCliHelper.Iteration] = {}
            self.traceback: Optional[TracebackType] = None
            self.start_time = time()

        @property
        def success(self) -> bool:
            return len(self.errors) == 0

        def get_stats(self, test: str) -> dict[str, dict[str, Any]]:
            result = self._stats.get(test, {})
            result.update({
                'with_warnings': bool(self.warnings) or bool(self.warning_message),
                'with_warrnings': bool(self.warnings) or bool(self.warning_message),
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
                     float_mode: str,
                     ):
            self.result = YdbCliHelper.WorkloadRunResult()
            self.iterations = iterations
            self.check_canonical = check_canonical
            self.workload_type = workload_type
            self.db_path = db_path
            self.query_names = query_names
            self.timeout = timeout
            self.query_syntax = query_syntax
            self.scale = scale
            self.query_prefix = query_prefix
            self.external_path = external_path
            self.threads = threads
            self.returncode = None
            self.stderr = None
            self.stdout = None
            self.__prefix = md5(','.join(query_names).encode()).hexdigest() if len(query_names) != 1 else query_names[0]
            self.__plan_path = f'{self.__prefix}.plan'
            self.__query_output_path = f'{self.__prefix}.result'
            self.json_path = f'{self.__prefix}.stats.json'
            self.float_mode = float_mode

        def get_plan_path(self, query_name: str, plan_name: Any) -> str:
            return f'{self.__plan_path}.{query_name}.{plan_name}'

        def get_query_output_path(self, query_name: str) -> str:
            return f'{self.__query_output_path}.{query_name}.out'

        def __get_cmd(self) -> list[str]:
            cmd = YdbCliHelper.get_cli_command() + [
                'workload', str(self.workload_type), '--path', YdbCluster.get_tables_path(self.db_path)]
            cmd += ['run']
            if self.external_path:
                cmd += ['--suite-path', self.external_path]
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
            if self.float_mode:
                cmd += ['--float-mode', self.float_mode]
            return cmd

        def run(self) -> YdbCliHelper.WorkloadRunResult:
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
        def __init__(self, runner: YdbCliHelper.WorkloadRunner, query_name: str):
            self.result = YdbCliHelper.WorkloadRunResult()
            self.result.start_time = runner.result.start_time
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
                while True:
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
                    msg = (self.result.stderr[begin_pos:] if end_pos < 0 else self.result.stderr[begin_pos:end_pos]).strip()
                    self.__init_iter(iter)
                    self.result.iterations[iter].error_message = msg
                    self.result.add_error(f'Iteration {iter}: {msg}')

        def __process_returncode(self) -> None:
            if self.__runner.returncode != 0 and not self.result.error_message and not self.result.warning_message:
                self.result.add_error(f'Invalid return code: {self.__runner.returncode} instead 0. stderr: {self.result.stderr}')

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
            self.__process_returncode()

    @staticmethod
    def workload_run(workload_type: WorkloadType, path: str, query_names: list[str], iterations: int = 5,
                     timeout: float = 100., check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.NO, query_syntax: str = '',
                     scale: Optional[int] = None, query_prefix=None, external_path='', threads: int = 0, float_mode: str = '') -> dict[str, YdbCliHelper.WorkloadRunResult]:
        runner = YdbCliHelper.WorkloadRunner(
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
            float_mode=float_mode,
        )
        extended_query_names = query_names + ["Sum", "Avg", "GAvg"]
        if runner.run():
            return {q: YdbCliHelper.WorkloadResultParser(runner, q).result for q in extended_query_names}
        return {q: runner.result for q in extended_query_names}
