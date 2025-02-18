from __future__ import annotations
from typing import Any, Optional
import yatest.common
import json
import os
import re
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from enum import StrEnum, Enum
from types import TracebackType
from time import time


class WorkloadType(StrEnum):
    Clickbench = 'clickbench'
    TPC_H = 'tpch'
    TPC_DS = 'tpcds'


class CheckCanonicalPolicy(Enum):
    NO = 0
    WARNING = 1
    ERROR = 2


class YdbCliHelper:
    @staticmethod
    def get_cli_command() -> list[str]:
        args = [
            '-e', YdbCluster.ydb_endpoint,
            '-d', f'/{YdbCluster.ydb_database}'
        ]
        cli = get_external_param('ydb-cli', 'main')
        if cli == 'git':
            return [yatest.common.work_path('ydb')] + args
        elif cli == 'main':
            return [yatest.common.binary_path(os.getenv('YDB_CLI_BINARY'))] + args
        else:
            return [cli] + args

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
            self.stats: dict[str, dict[str, Any]] = {}
            self.query_out: Optional[str] = None
            self.stdout: str = ''
            self.stderr: str = ''
            self.error_message: str = ''
            self.warning_message: str = ''
            self.plans: Optional[list[YdbCliHelper.QueryPlan]] = None
            self.explain = YdbCliHelper.Iteration()
            self.iterations: dict[int, YdbCliHelper.Iteration] = {}
            self.traceback: Optional[TracebackType] = None
            self.start_time = time()

        @property
        def success(self) -> bool:
            return len(self.error_message) == 0

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

    class WorkloadProcessor:
        def __init__(self,
                     workload_type: WorkloadType,
                     db_path: str,
                     query_num: int,
                     iterations: int,
                     timeout: float,
                     check_canonical: CheckCanonicalPolicy,
                     query_syntax: str,
                     scale: Optional[int],
                     query_prefix: Optional[str]):
            def _get_output_path(ext: str) -> str:
                return yatest.common.test_output_path(f'q{query_num}.{ext}')

            self.result = YdbCliHelper.WorkloadRunResult()
            self.workload_type = workload_type
            self.db_path = db_path
            self.query_num = query_num
            self.iterations = iterations
            self.timeout = timeout
            self.check_canonical = check_canonical
            self.query_syntax = query_syntax
            self.scale = scale
            self.query_prefix = query_prefix
            self._nodes_info: dict[str, dict[str, int]] = {}
            self._plan_path = _get_output_path('plan')
            self._query_output_path = _get_output_path('out')
            self._json_path = _get_output_path('json')

        def _add_error(self, msg: Optional[str]):
            if msg is not None and len(msg) > 0:
                if len(self.result.error_message) > 0:
                    self.result.error_message += f'\n\n{msg}'
                else:
                    self.result.error_message = msg

        def _add_warning(self, msg: Optional[str]):
            if msg is not None and len(msg) > 0:
                if len(self.result.warning_message) > 0:
                    self.result.warning_message += f'\n\n{msg}'
                else:
                    self.result.warning_message = msg

        def _init_iter(self, iter_num: int) -> None:
            if iter_num not in self.result.iterations:
                self.result.iterations[iter_num] = YdbCliHelper.Iteration()

        def _process_returncode(self, returncode) -> None:
            begin_str = f'{self.query_num}:'
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
                    self._init_iter(iter)
                    self.result.iterations[iter].error_message = msg
                    self._add_error(f'Iteration {iter}: {msg}')
            if returncode != 0 and len([x for x in filter(lambda x: x.error_message, self.result.iterations.values())]) == 0:
                self._add_error(f'Invalid return code: {returncode} instead 0. stderr: {self.result.stderr}')

        def _load_plan(self, name: str) -> YdbCliHelper.QueryPlan:
            result = YdbCliHelper.QueryPlan()
            pp = f'{self._plan_path}.{self.query_num}.{name}'
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

        def _load_plans(self) -> None:
            for i in range(self.iterations):
                self._init_iter(i)
                self.result.iterations[i].final_plan = self._load_plan(str(i))
                self.result.iterations[i].in_progress_plan = self._load_plan(f'{i}.in_progress')
            self.result.explain.final_plan = self._load_plan('explain')

        def _load_stats(self):
            if not os.path.exists(self._json_path):
                return
            with open(self._json_path, 'r') as r:
                json_data = r.read()
            for signal in json.loads(json_data):
                q = signal['labels']['query']
                if q not in self.result.stats:
                    self.result.stats[q] = {}
                self.result.stats[q][signal['sensor']] = signal['value']
            if self.result.stats.get(f'Query{self.query_num:02d}', {}).get("DiffsCount", 0) > 0:
                if self.check_canonical == CheckCanonicalPolicy.WARNING:
                    self._add_warning('There is diff in query results')
                else:
                    self._add_error('There is diff in query results')

        def _load_query_out(self) -> None:
            if (os.path.exists(self._query_output_path)):
                with open(self._query_output_path, 'r') as r:
                    self.result.query_out = r.read()

        @staticmethod
        def _get_nodes_info() -> dict[str, dict[str, Any]]:
            return {
                n.host: {
                    'start_time': n.start_time
                }
                for n in YdbCluster.get_cluster_nodes(db_only=True)
            }

        def _check_nodes(self):
            node_errors = []
            for node, info in self._get_nodes_info().items():
                if node in self._nodes_info:
                    if info['start_time'] > self._nodes_info[node]['start_time']:
                        node_errors.append(f'Node {node} was restarted')
                    self._nodes_info[node]['processed'] = True
            for node, info in self._nodes_info.items():
                if not info.get('processed', False):
                    node_errors.append(f'Node {node} is down')
            self._add_error('\n'.join(node_errors))

        def _parse_stdout(self):
            for line in self.result.stdout.splitlines():
                m = re.search(r'iteration ([0-9]*):\s*ok\s*([\.0-9]*)s', line)
                if m is not None:
                    iter = int(m.group(1))
                    self._init_iter(iter)
                    self.result.iterations[iter].time = float(m.group(2))

        def _get_cmd(self) -> list[str]:
            cmd = YdbCliHelper.get_cli_command() + [
                'workload', str(self.workload_type), '--path', self.db_path, 'run',
                '--json', self._json_path,
                '--output', self._query_output_path,
                '--executer', 'generic',
                '--include', str(self.query_num),
                '--iterations', str(self.iterations),
                '--plan', self._plan_path,
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
            return cmd

        def _exec_cli(self) -> None:
            process = yatest.common.process.execute(self._get_cmd(), check_exit_code=False)
            self.result.stdout = process.stdout.decode('utf-8', 'replace')
            self.result.stderr = process.stderr.decode('utf-8', 'replace')
            self._process_returncode(process.returncode)
            self._parse_stdout()

        def process(self) -> YdbCliHelper.WorkloadRunResult:
            try:
                wait_error = YdbCluster.wait_ydb_alive(20 * 60, self.db_path)
                if wait_error is not None:
                    self.result.error_message = wait_error
                else:
                    self._nodes_info = self._get_nodes_info()
                    self._exec_cli()
                    self._check_nodes()
                    self._load_stats()
                    self._load_query_out()
                    self._load_plans()
            except BaseException as e:
                self._add_error(str(e))
                self.result.traceback = e.__traceback__
            return self.result

    @staticmethod
    def workload_run(workload_type: WorkloadType, path: str, query_num: int, iterations: int = 5,
                     timeout: float = 100., check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.NO, query_syntax: str = '',
                     scale: Optional[int] = None, query_prefix=None) -> YdbCliHelper.WorkloadRunResult:
        return YdbCliHelper.WorkloadProcessor(
            workload_type,
            path,
            query_num,
            iterations,
            timeout,
            check_canonical,
            query_syntax,
            scale,
            query_prefix=query_prefix
        ).process()
