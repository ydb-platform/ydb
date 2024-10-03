from __future__ import annotations
from typing import Any, Optional
import yatest.common
import json
import os
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from enum import StrEnum
from time import time
from types import TracebackType


class WorkloadType(StrEnum):
    Clickbench = 'clickbench'
    TPC_H = 'tpch'
    TPC_DS = 'tpcds'


class YdbCliHelper:
    @staticmethod
    def get_cli_command() -> list[str]:
        cli = get_external_param('ydb-cli', 'git')
        if cli == 'git':
            return [yatest.common.work_path('ydb')]
        elif cli == 'main':
            path = os.path.join(yatest.common.context.project_path, '../../../apps/ydb/ydb')
            return [yatest.common.binary_path(path)]
        else:
            return [cli]

    class QueryPlan:
        def __init__(self, plan: dict | None = None, table: str | None = None, ast: str | None = None, svg: str | None = None) -> None:
            self.plan = plan
            self.table = table
            self.ast = ast
            self.svg = svg

    class WorkloadRunResult:
        def __init__(self):
            self.stats: dict[str, dict[str, Any]] = {}
            self.query_out: Optional[str] = None
            self.stdout: Optional[str] = None
            self.stderr: Optional[str] = None
            self.error_message: str = ''
            self.plans: Optional[list[YdbCliHelper.QueryPlan]] = None
            self.explain_plan: Optional[YdbCliHelper.QueryPlan] = None
            self.errors_by_iter: dict[int, str] = {}
            self.traceback: Optional[TracebackType] = None

        @property
        def success(self) -> bool:
            return len(self.error_message) == 0

    class WorkloadProcessor:
        def __init__(self,
                     workload_type: WorkloadType,
                     db_path: str,
                     query_num: int,
                     iterations: int,
                     timeout: float,
                     check_canonical: bool):
            def _get_output_path(ext: str) -> str:
                return yatest.common.work_path(f'q{query_num}.{ext}')

            self.result = YdbCliHelper.WorkloadRunResult()
            self.workload_type = workload_type
            self.db_path = db_path
            self.query_num = query_num
            self.iterations = iterations
            self.timeout = timeout
            self.check_canonical = check_canonical
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

        def _process_returncode(self, returncode, stderr: str) -> None:
            begin_str = f'{self.query_num}:'
            end_str = 'Query text:'
            iter_str = 'iteration '
            begin_pos = stderr.find(begin_str)
            if begin_pos >= 0:
                while True:
                    begin_pos = stderr.find(iter_str, begin_pos)
                    if begin_pos < 0:
                        break
                    begin_pos += len(iter_str)
                    end_pos = stderr.find('\n', begin_pos)
                    if end_pos < 0:
                        iter = int(stderr[begin_pos:])
                        begin_pos = len(stderr) - 1
                    else:
                        iter = int(stderr[begin_pos:end_pos])
                        begin_pos = end_pos + 1
                    end_pos = stderr.find(end_str, begin_pos)
                    msg = (stderr[begin_pos:] if end_pos < 0 else stderr[begin_pos:end_pos]).strip()
                    self.result.errors_by_iter[iter] = msg
                    self._add_error(f'Iteration {iter}: {msg}')
            if returncode != 0 and len(self.result.errors_by_iter) == 0:
                self._add_error(f'Invalid return code: {returncode} instead 0.')

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
            return result

        def _load_plans(self) -> None:
            self.result.plans = [self._load_plan(str(i)) for i in range(self.iterations)]
            self.result.explain_plan = self._load_plan('explain')

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
                self._add_error('There is diff in query results')

        def _load_query_out(self) -> None:
            if (os.path.exists(self._query_output_path)):
                with open(self._query_output_path, 'r') as r:
                    self.result.query_out = r.read()

        @staticmethod
        def _get_nodes_info() -> dict[str, dict[str, int]]:
            nodes, _ = YdbCluster.get_cluster_nodes()
            return {
                n['SystemState']['Host']: {
                    'start_time': int(int(n['SystemState'].get('StartTime', time() * 1000)) / 1000)
                }
                for n in nodes
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

        def _get_cmd(self) -> list[str]:
            cmd = YdbCliHelper.get_cli_command() + [
                '-e', YdbCluster.ydb_endpoint,
                '-d', f'/{YdbCluster.ydb_database}',
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
            query_preffix = get_external_param('query-prefix', '')
            if query_preffix:
                cmd += ['--query-settings', query_preffix]
            if self.check_canonical:
                cmd.append('--check-canonical')
            return cmd

        def _exec_cli(self) -> None:
            process = yatest.common.process.execute(self._get_cmd(), check_exit_code=False)
            self.result.stdout = process.stdout.decode('utf-8', 'replace')
            self.result.stderr = process.stderr.decode('utf-8', 'replace')
            self._process_returncode(process.returncode, self.result.stderr)

        def process(self) -> YdbCliHelper.WorkloadRunResult:
            try:
                wait_error = YdbCluster.wait_ydb_alive(300, self.db_path)
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
                     timeout: float = 100., check_canonical: bool = False) -> YdbCliHelper.WorkloadRunResult:
        return YdbCliHelper.WorkloadProcessor(workload_type, path, query_num, iterations, timeout, check_canonical).process()
