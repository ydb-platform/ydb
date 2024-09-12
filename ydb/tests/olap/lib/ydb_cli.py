from __future__ import annotations
import yatest.common
import json
import os
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import get_external_param
from enum import StrEnum


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

    class QueuePlan:
        def __init__(self, plan: dict | None = None, table: str | None = None, ast: str | None = None, svg: str | None = None) -> None:
            self.plan = plan
            self.table = table
            self.ast = ast
            self.svg = svg

    class WorkloadRunResult:
        def __init__(
            self, stats: dict[str, dict[str, any]] = {}, query_out: str = None, stdout: str = None, stderr: str = None,
            error_message: str | None = None, plans: list[YdbCliHelper.QueuePlan] | None = None,
            errors_by_iter: dict[int, str] | None = None
        ) -> None:
            self.stats = stats
            self.query_out = query_out if str != '' else None
            self.stdout = stdout if stdout != '' else None
            self.stderr = stderr if stderr != '' else None
            self.success = error_message is None
            self.error_message = '' if self.success else error_message
            self.plans = plans
            self.errors_by_iter = errors_by_iter

    @staticmethod
    def workload_run(type: WorkloadType, path: str, query_num: int, iterations: int = 5,
                     timeout: float = 100.) -> YdbCliHelper.WorkloadRunResult:
        def _try_extract_error_message(stderr: str) -> str:
            result = {}
            begin_str = f'{query_num}:'
            end_str = 'Query text:'
            iter_str = 'iteration '
            begin_pos = stderr.find(begin_str)
            if begin_pos < 0:
                return result
            while True:
                begin_pos = stderr.find(iter_str, begin_pos)
                if begin_pos < 0:
                    return result
                begin_pos += len(iter_str)
                end_pos = stderr.find('\n', begin_pos)
                if end_pos < 0:
                    iter = int(stderr[begin_pos:])
                    begin_pos = len(stderr) - 1
                else:
                    iter = int(stderr[begin_pos:end_pos])
                    begin_pos = end_pos + 1
                end_pos = stderr.find(end_str, begin_pos)
                if end_pos < 0:
                    result[iter] = stderr[begin_pos:].strip()
                else:
                    result[iter] = stderr[begin_pos:end_pos].strip()

        errors_by_iter = {}
        try:
            wait_error = YdbCluster.wait_ydb_alive(300, path)
            if wait_error is not None:
                return YdbCliHelper.WorkloadRunResult(error_message=f'Ydb cluster is dead: {wait_error}')

            json_path = yatest.common.work_path(f'q{query_num}.json')
            qout_path = yatest.common.work_path(f'q{query_num}.out')
            plan_path = yatest.common.work_path(f'q{query_num}.plan')
            cmd = YdbCliHelper.get_cli_command() + [
                '-e', YdbCluster.ydb_endpoint,
                '-d', f'/{YdbCluster.ydb_database}',
                'workload', str(type), '--path', path, 'run',
                '--json', json_path,
                '--output', qout_path,
                '--executer', 'generic',
                '--include', str(query_num),
                '--iterations', str(iterations),
                '--plan', plan_path,
                '--verbose'
            ]
            query_preffix = get_external_param('query-prefix', '')
            if query_preffix:
                cmd += ['--query-settings', query_preffix]
            err = None
            try:
                exec: yatest.common.process._Execution = yatest.common.process.execute(cmd, wait=False, check_exit_code=False)
                exec.wait(check_exit_code=False, timeout=timeout)
                if exec.returncode != 0:
                    errors_by_iter = _try_extract_error_message(exec.stderr.decode('utf-8'))
                    err = '\n\n'.join([f'Iteration {i}: {e}' for i, e in errors_by_iter.items()])
                    if not err:
                        err = f'Invalid return code: {exec.returncode} instesd 0.'
            except (yatest.common.process.TimeoutError, yatest.common.process.ExecutionTimeoutError):
                err = f'Timeout {timeout}s expeared.'
            stats = {}
            if (os.path.exists(json_path)):
                with open(json_path, 'r') as r:
                    json_data = r.read()
                if json_data:
                    for signal in json.loads(json_data):
                        q = signal['labels']['query']
                        if q not in stats:
                            stats[q] = {}
                        stats[q][signal['sensor']] = signal['value']

            if (os.path.exists(qout_path)):
                with open(qout_path, 'r') as r:
                    qout = r.read()
            plans = []
            for i in range(iterations):
                plans.append(YdbCliHelper.QueuePlan())
                pp = f'{plan_path}.{i}'
                if (os.path.exists(f'{pp}.json')):
                    with open(f'{pp}.json') as f:
                        plans[i].plan = json.load(f)
                if (os.path.exists(f'{pp}.table')):
                    with open(f'{pp}.table') as f:
                        plans[i].table = f.read()
                if (os.path.exists(f'{pp}.ast')):
                    with open(f'{pp}.ast') as f:
                        plans[i].ast = f.read()
                if (os.path.exists(f'{pp}.svg')):
                    with open(f'{pp}.svg') as f:
                        plans[i].svg = f.read()

            return YdbCliHelper.WorkloadRunResult(
                stats=stats,
                query_out=qout,
                plans=plans,
                stdout=exec.stdout.decode('utf-8'),
                stderr=exec.stderr.decode('utf-8'),
                error_message=err,
                errors_by_iter=errors_by_iter
            )
        except BaseException as e:
            return YdbCliHelper.WorkloadRunResult(error_message=str(e))
