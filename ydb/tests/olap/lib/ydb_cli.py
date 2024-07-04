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
        def __init__(self, plan: dict | None = None, table: str | None = None, ast: str | None = None) -> None:
            self.plan = plan
            self.table = table
            self.ast = ast

    class WorkloadRunResult:
        def __init__(
            self, stats: dict[str, dict[str, any]] = {}, query_out: str = None, stdout: str = None, stderr: str = None,
            error_message: str | None = None, plan: YdbCliHelper.QueuePlan | None = None
        ) -> None:
            self.stats = stats
            self.query_out = query_out if str != '' else None
            self.stdout = stdout if stdout != '' else None
            self.stderr = stderr if stderr != '' else None
            self.success = error_message is None
            self.error_message = '' if self.success else error_message
            self.plan = plan

    @staticmethod
    def workload_run(type: WorkloadType, path: str, query_num: int, iterations: int = 5,
                     timeout: float = 100.) -> YdbCliHelper.WorkloadRunResult:
        try:
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
                '--query-settings', "PRAGMA ydb.HashJoinMode='grace';" + get_external_param('query-prefix', ''),
                '--plan', plan_path
            ]
            err = None
            try:
                exec: yatest.common.process._Execution = yatest.common.process.execute(cmd, wait=False, check_exit_code=False)
                exec.wait(check_exit_code=False, timeout=timeout)
                if exec.returncode != 0:
                    err = f'Invalid return code: {exec.returncode} instesd 0.'
            except (yatest.common.process.TimeoutError, yatest.common.process.ExecutionTimeoutError):
                err = f'Timeout {timeout}s expeared.'
            stats = {}
            if (os.path.exists(json_path)):
                with open(json_path, 'r') as r:
                    for signal in json.load(r):
                        q = signal['labels']['query']
                        if q not in stats:
                            stats[q] = {}
                        stats[q][signal['sensor']] = signal['value']

            if (os.path.exists(qout_path)):
                with open(qout_path, 'r') as r:
                    qout = r.read()
            plan = YdbCliHelper.QueuePlan()
            if (os.path.exists(plan_path + '.json')):
                with open(plan_path + '.json') as f:
                    plan.plan = json.load(f)
            if (os.path.exists(plan_path + '.table')):
                with open(plan_path + '.table') as f:
                    plan.table = f.read()
            if (os.path.exists(plan_path + '.ast')):
                with open(plan_path + '.ast') as f:
                    plan.ast = f.read()

            return YdbCliHelper.WorkloadRunResult(
                stats=stats,
                query_out=qout,
                plan=plan,
                stdout=exec.stdout,
                stderr=exec.stderr,
                error_message=err
            )
        except BaseException as e:
            return YdbCliHelper.WorkloadRunResult(error_message=str(e))
