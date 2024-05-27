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


class WorkloadOptions:
    def __init__(self, path_option: str) -> None:
        self.path_option = path_option


_workload_options: dict[WorkloadType, WorkloadOptions] = {
    WorkloadType.Clickbench: WorkloadOptions('table'),
    WorkloadType.TPC_H: WorkloadOptions('path'),
}


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

    class WorkloadRunResult:
        def __init__(
            self, stats: dict[str, dict[str, any]] = {}, query_out: str = None, stdout: str = None, stderr: str = None,
            error_message: str | None = None
        ) -> None:
            self.stats = stats
            self.query_out = query_out if str != '' else None
            self.stdout = stdout if stdout != '' else None
            self.stderr = stderr if stderr != '' else None
            self.success = error_message is None
            self.error_message = '' if self.success else error_message

    @staticmethod
    def workload_run(type: WorkloadType, path: str, query_num: int, iterations: int = 5,
                     timeout: float = 100.) -> YdbCliHelper.WorkloadRunResult:
        try:
            json_path = yatest.common.work_path(f'q{query_num}.json')
            qout_path = yatest.common.work_path(f'q{query_num}.out')
            cmd = YdbCliHelper.get_cli_command() + [
                '-e', YdbCluster.ydb_endpoint,
                '-d', f'/{YdbCluster.ydb_database}',
                'workload', str(type), 'run',
                f'--{_workload_options.get(type).path_option}', path,
                '--json', json_path,
                '--output', qout_path,
                '--executer', 'generic',
                '--include', str(query_num),
                '--iterations', str(iterations),
                '--query-settings', "PRAGMA ydb.HashJoinMode='grace';",
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
            return YdbCliHelper.WorkloadRunResult(
                stats=stats,
                query_out=qout,
                stdout=exec.stdout,
                stderr=exec.stderr,
                error_message=err
            )
        except BaseException as e:
            return YdbCliHelper.WorkloadRunResult(error_message=str(e))
