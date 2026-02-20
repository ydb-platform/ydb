from __future__ import annotations

import allure
import json
import os
import pytest
import time
import traceback
import yatest.common
import ydb
import ydb.tests.olap.lib.remote_execution as re

from . import tpch
from .conftest import LoadSuiteBase
from .clickbench import ClickbenchParallelBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper, WorkloadType
from ydb.tests.olap.lib.utils import get_external_param
from threading import Thread, Event
from datetime import datetime
from matplotlib import pyplot, dates
from os import getenv


class ResourcePool:
    def __init__(self, name: str, users: list[str], **kwargs):
        self.name = name
        self.users = users
        self.params = kwargs

    def get_create_users_sql(self) -> str:
        result = ''
        if len(self.users) == 0:
            return result
        if len(self.users) > 1:
            result = f'DROP GROUP IF EXISTS {self.name}_users;\n'
        result += f'DROP USER IF EXISTS {", ".join(self.users)};\n'
        for user in self.users:
            result += f'CREATE USER {user} PASSWORD NULL; GRANT ALL ON `/{YdbCluster.ydb_database}` TO {user};\n'
        if len(self.users) > 1:
            result += f'CREATE GROUP {self.name}_users WITH USER {", ".join(self.users)};\n'
        return result

    def get_create_sql(self) -> str:
        params = ','.join([f'\n    {k.upper()} = {v}' for k, v in self.params.items()])
        result = f'CREATE RESOURCE POOL {self.name} WITH({params}\n);\n'
        if len(self.users) == 0:
            return result
        if len(self.users) == 1:
            member = self.users[0]
        else:
            member = f'{self.name}_users'
        result += f'''
            CREATE RESOURCE POOL CLASSIFIER {self.name} WITH (
                RESOURCE_POOL = '{self.name}',
                MEMBER_NAME = '{member}'
            );
        '''
        return result


class WorkloadManagerBase(LoadSuiteBase):
    stop_checking = Event()
    signal_errors: list[tuple[datetime, str]] = []
    threads: int = 1

    @classmethod
    def check_signals_thread(cls) -> None:
        while not cls.stop_checking.wait(1.):
            try:
                error = cls.check_signals()
            except BaseException as e:
                error = f'{traceback.format_exception(e)}'
            if error:
                cls.signal_errors.append((datetime.now(), error))

    @classmethod
    def check_signals(cls) -> str:
        pass

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        pass

    @classmethod
    def get_users(cls) -> list[str]:
        result = set()
        for r in cls.get_resource_pools():
            result.update(r.users)
        return list(result)

    @classmethod
    def do_setup_class(cls):
        def _exists(path: str) -> bool:
            try:
                YdbCluster.get_ydb_driver().scheme_client.describe_path(f'/{YdbCluster.ydb_database}/{path}')
            except BaseException:
                return False
            return True

        cls.benchmark_setup()

        sessions_pool = ydb.QuerySessionPool(YdbCluster.get_ydb_driver())

        for pool in cls.get_resource_pools():
            if _exists('.metadata/workload_manager/classifiers/resource_pool_classifiers'):
                c = sessions_pool.execute_with_retries(f'''
                    SELECT
                        count(*)
                    FROM `.metadata/workload_manager/classifiers/resource_pool_classifiers`
                    WHERE name="{pool.name}"
                ''')
                if c[0].rows[0][0] > 0:
                    sessions_pool.execute_with_retries(f'DROP RESOURCE POOL CLASSIFIER {pool.name}')
            if _exists(f'.metadata/workload_manager/pools/{pool.name}'):
                sessions_pool.execute_with_retries(f'DROP RESOURCE POOL {pool.name}')

            sessions_pool.execute_with_retries(pool.get_create_users_sql())
            sessions_pool.execute_with_retries(pool.get_create_sql())

    @classmethod
    def before_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        pass

    @classmethod
    def after_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        pass

    def test(self):
        check_thread = Thread(target=self.check_signals_thread)
        self.stop_checking.clear()
        check_thread.start()
        overall_result = YdbCliHelper.WorkloadRunResult()
        try:
            qparams = self._get_query_settings()
            self.save_nodes_state()
            self.before_workload(overall_result)
            if self.workload_type is not None:
                results = YdbCliHelper.workload_run(
                    path=self.get_path(),
                    query_names=self.get_query_list(),
                    iterations=qparams.iterations,
                    workload_type=self.workload_type,
                    timeout=qparams.timeout,
                    check_canonical=self.check_canonical,
                    query_syntax=self.query_syntax,
                    scale=self.scale,
                    query_prefix=qparams.query_prefix,
                    external_path=self.get_external_path(),
                    threads=self.threads,
                    users=self.get_users(),
                )
                for query, result in results.items():
                    try:
                        with allure.step(query):
                            self.process_query_result(result, query, True)
                    except BaseException:
                        pass
            else:
                results = {}
            self.after_workload(overall_result)
        finally:
            self.stop_checking.set()
            check_thread.join()
        if len(results) > 0:
            overall_result.merge(*results.values())
        overall_result.iterations.clear()
        self.process_query_result(overall_result, 'test', True)
        if len(self.signal_errors) > 0:
            errors = '\n'.join([f'{d}: {e}' for d, e in self.signal_errors])
            pytest.fail(f'Errors while execute: {errors}')


class WorkloadManagerClickbenchBase:
    workload_type = ClickbenchParallelBase.workload_type
    iterations: int = ClickbenchParallelBase.iterations

    @classmethod
    def get_query_list(cls) -> list[str]:
        return ClickbenchParallelBase.get_query_list()

    @classmethod
    def get_path(cls) -> str:
        return ClickbenchParallelBase.get_path()

    @classmethod
    def benchmark_setup(cls) -> None:
        if not hasattr(cls, 'verify_data') or cls.verify_data:
            ClickbenchParallelBase.do_setup_class()


class WorkloadManagerTpchBase:
    workload_type = tpch.TpchParallelBase.workload_type
    iterations: int = tpch.TpchParallelBase.iterations

    @classmethod
    def get_query_list(cls) -> list[str]:
        return tpch.TpchParallelBase.get_query_list()

    @classmethod
    def get_path(cls) -> str:
        return get_external_param(f'table-path-{cls.suite()}', f'tpch/s{cls.scale}'.replace('.', '_'))

    @classmethod
    def benchmark_setup(cls) -> None:
        if not cls.verify_data or getenv('NO_VERIFY_DATA', '0') == '1' or getenv('NO_VERIFY_DATA_TPCH', '0') == '1' or getenv(f'NO_VERIFY_DATA_TPCH_{cls.scale}'):
            return
        tpch.TpchParallelBase.check_tables_size(folder=cls.get_path(), tables=tpch.TpchSuiteBase._get_tables_size(cls))


class WorkloadManagerConcurrentQueryLimit(WorkloadManagerBase):
    query_limit = 2
    hard_query_limit: int = 0
    max_in_fly = 0.

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [ResourcePool('test_pool', ['testuser'], concurrent_query_limit=cls.query_limit)]

    @classmethod
    def before_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        cls.hard_query_limit = cls.query_limit + len(YdbCluster.get_cluster_nodes(db_only=True))
        cls.threads = 2 * cls.hard_query_limit

    @classmethod
    def after_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        assert cls.max_in_fly > 0, "detector 'max queries in fly' does't work"

    @classmethod
    def check_signals(cls) -> str:
        metrics = YdbCluster.get_metrics(db_only=True, counters='kqp', metrics={
            'local_in_fly': {'subsystem': 'workload_manager', 'sensor': 'LocalInFly'}
        })
        sum_in_fly = sum([values.get('local_in_fly', 0.) for slot, values in metrics.items()])
        cls.max_in_fly = max(sum_in_fly, cls.max_in_fly)
        if sum_in_fly > cls.hard_query_limit:
            return f'Sum in fly is {sum_in_fly}, but limit is {cls.hard_query_limit}'
        return ''


class WorkloadManagerComputeScheduler(WorkloadManagerBase):
    metrics: list[(float, dict[str, float])] = []
    metrics_keys = set()

    @classmethod
    def get_key_measurements(cls) -> tuple[list[LoadSuiteBase.KeyMeasurement], str]:
        return [
            LoadSuiteBase.KeyMeasurement(f'satisfaction_avg_{p.name}', f'Satisfaction Avg {p.name}', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc', 1.e-5),
                LoadSuiteBase.KeyMeasurement.Interval('#ffcccc')
            ], f'Satisfaction for resource pool <b>{p.name}</b>. See explanations below.') for p in cls.get_resource_pools()
        ], '''<p>Parameter <b>satisfaction</b> is a metric that allows you to assess the level of satisfaction of a certain
        pool with resources (in this case, CPU time). It demonstrates how efficiently the pool uses the resources allocated
        to it compared to the amount that was planned for it.</p>

        <p>The target value of the metric is 1.0. It means that the pool uses resources
        in full compliance with the share allocated to it. Values below 1.0 indicate that the pool is underutilized,
        while values above 1.0 indicate that the pool is using more resources than were planned.</p>

        <p>Calculation formula: Satisfaction = Usage / FairShare<br/>

        where:<br/>

        Usage is the amount of CPU actually used by the pool;
        FairShare is the planned (fair) amount of CPU for the pool, which is calculated approximately as the product of the total amount
        of available CPU and the share of resources requested by the pool.</p>

        <p>In this test, we average the satisfaction across all cluster nodes and over time.</p>'''

    @classmethod
    def before_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        cls.metrics = []
        cls.metrics_keys = set()

    @classmethod
    def after_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        keys = sorted(cls.metrics_keys)
        pools = cls.get_resource_pools()
        report = ('<html><body><table border=1 valign="center" width="100%">'
                  '<tr><th style="padding-left: 10; padding-right: 10">time</th>' +
                  ''.join([f'<th style="padding-left: 10; padding-right: 10">{k[:-2] if k.endswith(' d') else k}</th>' for k in keys]) +
                  '</tr>\n')
        norm_metrics = []
        first_i = None
        last_i = None
        for r in range(len(cls.metrics)):
            record: dict[str, float] = {}
            cur_t, cur_m = cls.metrics[r]
            for k, v in cur_m.items():
                if k.endswith(' d'):
                    if r == 0:
                        record[k] = 0.
                    else:
                        prev_t, prev_m = cls.metrics[r - 1]
                        record[k] = (v - prev_m.get(k, 0.)) / (cur_t - prev_t)
                elif not k.endswith('satisfaction') or v >= 0.:
                    record[k] = v
            for p in pools:
                s = record.get(f'{p.name} satisfaction', -1.)
                if s >= 0.:
                    if first_i is None:
                        first_i = r
                    last_i = r
            norm_metrics.append(record)
            line = f'<tr><th style="padding-left: 10; padding-right: 10">{datetime.fromtimestamp(cur_t)}</th>'
            empty = True
            for k in keys:
                v = record.get(k)
                empty = empty and v is None
                if k.find('satisfaction') and v is not None and v < 0:
                    v = None
                v = f'{v:.3f}' if v is not None else ''
                line += f'<td style="padding-left: 10; padding-right: 10">{v}</td>'
            line += '</tr>\n'
            if not empty:
                report += line
        report += '</table></body></html>'
        allure.attach(report, 'metrics', allure.attachment_type.HTML)
        times = [datetime.fromtimestamp(t) for t, _ in cls.metrics]
        fig, axs = pyplot.subplots(len(pools), 1, layout='constrained', figsize=(6.4, 3.2 * len(pools)))
        if len(pools) == 1:
            axs = [axs]
        for p in range(len(pools)):
            pool = pools[p]
            axs[p].set_title(pool.name)
            axs[p].plot(times, [m.get(f'{pool.name} satisfaction') for m in norm_metrics], label='satisfaction')
            axs[p].plot(times, [m.get(f'{pool.name} adjusted satisfaction d') for m in norm_metrics], label='adj satisfaction')
            if last_i is not None:
                axs[p].plot([datetime.fromtimestamp(cls.metrics[first_i][0]), datetime.fromtimestamp(cls.metrics[last_i][0])], [1, 1], label='period')
            axs[p].set_ylabel('satisfaction')
            axs[p].legend(fontsize=10, loc='lower right')
            axs[p].grid()
            axs[p].xaxis.set_major_formatter(
                dates.ConciseDateFormatter(axs[p].xaxis.get_major_locator())
            )

        pyplot.savefig('satisfaction.plot.svg', format='svg')
        with open('satisfaction.plot.svg') as s:
            allure.attach(s.read(), 'satisfaction.plot.svg', allure.attachment_type.SVG)
        if last_i is not None:
            for pool in pools:
                last_t, last_v = cls.metrics[last_i]
                first_t, first_v = cls.metrics[first_i]
                sat = last_v.get(f'{pool.name} adjusted satisfaction d', 0.) - first_v.get(f'{pool.name} adjusted satisfaction d', 0.)
                if last_t > first_t:
                    sat /= last_t - first_t
                result.add_stat('test', f'satisfaction_avg_{pool.name}', sat)

    @classmethod
    def check_signals(cls) -> str:
        metrics_request = {}
        for pool in cls.get_resource_pools():
            metrics_request.update({
                f'{pool.name} satisfaction': {'scheduler/pool': pool.name, 'sensor': 'Satisfaction'},
                f'{pool.name} adjusted satisfaction d': {'scheduler/pool': pool.name, 'sensor': 'AdjustedSatisfaction'},
            })
        metrics = YdbCluster.get_metrics(db_only=True, counters='kqp', metrics=metrics_request)
        sum = {}
        count = {}
        for slot, values in metrics.items():
            for k, v in values.items():
                if not k.endswith('satisfaction') or v >= 0.:
                    sum.setdefault(k, 0.)
                    count.setdefault(k, 0)
                    sum[k] += v
                    count[k] += 1
                cls.metrics_keys.add(k)
        for k in sum.keys():
            if count[k] > 0:
                sum[k] /= count[k]
            if k.find('satisfaction') >= 0:
                sum[k] /= 1.e6
        cls.metrics.append((time.time(), sum))
        return ''


class WorkloadManagerComputeSchedulerP3(WorkloadManagerComputeScheduler):
    threads = 3

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [
            ResourcePool('test_pool_30', ['testuser30'], total_cpu_limit_percent_per_node=30, resource_weight=4),
            ResourcePool('test_pool_40', ['testuser40'], total_cpu_limit_percent_per_node=40, resource_weight=4),
            ResourcePool('test_pool_50', ['testuser50'], total_cpu_limit_percent_per_node=50, resource_weight=4),
        ]


class WorkloadManagerComputeSchedulerP1(WorkloadManagerComputeScheduler):
    threads = 1

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [
            ResourcePool('test_pool_100', ['testuser100'], total_cpu_limit_percent_per_node=100, resource_weight=4),
        ]


class TestWorkloadManagerClickbenchComputeScheduler(WorkloadManagerClickbenchBase, WorkloadManagerComputeSchedulerP3):
    pass


class TestWorkloadManagerClickbenchConcurrentQueryLimit(WorkloadManagerClickbenchBase, WorkloadManagerConcurrentQueryLimit):
    pass


class TestWorkloadManagerTpchComputeSchedulerS100(WorkloadManagerTpchBase, WorkloadManagerComputeSchedulerP3):
    tables_size = tpch.TestTpch100.tables_size
    scale = tpch.TestTpch100.scale
    timeout = tpch.TestTpch100.timeout * len(WorkloadManagerComputeSchedulerP3.get_resource_pools())
    threads = 1


class TestWorkloadManagerTpchComputeSchedulerP1S10(WorkloadManagerTpchBase, WorkloadManagerComputeSchedulerP1):
    tables_size = tpch.TpchParallelS1T10.tables_size
    scale = tpch.TpchParallelS1T10.scale
    timeout = tpch.TpchParallelS1T10.timeout * len(WorkloadManagerComputeSchedulerP1.get_resource_pools())
    threads = tpch.TpchParallelS1T10.threads
    iterations = tpch.TpchParallelS1T10.iterations


class TestWorkloadManagerClickbenchComputeSchedulerP1T1(WorkloadManagerClickbenchBase, WorkloadManagerComputeSchedulerP1):
    threads = 1
    iterations = ClickbenchParallelBase.iterations


class TestWorkloadManagerClickbenchComputeSchedulerP1T4(WorkloadManagerClickbenchBase, WorkloadManagerComputeSchedulerP1):
    threads = 4
    iterations = ClickbenchParallelBase.iterations


class WorkloadManagerOltp(WorkloadManagerComputeScheduler):
    threads = 1
    tpcc_started: bool = False
    tpcc_warehouses: int = 4500
    tpcc_threads: int = 4
    verify_data: bool = False
    __static_nodes: list[YdbCluster.Node] = []

    @classmethod
    def get_remote_tmpdir(cls):
        tmpdir = '/tmp'
        for node in cls.__static_nodes:
            if re.is_localhost(node.host):
                tmpdir = os.getenv('TMP') or os.getenv('TMPDIR') or yatest.common.work_path()
                break
        return os.path.join(tmpdir, cls.__name__, 'scripts', 'tpcc')

    @classmethod
    def do_setup_class(cls) -> None:
        cls.__static_nodes = YdbCluster.get_cluster_nodes(role=YdbCluster.Node.Role.STORAGE, db_only=False)
        results = re.deploy_binaries_to_hosts(
            [YdbCliHelper.get_cli_path()],
            [n.host for n in cls.__static_nodes],
            cls.get_remote_tmpdir()
        )
        for host, host_results in results.items():
            for bin, res in host_results.items():
                assert res.get('success', False), f"host: {host}, bin: {bin}, path: {res.get('path')}, error: {res.get('error')}"

        super().do_setup_class()

    @classmethod
    def get_tpcc_path(cls):
        root = get_external_param(f'table-path-{cls.suite()}', 'tpcc')
        return f'{root}/w10000'

    @classmethod
    def run_tpcc(cls, time: float, user: str):
        node = cls.__static_nodes[0]
        tmpdir = cls.get_remote_tmpdir()
        cmd = ['start-stop-daemon', '--start', '--pidfile', f'{tmpdir}/ydb_pid', '--make-pid', '--background', '--no-close', '--exec']
        cmd += [f'{tmpdir}/ydb', '--', '-e', f'grpc://{node.host}:{node.grpc_port}', '-d', f'/{YdbCluster.ydb_database}']
        if user:
            cmd += ['--user', user, '--no-password']
        cmd += ['workload', 'tpcc', '-p', YdbCluster.get_tables_path(cls.get_tpcc_path()), 'run', '--no-tui', '--warmup', '1s', '--format', 'Json']
        cmd += ['-t', f'{time}s', '-w', str(cls.tpcc_warehouses)]  # , '--threads', str(cls.tpcc_threads)]
        cmd += [f'>{tmpdir}/ydb_out', f'2>{tmpdir}/ydb_err']

        script_path = yatest.common.work_path('run_tpcc.sh')
        with open(script_path, 'w') as script_file:
            script_file.write('#!/bin/bash\n')
            script_file.write(' '.join(cmd))
        res = re.deploy_binary(script_path, node.host, tmpdir)
        assert res.get('success', False), f"slot: {node.slot}, bin: run_tpcc.sh, path: {res.get('path')}, error: {res.get('error')}"

        cls.execute_ssh(node.host, f'{tmpdir}/run_tpcc.sh').wait()
        cls.tpcc_started = True

    @classmethod
    def terminate_tpcc(cls):
        node = cls.__static_nodes[0]
        tmpdir = cls.get_remote_tmpdir()
        cmd = ['start-stop-daemon', '--stop', '--pidfile', f'{tmpdir}/ydb_pid']
        cls.execute_ssh(node.host, ' '.join(cmd)).wait(check_exit_code=False)

    @classmethod
    def tpcc_is_running(cls) -> bool:
        node = cls.__static_nodes[0]
        tmpdir = cls.get_remote_tmpdir()
        cmd = ['start-stop-daemon', '--status', '--pidfile', f'{tmpdir}/ydb_pid']
        pr = cls.execute_ssh(node.host, ' '.join(cmd))
        pr.wait(check_exit_code=False)
        return pr.returncode == 0

    @classmethod
    def wait_tpcc(cls) -> str:
        while cls.tpcc_is_running():
            time.sleep(1)
        node = cls.__static_nodes[0]
        tmpdir = cls.get_remote_tmpdir()
        cmd = ['cat', f'{tmpdir}/ydb_out']
        pr = cls.execute_ssh(node.host, ' '.join(cmd))
        pr.wait()
        return pr.stdout

    @classmethod
    def after_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        if cls.tpcc_started:
            cls.terminate_tpcc()
            stats = json.loads(cls.wait_tpcc())
            result.add_stat('test', 'tpcc_efficiency', stats.get('summary', {}).get('efficiency', 0.))
        super().after_workload(result)

    @classmethod
    def get_key_measurements(cls) -> tuple[list[LoadSuiteBase.KeyMeasurement], str]:
        return [
            LoadSuiteBase.KeyMeasurement('tpcc_efficiency', 'TPC-C Efficiency', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Efficiency of TPC-C')
        ], ''


class TestWorkloadManagerOltp100(WorkloadManagerOltp):
    tpcc_pool_perc = 100
    timeout: float = 900

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [
            ResourcePool(f'test_pool_{cls.tpcc_pool_perc}', [f'testuser{cls.tpcc_pool_perc}'], total_cpu_limit_percent_per_node=cls.tpcc_pool_perc, resource_weight=4),
        ]

    @classmethod
    def before_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        super().before_workload(result)
        cls.run_tpcc(cls.timeout, user=f'testuser{cls.tpcc_pool_perc}')

    @classmethod
    def after_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        if cls.tpcc_started:
            cls.wait_tpcc()
        super().after_workload(result)

    @classmethod
    def benchmark_setup(cls) -> None:
        pass


class TestWorkloadManagerOltp50(TestWorkloadManagerOltp100):
    tpcc_pool_perc = 50


class WorkloadManagerOltpTpch20Base(WorkloadManagerTpchBase, WorkloadManagerOltp):
    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [
            ResourcePool('test_pool_20', ['testuser20'], total_cpu_limit_percent_per_node=20, resource_weight=4),
        ]

    @classmethod
    def before_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        super().before_workload(result)
        cls.run_tpcc(cls.timeout, user='')


class TestWorkloadManagerOltpTpch20s100(WorkloadManagerOltpTpch20Base):
    tables_size = tpch.TestTpch100.tables_size
    scale = tpch.TestTpch100.scale
    timeout = tpch.TestTpch100.timeout
    iterations = 2


class TestWorkloadManagerOltpAdHoc(WorkloadManagerOltp):
    workload_type = WorkloadType.EXTERNAL
    iterations = 100
    threads = 5

    @classmethod
    def get_query_list(cls) -> list[str]:
        return [f'SELECT MAX(O_ENTRY_D) FROM `{cls.get_tpcc_path()}/oorder`']

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [
            ResourcePool('test_pool_10', ['testuser10'], total_cpu_limit_percent_per_node=10, resource_weight=4),
        ]

    @classmethod
    def before_workload(cls, result: YdbCliHelper.WorkloadRunResult):
        super().before_workload(result)
        cls.run_tpcc(cls.timeout, user='')

    @classmethod
    def benchmark_setup(cls) -> None:
        pass

    @classmethod
    def get_path(cls) -> str:
        return ''
