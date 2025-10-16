from __future__ import annotations

import ydb
import pytest
import allure
import time
import traceback

from . import tpch
from .conftest import LoadSuiteBase
from .clickbench import ClickbenchParallelBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
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
            self.after_workload(overall_result)
        finally:
            self.stop_checking.set()
            check_thread.join()
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
            ResourcePool('test_pool_30', ['testuser1'], total_cpu_limit_percent_per_node=30, resource_weight=4),
            ResourcePool('test_pool_40', ['testuser2'], total_cpu_limit_percent_per_node=40, resource_weight=4),
            ResourcePool('test_pool_50', ['testuser3'], total_cpu_limit_percent_per_node=50, resource_weight=4),
        ]


class WorkloadManagerComputeSchedulerP1(WorkloadManagerComputeScheduler):
    threads = 1

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [
            ResourcePool('test_pool_100', ['testuser1'], total_cpu_limit_percent_per_node=100, resource_weight=4),
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
