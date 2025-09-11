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
    def before_workload(cls):
        pass

    @classmethod
    def after_workload(cls):
        pass

    def test(self):
        check_thread = Thread(target=self.check_signals_thread)
        self.stop_checking.clear()
        check_thread.start()
        start_time = time.time()
        try:
            qparams = self._get_query_settings()
            self.save_nodes_state()
            self.before_workload()
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
            self.after_workload()
        finally:
            self.stop_checking.set()
            check_thread.join()
        overall_result = YdbCliHelper.WorkloadRunResult()
        overall_result.merge(*results.values())
        overall_result.iterations.clear()
        overall_result.start_time = start_time
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
    def before_workload(cls):
        cls.hard_query_limit = cls.query_limit + len(YdbCluster.get_cluster_nodes(db_only=True))
        cls.threads = 2 * cls.hard_query_limit

    @classmethod
    def after_workload(cls):
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
    threads = 10
    metrics: list[(float, dict[str, float])] = []
    metrics_keys = set()

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [
            ResourcePool('test_pool_30', ['testuser1'], total_cpu_limit_percent_per_node=30, resource_weight=4),
            ResourcePool('test_pool_40', ['testuser2'], total_cpu_limit_percent_per_node=40, resource_weight=4),
            ResourcePool('test_pool_50', ['testuser3'], total_cpu_limit_percent_per_node=50, resource_weight=4),
        ]

    @classmethod
    def before_workload(cls):
        cls.metrics = []
        cls.metrics_keys = set()

    @classmethod
    def after_workload(cls):
        keys = sorted(cls.metrics_keys)
        report = ('<html><body><table border=1 valign="center" width="100%">'
                  '<tr><th style="padding-left: 10; padding-right: 10">time</th>' +
                  ''.join([f'<th style="padding-left: 10; padding-right: 10">{k[:-2] if k.endswith(' d') else k}</th>' for k in keys]) +
                  '</tr>\n')
        norm_metrics = []
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
                else:
                    record[k] = v
            norm_metrics.append(record)
            report += f'<tr><th style="padding-left: 10; padding-right: 10">{datetime.fromtimestamp(cur_t)}</th>'
            for k in keys:
                v = record.get(k)
                v = f'{record.get(k):.1f}' if v is not None else ''
                report += f'<td style="padding-left: 10; padding-right: 10">{v}</td>'
            report += '</tr>\n'
        report += '</table></body></html>'
        allure.attach(report, 'metrics', allure.attachment_type.HTML)
        times = [datetime.fromtimestamp(t) for t, _ in cls.metrics]
        pools = cls.get_resource_pools()
        fig, axs = pyplot.subplots(len(pools), 1, layout='constrained', figsize=(6.4, 3.2 * len(pools)))
        for p in range(len(pools)):
            pool = pools[p]
            axs[p].plot(times, [m.get(f'{pool.name} satisfaction') for m in norm_metrics], label=pool.name)
            axs[p].set_ylabel('satisfaction')
            axs[p].legend()
            axs[p].grid()
            axs[p].xaxis.set_major_formatter(
                dates.ConciseDateFormatter(axs[p].xaxis.get_major_locator())
            )

        pyplot.savefig('satisfaction.plot.svg', format='svg')
        with open('satisfaction.plot.svg') as s:
            allure.attach(s.read(), 'satisfaction.plot.svg', allure.attachment_type.SVG)

    @classmethod
    def check_signals(cls) -> str:
        metrics_request = {}
        for pool in cls.get_resource_pools():
            metrics_request.update({
                f'{pool.name} satisfaction': {'scheduler/pool': pool.name, 'sensor': 'Satisfaction'},
            })
        metrics = YdbCluster.get_metrics(db_only=True, counters='kqp', metrics=metrics_request)
        sum = {}
        for slot, values in metrics.items():
            for k, v in values.items():
                sum.setdefault(k, 0.)
                sum[k] += v
                cls.metrics_keys.add(k)
        for k in sum.keys():
            if not k.endswith(' d'):
                sum[k] /= len(metrics)
        cls.metrics.append((time.time(), sum))
        return ''


class TestWorkloadManagerClickbenchComputeScheduler(WorkloadManagerClickbenchBase, WorkloadManagerComputeScheduler):
    pass


class TestWorkloadManagerClickbenchConcurrentQueryLimit(WorkloadManagerClickbenchBase, WorkloadManagerConcurrentQueryLimit):
    pass


class TestWorkloadManagerTpchComputeSchedulerS100(WorkloadManagerTpchBase, WorkloadManagerComputeScheduler):
    tables_size = tpch.TestTpch100.tables_size
    scale = tpch.TestTpch100.scale
