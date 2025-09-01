from __future__ import annotations

import ydb
import pytest
import allure
import traceback

from .clickbench import ClickbenchParallelBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from threading import Thread, Event
from datetime import datetime


class ResourcePool:
    def __init__(self, name: str, users: list[str] | str, **kvargs):
        self.name = name
        self.users = users if isinstance(users, list) else [users]
        self.params = kvargs

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


class WorkloadMangerClickbenchBase(ClickbenchParallelBase):
    stop_checking = Event()
    signal_errors: list[tuple[datetime, str]] = []

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

        check_thread = Thread(target=cls.check_signals_thread)
        check_thread.start()
        try:
            super().do_setup_class()
        except BaseException:
            raise
        finally:
            cls.stop_checking.set()
            check_thread.join()
        if len(cls.signal_errors) > 0:
            errors = '\n'.join([f'{d}: {e}' for d, e in cls.signal_errors])
            pytest.fail(f'Errors while execute: {errors}')


class TestWorkloadMangerClickbenchConcurentQueryLimit(WorkloadMangerClickbenchBase):
    query_limit = 2
    hard_query_limit: int = 0
    max_in_fly = 0.

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [ResourcePool('test_pool', 'testuser', concurrent_query_limit=cls.query_limit)]

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


class TestWorkloadMangerClickbenchComputeSheduler(WorkloadMangerClickbenchBase):
    threads = 1
    metrics = []
    metrics_keys = set()

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        return [
            ResourcePool('test_pool_30', 'testuser1', total_cpu_limit_percent_per_node=30, resource_weight=4),
            ResourcePool('test_pool_40', 'testuser2', total_cpu_limit_percent_per_node=40, resource_weight=4),
            ResourcePool('test_pool_50', 'testuser3', total_cpu_limit_percent_per_node=50, resource_weight=4),
        ]

    @classmethod
    def after_workload(cls):
        keys = sorted(cls.metrics_keys)
        report = '<html><body><table border=1 valign="center" width="100%">\n<tr>' + ''.join([f'<th style="padding-left: 10; padding-right: 10">{k}</th>' for k in keys]) + '</tr>\n'
        for record in cls.metrics:
            report += '<tr>' + ''.join([f'<td style="padding-left: 10; padding-right: 10">{record.get(k)}</td>' for k in keys]) + '</tr>\n'
        report += '</table></body></html>'
        allure.attach(report, 'metrics', allure.attachment_type.HTML)

    @classmethod
    def check_signals(cls) -> str:
        metrics_request = {}
        for pool in cls.get_resource_pools():
            metrics_request.update({
                f'{pool.name} satisfaction': {'scheduler/pool': pool.name, 'sensor': 'Satisfaction'},
                f'{pool.name} usage': {'scheduler/pool': pool.name, 'sensor': 'Usage'},
                f'{pool.name} throttle': {'scheduler/pool': pool.name, 'sensor': 'Throttle'},
                f'{pool.name} fair share': {'scheduler/pool': pool.name, 'sensor': 'FairShare'},
            })
        metrics = YdbCluster.get_metrics(db_only=True, counters='kqp', metrics=metrics_request)
        sum = {}
        for slot, values in metrics.items():
            for k, v in values.items():
                sum.setdefault(k, 0.)
                sum[k] += v
                cls.metrics_keys.add(k)
        cls.metrics.append(sum)
        return ''
