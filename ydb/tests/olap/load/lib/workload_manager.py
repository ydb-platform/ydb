from __future__ import annotations

import ydb
import pytest

from .clickbench import ClickbenchParallelBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from threading import Thread, Event
from datetime import datetime


class TestWorkloadMangerClickbenchConcurentQueryLimit(ClickbenchParallelBase):
    threads: int = 10
    resource_pools = ['testpool']
    user_group = 'testgroup'
    stop_checking = Event()
    hard_query_limit: int = 0
    signal_errors: list[tuple[datetime, str]] = []
    max_in_fly = 0.

    @classmethod
    def get_users(cls) -> list[str]:
        return ['testuser1', 'testuser2']

    @classmethod
    def check_signals_thread(cls) -> None:
        while not cls.stop_checking.wait(1.):
            try:
                error = cls.check_signals()
            except BaseException as e:
                error = str(e)
            if error:
                cls.signal_errors.append((datetime.now(), error))

    @classmethod
    def check_signals(cls) -> str:
        metrics = YdbCluster.get_metrics(db_only=True, counters='kqp', metrics={
            'local_in_fly': {'subsystem': 'workload_manager', 'sensor': 'LocalInFly'}
        })
        sum_in_fly = sum([values.get('local_in_fly', 0.) for slot, values in metrics.items()])
        cls.max_in_fly = max(sum_in_fly, cls.max_in_fly)
        if sum_in_fly > cls.hard_query_limit:
            return f'Sum in fly is {sum_in_fly}, but limit is {cls.hard_query_limit}'

    @classmethod
    def do_setup_class(cls):
        def _exists(path: str) -> bool:
            try:
                YdbCluster.get_ydb_driver().scheme_client.describe_path(f'/{YdbCluster.ydb_database}/{path}')
            except BaseException:
                return False
            return True

        sessions_pool = ydb.QuerySessionPool(YdbCluster.get_ydb_driver())
        query_limit = 2
        cls.hard_query_limit = query_limit + len(YdbCluster.get_cluster_nodes(db_only=True))

        for pool in cls.resource_pools:
            if _exists('.metadata/workload_manager/classifiers/resource_pool_classifiers'):
                c = sessions_pool.execute_with_retries(f'''
                    SELECT
                        count(*)
                    FROM `.metadata/workload_manager/classifiers/resource_pool_classifiers`
                    WHERE name="{pool}"
                ''')
                if c[0].rows[0][0] > 0:
                    sessions_pool.execute_with_retries(f'DROP RESOURCE POOL CLASSIFIER {pool}')
            if _exists(f'.metadata/workload_manager/pools/{pool}'):
                sessions_pool.execute_with_retries(f'DROP RESOURCE POOL {pool}')
        sessions_pool.execute_with_retries(f'DROP GROUP IF EXISTS {cls.user_group}; DROP USER IF EXISTS {", ".join(cls.get_users())}')

        for user in cls.get_users():
            sessions_pool.execute_with_retries(f'CREATE USER {user} PASSWORD NULL; GRANT ALL ON `/{YdbCluster.ydb_database}` TO {user}')
        sessions_pool.execute_with_retries(f'CREATE GROUP {cls.user_group} WITH USER {", ".join(cls.get_users())}')

        for pool in cls.resource_pools:
            sessions_pool.execute_with_retries(f'''
                CREATE RESOURCE POOL {pool} WITH (
                    CONCURRENT_QUERY_LIMIT = {query_limit}
                )''')
            sessions_pool.execute_with_retries(f'''
                CREATE RESOURCE POOL CLASSIFIER {pool} WITH (
                    RESOURCE_POOL = '{pool}',
                    MEMBER_NAME = '{cls.user_group}'
                )''')
        cls.threads = cls.hard_query_limit
        check_thread = Thread(target=cls.check_signals_thread)
        check_thread.start()
        super().do_setup_class()
        cls.stop_checking.set()
        check_thread.join()
        if len(cls.signal_errors) > 0:
            errors = '\n'.join([f'{d}: {e}' for d, e in cls.signal_errors])
            pytest.fail(f'Errors while execute: {errors}')
        assert cls.max_in_fly > 0, "detector 'max queries in fly' does't work"
