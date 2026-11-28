import json
import time

import ydb

from ydb.tests.fq.streaming_common.common import (
    StreamingTestBase,
    counter_nodes,
    get_completed_checkpoints,
    get_sensors,
)
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.wait_for import wait_for

M = 1024 * 1024
G = 1024 * M
MEMORY_POOL = 'mem_alloc'
NODE_MEMORY_LIMIT = 8 * G
DEFAULT_LIMIT = M
DEFAULT_DELTA = M
TIMEOUT = plain_or_under_sanitizer(120, 300)


def memory_metrics(kikimr):
    result = {}
    for node_id in counter_nodes(kikimr.cluster):
        sensors = get_sensors(kikimr.cluster, node_id, 'kqp')
        values = {}
        for name in ('RM/ComputeActors', 'RM/Memory', 'RM/ExternalMemory', 'RM/ExtraMemAllocs'):
            # Read node totals, excluding similarly named counters in subgroups.
            matches = [
                s['value']
                for s in sensors.data
                if s['labels'].get('sensor') == name
                and not (s['labels'].keys() - {'sensor', 'database', 'slot', 'host'})
            ]
            assert len(matches) == 1, (node_id, name, matches)
            values[name] = matches[0]
        pool_labels = {'schedulerPool': MEMORY_POOL}
        for name in ('MemoryLimit', 'MemoryUsage'):
            value = sensors.find_sensor(dict(pool_labels, sensor=name))
            values[name] = value if value is not None else 0
        # Long-lived services also reserve RM memory. Compare query allocations
        # against the idle node baseline captured before creating any queries.
        values['RM/Memory'] -= getattr(kikimr, 'idle_memory', {}).get(node_id, 0)
        result[node_id] = values
    return result


def total_metric(kikimr, name):
    return sum(node[name] for node in memory_metrics(kikimr).values())


def wait_pool_memory(kikimr, expected):
    assert wait_for(
        lambda: total_metric(kikimr, 'MemoryUsage') == expected,
        timeout_seconds=TIMEOUT,
        step_seconds=0.2,
        multiply=1,
    ), (expected, memory_metrics(kikimr))


def wait_memory_released(kikimr):
    def released():
        return all(
            all(node[name] == 0 for name in ('RM/ComputeActors', 'RM/Memory', 'RM/ExternalMemory', 'MemoryUsage'))
            for node in memory_metrics(kikimr).values()
        )

    assert wait_for(released, timeout_seconds=TIMEOUT, step_seconds=0.2, multiply=1), memory_metrics(kikimr)


def issue_messages(value):
    if isinstance(value, (str, bytes)):
        value = json.loads(value)
    if isinstance(value, dict):
        if 'message' in value:
            yield value['message']
        for nested in value.values():
            if isinstance(nested, (dict, list)):
                yield from issue_messages(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from issue_messages(nested)


class MemoryQuery(StreamingTestBase):
    def __init__(self, kikimr, name, partitions=1):
        self.kikimr = kikimr
        self.name = name
        self.queries = []
        self.input, self.output, self.endpoint = self.get_io_names(
            kikimr, name, False, lambda value: value, partitions_count=partitions
        )

    def aggregation_sql(self, count=False):
        value = 'CAST(COUNT(*) AS String)' if count else 'Data'
        # Keep keys until the test stops the query: window closure must not race
        # the allocation checks, especially under sanitizers.
        return f'''
            INSERT INTO {self.output}
            SELECT {value} AS Data FROM {self.input}
            GROUP BY HOP(Just(CurrentUtcTimestamp()), 'PT1H', 'PT1H', 'PT1H'), Data;
        '''

    def create(self, sql, wait=True):
        name = f'{self.name}_query_{len(self.queries)}'
        path = f'{self.kikimr.get_database_name()}/{name}'
        # Concurrent queries must each own their reader, rather than compete
        # for partitions under a shared consumer.
        consumer = f'{name}_consumer'
        self.kikimr.external_ydb_client.query(f'ALTER TOPIC `{self.input_topic}` ADD CONSUMER `{consumer}`;')
        # CREATE can leave the object behind when starting its execution fails.
        # Keep it for cleanup and preserve the first error instead of retrying DDL.
        self.queries.append(path)
        self.kikimr.ydb_client.session_pool.execute_with_retries(
            f'''
            CREATE STREAMING QUERY `{path}` WITH (
                RESOURCE_POOL = '{MEMORY_POOL}', CHECKPOINT_INTERVAL = 'PT1H'
            ) AS DO BEGIN
                PRAGMA pq.Consumer = '{consumer}';
                {sql}
            END DO;
        ''',
            settings=ydb.BaseRequestSettings().with_timeout(TIMEOUT),
            retry_settings=ydb.RetrySettings(max_retries=0),
        )
        if wait:
            assert self.wait_started_or_memory_error(path), self.state(path)
            self.wait_pool_limit()
        return path

    def state(self, query):
        rows = self.kikimr.ydb_client.query(
            f'''
            SELECT Status, Issues, RetryCount FROM `.sys/streaming_queries` WHERE Path = '{query}';
        ''',
            timeout=TIMEOUT,
        )[0].rows
        assert len(rows) == 1, rows
        return rows[0]

    def wait_pool_limit(self):
        limit = self.kikimr.memory_settings['total']

        def applied():
            nodes = memory_metrics(self.kikimr)
            active = [node for node in nodes.values() if node['MemoryUsage'] > 0]
            return bool(active) and all(
                node['MemoryLimit'] == limit and node['MemoryUsage'] == node['RM/Memory'] + node['RM/ExternalMemory']
                for node in active
            )

        assert wait_for(applied, timeout_seconds=TIMEOUT, step_seconds=0.2, multiply=1), memory_metrics(self.kikimr)

    def wait_started_or_memory_error(self, query):
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            if get_completed_checkpoints(self.kikimr.cluster, query) > 0:
                return True
            state = self.state(query)
            issues = '\n'.join(issue_messages(state['Issues'])).lower()
            if (state['RetryCount'] > 0 or state['Status'] in ('SUSPENDED', 'FAILED')) and any(
                message in issues for message in ('not enough memory', 'memory limit exceeded', 'not enough resources')
            ):
                return False
            assert state['Status'] not in ('FAILED', 'STOPPED'), state
            time.sleep(0.2)
        raise AssertionError(f'Query did not start or report a memory error: {self.state(query)}')

    def stop(self, query):
        self.kikimr.ydb_client.query(f'ALTER STREAMING QUERY `{query}` SET (RUN = FALSE);', timeout=TIMEOUT)
        assert wait_for(
            lambda: self.state(query)['Status'] == 'STOPPED', timeout_seconds=TIMEOUT, step_seconds=0.2, multiply=1
        ), self.state(query)

    def write_until(self, predicate, query):
        for offset in range(0, 10000, 128):
            self.write_stream(
                [f'{index:09x}:' + 'x' * 256 for index in range(offset, min(offset + 128, 10000))],
                endpoint=self.endpoint,
            )
            # The write acknowledgement does not imply that the compute actor
            # has processed the batch yet.
            if wait_for(predicate, timeout_seconds=0.2, step_seconds=0.05, multiply=1):
                return
        assert wait_for(predicate, timeout_seconds=TIMEOUT, step_seconds=0.2, multiply=1), (
            self.state(query),
            memory_metrics(self.kikimr),
        )

    def close(self):
        for query in reversed(self.queries):
            self.kikimr.ydb_client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;', timeout=TIMEOUT)
