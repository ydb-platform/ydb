#!/usr/bin/env python
# -*- coding: utf-8 -*-


from ydb.tests.fq.streaming_common.common import StreamingTestBase, counter_nodes, get_sensors
from ydb.tests.library.common.wait_for import wait_for


class YdbTopics(StreamingTestBase):
    """Topics and SQL lifecycle for the tests already migrated to Query Service."""

    def __init__(self, kikimr, name, partitions=1, shared=False):
        self.kikimr = kikimr
        self.name = name
        self.queries = []
        self.input, self.output, self.endpoint = self.get_io_names(
            kikimr, name, False, lambda value: value, partitions_count=partitions, shared=shared
        )

    def write(self, data, topic_path=None, partition_key=None):
        self.write_stream(data, topic_path=topic_path, partition_key=partition_key, endpoint=self.endpoint)

    def read(self, count, topic_path=None, timeout=None):
        return self.read_stream(count, topic_path=topic_path, timeout=timeout, endpoint=self.endpoint)

    def start(self, sql, *, with_checkpoints=True, disposition='FRESH', timeout=120):
        query = self.kikimr.get_database_name() + '/' + self.name + '_query_' + str(len(self.queries))
        self.queries.append(query)
        self.kikimr.ydb_client.query(
            f'''
            CREATE STREAMING QUERY `{query}` WITH (
                CHECKPOINT_INTERVAL = 'PT1S', STREAMING_DISPOSITION = {disposition}
            ) AS DO BEGIN
                {sql}
            END DO;
        ''',
            timeout=timeout,
        )
        if with_checkpoints:
            self.checkpoint(query, timeout=timeout)
        else:
            assert wait_for(
                lambda: any(
                    self.get_actor_count(self.kikimr, node_id, actor) > 0
                    for node_id in counter_nodes(self.kikimr.cluster)
                    for actor in ('DQ_MESSAGE_STREAM_READ_ACTOR', 'DQ_PQ_READ_ACTOR')
                ),
                timeout_seconds=timeout,
                step_seconds=0.5,
            ), self.state(query)
        return query

    def state(self, query):
        rows = self.kikimr.ydb_client.query(f'''
            SELECT Status, Issues FROM `.sys/streaming_queries` WHERE Path = '{query}';
        ''')[0].rows
        assert len(rows) == 1, rows
        return rows[0]

    def stop(self, query):
        self.kikimr.ydb_client.query(f'ALTER STREAMING QUERY `{query}` SET (RUN = FALSE);')
        assert wait_for(
            lambda: self.state(query)['Status'] == 'STOPPED', timeout_seconds=120, step_seconds=0.5
        ), self.state(query)

    def resume(self, query, *, disposition):
        self.kikimr.ydb_client.query(f'''
            ALTER STREAMING QUERY `{query}` SET (RUN = TRUE, STREAMING_DISPOSITION = {disposition});
        ''')
        self.checkpoint(query)

    def checkpoint(self, query, count=2, timeout=120):
        self.wait_completed_checkpoints(self.kikimr, query.rsplit('/', 1)[1], checkpoints_count=count, timeout=timeout)

    def wait_actor_count(self, activity, expected):
        def count():
            return sum(
                self.get_actor_count(self.kikimr, node_id, activity) for node_id in counter_nodes(self.kikimr.cluster)
            )

        assert wait_for(lambda: count() == expected, timeout_seconds=120, step_seconds=0.5), (
            activity,
            count(),
            expected,
        )

    def wait_read_session_closed(self, topic, consumer):
        description = None

        def closed():
            nonlocal description
            description = self.kikimr.external_ydb_client.driver.topic_client.describe_consumer(
                topic, consumer, include_stats=True
            )
            partitions = [item for item in description.partitions if item.active]
            return bool(partitions) and all(
                item.partition_consumer_stats is not None and not item.partition_consumer_stats.read_session_id
                for item in partitions
            )

        assert wait_for(closed, timeout_seconds=120, step_seconds=0.5), description

    def metrics(self, query):
        result = {}
        for node_id in counter_nodes(self.kikimr.cluster):
            metrics = get_sensors(self.kikimr.cluster, node_id, 'kqp').find_sensors(
                {'subsystem': 'streaming_queries', 'path': query}, key_label='sensor'
            )
            for name, value in metrics.items():
                result[name] = result.get(name, 0) + value
        return result

    def restart_compute_node(self, timeout=120, *, with_coordinator=None):
        """Restart a dynamic node hosting a live KQP_COMPUTE_ACTOR."""

        def find_node():
            for node_id in self.kikimr.cluster.slots:
                if with_coordinator is not None:
                    has_coordinator = self.get_actor_count(self.kikimr, node_id, 'YQ_CHECKPOINT_COORDINATOR') > 0
                    if has_coordinator != with_coordinator:
                        continue
                if self.get_actor_count(self.kikimr, node_id, 'KQP_COMPUTE_ACTOR') > 0:
                    return node_id
            return None

        assert wait_for(lambda: find_node() is not None, timeout_seconds=timeout, step_seconds=0.5), (
            'No matching node hosting KQP_COMPUTE_ACTOR found',
            with_coordinator,
        )
        node_id = find_node()
        assert node_id is not None, 'KQP_COMPUTE_ACTOR disappeared before restart'
        self.restart_node(self.kikimr, node_id)
        self.kikimr.wait_kqp_node_count(len(self.kikimr.cluster.slots), timeout_seconds=timeout)
        return node_id

    def start_select(self, sql, topics, timeout=120):
        consumer = self.name + '_select_consumer'
        external = self.kikimr.external_ydb_client
        for topic in topics:
            external.query(f'ALTER TOPIC `{topic}` ADD CONSUMER `{consumer}`;')
        future = self.kikimr.ydb_client.query_async(f'PRAGMA pq.Consumer = "{consumer}";\n{sql}', timeout=timeout)

        def readers_started():
            if future.done():
                future.result()
                raise AssertionError('Topic SELECT finished before input was written')
            for topic in topics:
                description = external.driver.topic_client.describe_consumer(topic, consumer, include_stats=True)
                partitions = [partition for partition in description.partitions if partition.active]
                if not partitions or not all(
                    partition.partition_consumer_stats is not None
                    and partition.partition_consumer_stats.read_session_id
                    for partition in partitions
                ):
                    return False
            return True

        assert wait_for(readers_started, timeout_seconds=timeout, step_seconds=0.2), 'Topic readers did not start'
        return future

    def close(self):
        # Drop queries before their external data source and topics.
        try:
            for query in reversed(self.queries):
                self.kikimr.ydb_client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;')
        finally:
            try:
                self.kikimr.ydb_client.query(f'DROP EXTERNAL DATA SOURCE IF EXISTS `{self.name}`;')
            finally:
                for topic in (self.output_topic, self.input_topic):
                    self.kikimr.external_ydb_client.query(f'DROP TOPIC IF EXISTS `{topic}`;')
