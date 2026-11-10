#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.fq.streaming_common.common import StreamingTestBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.wait_for import wait_for


class TestStop(StreamingTestBase):
    def test_stop_query(self, ydb_topics):
        io = ydb_topics('select_stop')
        external = io.kikimr.external_ydb_client
        consumer = io.name + '_input_consumer'
        external.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{consumer}`;')
        query = io.start(f"""
            PRAGMA pq.Consumer = '{consumer}';
            PRAGMA ydb.OptValidateStreamingCheckpoints = 'FALSE';
            INSERT INTO {io.output} SELECT Data FROM {io.input} LIMIT 3;
        """)

        messages = ['A', 'B']
        io.write(messages)

        def output_size():
            description = external.driver.topic_client.describe_topic(io.output_topic, include_stats=True)
            return sum(
                partition.partition_stats.partition_end
                for partition in description.partitions
                if partition.partition_stats is not None
            )

        # Wait for the partial result without reading it until after the stop.
        assert wait_for(
            lambda: output_size() == len(messages),
            timeout_seconds=plain_or_under_sanitizer(120, 500),
            step_seconds=0.5,
        ), output_size()
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)

        io.stop(query)
        assert io.read(len(messages)) == messages

        # Reaching the original LIMIT after STOP must not resume processing.
        io.write(['C'])
        assert not io.read(1, timeout=1)
        assert io.state(query)['Status'] == 'STOPPED', io.state(query)
        assert output_size() == len(messages)
