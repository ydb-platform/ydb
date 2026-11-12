#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import time

from ydb.tests.tools.datastreams_helpers.control_plane import create_read_rule
from ydb.tests.tools.datastreams_helpers.data_plane import READ_TOOL_TIMEOUT
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer


class TestDisposition(TestYdsBase):
    def test_disposition_oldest(self, ydb_topics):
        io = ydb_topics('test_disposition_oldest')
        sql = f'INSERT INTO {io.output} SELECT Data FROM {io.input};'
        query = None
        expected = []
        for batch in (['1', '2'], ['3', '4'], ['5', '6']):
            io.write(batch)
            expected += batch
            if query is None:
                query = io.start(sql, disposition='OLDEST')
            else:
                io.resume(query, disposition='OLDEST')
            assert io.read(len(expected)) == expected
            io.stop(query)

    def test_disposition_fresh(self, ydb_topics):
        io = ydb_topics('test_disposition_fresh')
        sql = f'INSERT INTO {io.output} SELECT Data FROM {io.input};'
        query = None
        for batch in (['1', '2'], ['3', '4'], ['5', '6']):
            io.write(batch)
            if query is None:
                query = io.start(sql, disposition='FRESH')
            else:
                io.resume(query, disposition='FRESH')
            assert io.read(1, timeout=1) == []
            io.stop(query)

    def test_disposition_from_time(self, ydb_topics):
        io = ydb_topics('disposition_from_time')
        timestamp_consumer = io.consumer_name + '_timestamps'
        create_read_rule(io.input_topic, timestamp_consumer, default_endpoint=io.endpoint)
        sql = f'INSERT INTO {io.output} SELECT Data FROM {io.input};'
        query = None
        for batch in (['1', '2'], ['3', '4'], ['5', '6']):
            io.write(batch)
            with io.kikimr.external_ydb_client.driver.topic_client.reader(
                io.input_topic, consumer=timestamp_consumer
            ) as reader:
                messages = [reader.receive_message(timeout=READ_TOOL_TIMEOUT) for _ in batch]
                assert [message.data.decode('utf-8') for message in messages] == batch
                for message in messages:
                    reader.commit_with_ack(message, timeout=READ_TOOL_TIMEOUT)
            # Use the topic's write timestamps rather than the wall clock.
            from_time = min(message.written_at for message in messages).replace(tzinfo=datetime.timezone.utc)
            timestamp = from_time.isoformat().replace('+00:00', 'Z')
            disposition = f'(FROM_TIME = "{timestamp}")'
            if query is None:
                query = io.start(sql, disposition=disposition)
            else:
                io.resume(query, disposition=disposition)
            assert io.read(len(batch)) == batch
            io.stop(query)
            time.sleep(1)  # Keep batches in distinct topic write-time intervals.

    def test_disposition_time_ago(self, ydb_topics):
        io = ydb_topics('disposition_time_ago')
        sql = f'INSERT INTO {io.output} SELECT Data FROM {io.input};'
        query = None
        lookback = plain_or_under_sanitizer(30, 60)
        for batch in (['1', '2'], ['3', '4'], ['5', '6']):
            io.write(batch)
            disposition = f'(TIME_AGO = "PT{lookback}S")'
            if query is None:
                query = io.start(sql, disposition=disposition)
            else:
                io.resume(query, disposition=disposition)
            assert io.read(len(batch)) == batch
            io.stop(query)
            time.sleep(lookback + 1)
