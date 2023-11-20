#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, create_read_rule
from ydb.tests.tools.datastreams_helpers.data_plane import write_stream, read_stream


class TestTimeout(object):
    def test_timeout(self):
        consumer_name = "test_client"
        topic = "timeout"
        create_stream(topic, partitions_count=1)
        create_read_rule(topic, consumer_name)

        data = ["1", "2"]
        write_stream(topic, data)

        assert read_stream(topic, len(data) + 42, commit_after_processing=True, consumer_name=consumer_name, timeout=3) == data
