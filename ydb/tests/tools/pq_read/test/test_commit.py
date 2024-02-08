#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, create_read_rule
from ydb.tests.tools.datastreams_helpers.data_plane import write_stream, read_stream


class TestCommit(object):
    def test_commit(self):
        consumer_name = "test_client"
        topic = "topic"
        create_stream(topic, partitions_count=1)
        create_read_rule(topic, consumer_name)

        data = ["1", "2"]
        write_stream(topic, data)

        assert read_stream(topic, len(data), commit_after_processing=True, consumer_name=consumer_name) == data

        data_2 = ["3", "4"]
        write_stream(topic, data_2)

        assert read_stream(topic, len(data_2), commit_after_processing=True, consumer_name=consumer_name) == data_2
