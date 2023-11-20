#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, create_read_rule
from ydb.tests.tools.datastreams_helpers.data_plane import write_stream, read_stream


class TestYdsBase(object):
    def init_topics(self, prefix, create_input=True, create_output=True, partitions_count=1):
        self.consumer_name = prefix + "_consumer"

        if create_input:
            self.input_topic = prefix + "_input"
            create_stream(self.input_topic, partitions_count=partitions_count)

        if create_output:
            self.output_topic = prefix + "_output"
            create_stream(self.output_topic, partitions_count=partitions_count)
            create_read_rule(self.output_topic, self.consumer_name)

    def write_stream(self, data, topic_path=None):
        topic = topic_path if topic_path else self.input_topic
        write_stream(topic, data)

    def read_stream(self, messages_count, commit_after_processing=True, topic_path=None):
        topic = topic_path if topic_path else self.output_topic
        return read_stream(topic, messages_count, commit_after_processing, self.consumer_name)

    def wait_until(self, predicate, wait_time=yatest_common.plain_or_under_sanitizer(10, 50)):
        deadline = time.time() + wait_time
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(1)
        return False
