#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, create_read_rule
from ydb.tests.tools.datastreams_helpers.data_plane import write_stream, read_stream


class TestYdsBase(object):
    @staticmethod
    def __unwrap_endpoint(endpoint):
        if endpoint is not None:
            return endpoint.database, endpoint.endpoint
        return None, None

    def init_topics(self, prefix, create_input=True, create_output=True, partitions_count=1, endpoint=None):
        self.consumer_name = prefix + "_consumer"

        if create_input:
            self.input_topic = prefix + "_input"
            create_stream(self.input_topic, partitions_count=partitions_count, default_endpoint=endpoint)

        if create_output:
            self.output_topic = prefix + "_output"
            create_stream(self.output_topic, partitions_count=partitions_count, default_endpoint=endpoint)
            create_read_rule(self.output_topic, self.consumer_name, default_endpoint=endpoint)

    def write_stream(self, data, topic_path=None, partition_key=None, endpoint=None):
        database, fqdn = self.__unwrap_endpoint(endpoint)
        topic = topic_path if topic_path else self.input_topic
        write_stream(topic, data, partition_key=partition_key, database=database, endpoint=fqdn)

    def read_stream(self, messages_count, commit_after_processing=True, topic_path=None, endpoint=None, timeout=None):
        database, fqdn = self.__unwrap_endpoint(endpoint)
        topic = topic_path if topic_path else self.output_topic
        return read_stream(topic, messages_count, commit_after_processing, self.consumer_name, database=database, endpoint=fqdn, timeout=timeout)

    def wait_until(self, predicate, wait_time=plain_or_under_sanitizer(10, 50)):
        deadline = time.time() + wait_time
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(1)
        return False
