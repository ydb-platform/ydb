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

    def write_stream_with_message_metadata(self, kikimr, data, topic_path=None, endpoint=None):
        """Write payloads via Topic API with per-message metadata (key/value pairs).

        data: list of (payload_str, metadata_dict) where metadata_dict maps str -> str or bytes.
        When endpoint is set (same as for write_stream/read_stream), the writer uses that cluster;
        otherwise kikimr's local driver is used.
        """
        import ydb

        topic = topic_path if topic_path else self.input_topic

        def write_with_driver(driver):
            with driver.topic_client.writer(topic, producer_id="fq_test_message_meta_writer") as writer:
                for item in data:
                    payload, meta = item
                    writer.write(ydb.TopicWriterMessage(str(payload), metadata_items=meta or {}))

        if endpoint is not None:
            database, fqdn = self.__unwrap_endpoint(endpoint)
            driver_config = ydb.DriverConfig(f"grpc://{fqdn}", database, auth_token="root@builtin")
            driver = ydb.Driver(driver_config)
            driver.wait(timeout=5)
            try:
                write_with_driver(driver)
            finally:
                driver.stop()
        else:
            driver = getattr(kikimr, "driver", None) or kikimr.ydb_client.driver
            write_with_driver(driver)

    def read_stream(self, messages_count, commit_after_processing=True, topic_path=None, endpoint=None, timeout=None):
        database, fqdn = self.__unwrap_endpoint(endpoint)
        topic = topic_path if topic_path else self.output_topic
        return read_stream(topic, messages_count, commit_after_processing, self.consumer_name, database=database, endpoint=fqdn, timeout=timeout)

    def read_topic_messages_with_metadata(self, kikimr, messages_count, topic_path=None, endpoint=None, timeout_sec=None):
        """Read messages via Topic API (payload + metadata_items). Requires a read rule for ``consumer_name`` on the topic."""
        import ydb

        if timeout_sec is None:
            timeout_sec = plain_or_under_sanitizer(30, 120)

        topic = topic_path if topic_path else self.input_topic
        consumer = self.consumer_name

        def normalize_meta(meta):
            if not meta:
                return {}
            if isinstance(meta, dict):
                return {
                    (k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)): (
                        v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v
                    )
                    for k, v in meta.items()
                }
            return dict(meta)

        def read_with_driver(driver):
            rows = []
            with driver.topic_client.reader(topic, consumer=consumer) as reader:
                deadline = time.time() + timeout_sec
                while len(rows) < messages_count and time.time() < deadline:
                    try:
                        msg = reader.receive_message(timeout=1.0)
                    except TimeoutError:
                        continue
                    meta = getattr(msg, "metadata_items", None) or {}
                    data = msg.data
                    if isinstance(data, (bytes, bytearray)):
                        data = data.decode("utf-8")
                    else:
                        data = str(data)
                    rows.append((data, normalize_meta(meta)))
                    reader.commit(msg)
            assert len(rows) == messages_count, f"expected {messages_count} messages, got {len(rows)}"
            return rows

        if endpoint is not None:
            database, fqdn = self.__unwrap_endpoint(endpoint)
            driver_config = ydb.DriverConfig(f"grpc://{fqdn}", database, auth_token="root@builtin")
            driver = ydb.Driver(driver_config)
            driver.wait(timeout=5)
            try:
                return read_with_driver(driver)
            finally:
                driver.stop()
        driver = getattr(kikimr, "driver", None) or kikimr.ydb_client.driver
        return read_with_driver(driver)

    def wait_until(self, predicate, wait_time=plain_or_under_sanitizer(10, 50)):
        deadline = time.time() + wait_time
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(1)
        return False
