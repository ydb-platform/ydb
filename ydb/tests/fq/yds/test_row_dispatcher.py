#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import logging
import time
import json
import random
import string

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig

from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, create_read_rule, delete_stream
from ydb.tests.tools.datastreams_helpers.data_plane import read_stream, write_stream
from ydb.tests.tools.fq_runner.fq_client import StreamingDisposition
from ydb.tests.tools.fq_runner.kikimr_runner import plain_or_under_sanitizer_wrapper

import ydb.public.api.protos.ydb_value_pb2 as ydb_value
import ydb.public.api.protos.draft.fq_pb2 as fq

YDS_CONNECTION = "yds"
COMPUTE_NODE_COUNT = 3
ROW_DISPATCHER_CONSUMER = 'row_dispatcher_consumer'
CONSUMER_PRAGMA = f'PRAGMA pq.Consumer="{ROW_DISPATCHER_CONSUMER}";\n'


def create_input_consumer(io):
    io.kikimr.external_ydb_client.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{ROW_DISPATCHER_CONSUMER}`;')


class Param(object):
    def __init__(self, rebalancing_timeout_sec=60):
        self.rebalancing_timeout_sec = rebalancing_timeout_sec


@pytest.fixture
def kikimr(request):
    kikimr_conf = StreamingOverKikimrConfig(
        cloud_mode=True, node_count={"/cp": TenantConfig(1), "/compute": TenantConfig(COMPUTE_NODE_COUNT)}
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.compute_plane.fq_config['row_dispatcher']['enabled'] = True
    kikimr.compute_plane.fq_config['row_dispatcher']['without_consumer'] = False
    kikimr.compute_plane.fq_config['row_dispatcher']['json_parser'] = {}
    if hasattr(request, "param"):
        kikimr.compute_plane.fq_config['row_dispatcher']['coordinator'][
            'rebalancing_timeout_sec'
        ] = request.param.rebalancing_timeout_sec
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop_mvp_mock_server()
    kikimr.stop()


def start_yds_query(kikimr, client, sql, timeout=plain_or_under_sanitizer_wrapper(30, 150)) -> str:
    query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
    client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
    kikimr.compute_plane.wait_zero_checkpoint(query_id, timeout)
    return query_id


def stop_yds_query(client, query_id):
    client.abort_query(query_id)
    client.wait_query(query_id)


def wait_actor_count(kikimr, activity, expected_count):
    deadline = time.time() + 60
    while True:
        count = 0
        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            if kikimr.compute_plane.kikimr_cluster.nodes[node_index].is_alive():
                count = count + kikimr.compute_plane.get_actor_count(node_index, activity)
            if count == expected_count:
                return node_index  # return any node
        assert time.time() < deadline, f"Waiting actor {activity} count failed, current count {count}"
        time.sleep(1)


def wait_row_dispatcher_sensor_value(kikimr, sensor, expected_count, exact_match=True):
    deadline = time.time() + 60
    while True:
        count = 0
        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            value = kikimr.compute_plane.get_sensors(node_index, "yq").find_sensor(
                {"subsystem": "row_dispatcher", "sensor": sensor}
            )
            count += value if value is not None else 0
        if count == expected_count:
            break
        if not exact_match and count > expected_count:
            break
        assert time.time() < deadline, f"Waiting sensor {sensor} value failed, current count {count}"
        time.sleep(1)
    pass


def wait_public_sensor_value(kikimr, query_id, sensor, expected_value):
    deadline = time.time() + 60
    cloud_id = "mock_cloud"
    folder_id = "my_folder"
    while True:
        count = 0
        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            value = kikimr.compute_plane.get_sensors(node_index, "yq_public").find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": sensor}
            )
            count += value if value is not None else 0
        if count >= expected_value:
            break
        assert time.time() < deadline, f"Waiting sensor {sensor} value failed, current count {count}"
        time.sleep(1)
    pass


class TestPqRowDispatcher(TestYdsBase):

    def init(self, client, topic_name_prefix, partitions=1):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics(topic_name_prefix, create_input=True, create_output=True, partitions_count=partitions)
        create_read_rule(self.input_topic, ROW_DISPATCHER_CONSUMER)

    def run_and_check(self, kikimr, client, sql, input, output, expected_predicate):
        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        self.write_stream(input)
        assert self.read_stream(len(output), topic_path=self.output_topic) == output

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert expected_predicate in issues, "Incorrect Issues: " + issues

    def test_read_raw_format_with_row_dispatcher(self, ydb_topics):
        io = ydb_topics("test_read_raw_format_with_row_dispatcher", shared=True)
        create_input_consumer(io)

        sql1 = CONSUMER_PRAGMA + Rf'''INSERT INTO {io.output}
                    SELECT * FROM {io.input} WITH (format=raw, SCHEMA (data String NOT NULL));'''

        query_id = io.start(sql1)
        data = ['{"time" = 101;}', '{"time" = 102;}']

        io.write(data)
        assert io.read(len(data), topic_path=io.output_topic) == data
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)
        io.stop(query_id)

        sql2 = CONSUMER_PRAGMA + Rf'''INSERT INTO {io.output}
                    SELECT * FROM {io.input} WITH (format=raw, SCHEMA (data String NOT NULL))
                    WHERE data != "romashka";'''

        query_id = io.start(sql2)
        data = ['{"time" = 103;}', '{"time" = 104;}']

        io.write(data)
        assert io.read(len(data), topic_path=io.output_topic) == data
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)
        io.stop(query_id)

    def test_simple_not_null(self, ydb_topics):
        io = ydb_topics("test_simple_not_null", shared=True)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": "hello1", "event": "event1"}',
            '{"time": 102, "data": "hello2", "event": "event2"}',
            '{"time": 103, "data": "hello3", "event": "event3"}',
        ]

        io.write(data)
        expected = ['101', '102', '103']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.wait_actor_count("DQ_PQ_READ_ACTOR", 1)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

    def test_metadatafields(self, ydb_topics):
        io = ydb_topics('test_metadatafields', shared=True)
        create_input_consumer(io)

        # Its not completely clear why metadatafields appear in this request(
        sql = CONSUMER_PRAGMA + Rf'''
            PRAGMA FeatureR010="prototype";
            PRAGMA config.flags("TimeOrderRecoverDelay", "-10");
            PRAGMA config.flags("TimeOrderRecoverAhead", "10");
            INSERT INTO {io.output}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL))
                MATCH_RECOGNIZE(
                    ORDER BY CAST(time as Timestamp)
                    MEASURES LAST(A.time) as b_key
                    PATTERN (A )
                    DEFINE A as A.time > 4
                );'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 100}',
            '{"time": 109}',
            '{"time": 118}',
        ]
        io.write(data)
        assert len(io.read(1, topic_path=io.output_topic)) == 1
        io.stop(query_id)

    def test_simple_optional(self, ydb_topics):
        io = ydb_topics("test_simple_optional", shared=True)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String));'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101, "data": "hello1", "event": "event1"}', '{"time": 102, "event": "event2"}']

        io.write(data)
        expected = ['101', '102']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.wait_actor_count("DQ_PQ_READ_ACTOR", 1)

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_scheme_error(self, kikimr, client):
        self.init(client, "test_scheme_error")

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"this": "is", not json}', '{"noch einmal / nicht json"}']
        self.write_stream(data)

        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "Failed to parse json message for offset" in issues, "Incorrect Issues: " + issues

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 0)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)
        data = ['{"time": 101, "data": "hello1", "event": "event1"}']
        self.write_stream(data)
        expected = ['101']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected
        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_nested_types(self, kikimr, client):
        self.init(client, "test_nested_types")

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT data FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data Json NOT NULL, event String NOT NULL))
                WHERE event = "event1" or event = "event2" or event = "event4";'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        large_string = "abcdefghjkl1234567890+abcdefghjkl1234567890"
        data = [
            '{"time": 101, "data": {"key": "value", "second_key":"' + large_string + '"}, "event": "event1"}',
            '{"time": 102, "data": ["key1", "key2", "' + large_string + '"], "event": "event2"}',
            '{"time": 103, "data": ["' + large_string + '"], "event": "event3"}',
            '{"time": 104, "data": "' + large_string + '", "event": "event4"}',
        ]

        self.write_stream(data)
        expected = [
            '{"key": "value", "second_key":"' + large_string + '"}',
            '["key1", "key2", "' + large_string + '"]',
            '"' + large_string + '"',
        ]
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        stop_yds_query(client, query_id)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert "Row dispatcher will use the predicate:" in issues, "Incorrect Issues: " + issues

    @yq_v1
    def test_nested_structured_types(self, kikimr, client):
        self.init(client, "test_nested_structured_types")

        large_string = "abcdefghjkl1234567890+abcdefghjkl1234567890"
        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(data))))
             FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (
                    format = json_each_row,
                    SCHEMA = (
                        time UInt64 NOT NULL,
                        data Struct<key: String, second_key: Tuple<Int64, Bool?>> NOT NULL,
                        event String NOT NULL
                    )
                )
                WHERE (event = "event1" or event = "event2" or event = "event4" or event = "event5")
                  AND (data.key = "value" OR data.key = "ev2" OR data.key = "{large_string}")
                  AND data.second_key.0 > 0 AND data.second_key.1 IS DISTINCT FROM true;
        '''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": {"key": "value", "second_key":[1, false]}, "event": "event1"}',
            '{"time": 102, "data": {"second_key":[2], "key":"' + large_string + '"}, "event": "event2"}',
            '{"time": 103, "data": {"key":"ev2", "second_key":[3]}, "event": "event3"}',
            '{"time": 104, "data": {"key":"ev2", "second_key":[42, true]}, "event": "event4"}',
            '{"time": 105, "data": {"key":"ev2", "second_key":[42, false]}, "event": "event5"}',
        ]

        self.write_stream(data)
        expected = [
            '{"key":"value","second_key":[1,false]}',
            '{"key":"' + large_string + '","second_key":[2,null]}',
            '{"key":"ev2","second_key":[42,false]}',
        ]
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        stop_yds_query(client, query_id)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert "Row dispatcher will use the predicate:" in issues and "second_key" in issues, (
            "Incorrect Issues: " + issues
        )

    def test_nested_types_without_predicate(self, ydb_topics):
        io = ydb_topics('test_nested_types_without_predicate', shared=True)
        create_input_consumer(io)

        sql = (
            CONSUMER_PRAGMA
            + Rf'''
            INSERT INTO {io.output}
            SELECT data FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data Json NOT NULL, event String NOT NULL));'''
        )

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": {"key": "value"}, "event": "event1"}',
            '{"time": 102, "data": ["key1", "key2"], "event": "event2"}',
        ]

        io.write(data)
        expected = ['{"key": "value"}', '["key1", "key2"]']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.wait_actor_count("DQ_PQ_READ_ACTOR", 1)
        io.stop(query_id)

    @yq_v1
    def test_filters_non_optional_field(self, kikimr, client):
        self.init(client, "test_filters_non_optional_field")

        sql = (
            CONSUMER_PRAGMA
            + Rf'''
            PRAGMA AnsiLike;
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String NOT NULL, event String NOT NULL, nested Json NOT NULL)) WHERE '''
        )
        data = [
            '{"time": 101, "data": "hello1", "event": "event1", "nested": {"xyz": "key"}}',
            '{"time": 102, "data": "hello2", "event": "event2", "nested": ["abc", "key"]}',
        ]
        expected = ['102']
        filter = "time > 101;"
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: (`time` > 101)')
        filter = 'data = "hello2"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: (`data` = \\"hello2\\")')
        filter = ' event IS NOT DISTINCT FROM "event2"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (`event` IS NOT DISTINCT FROM \\"event2\\")'
        )
        filter = ' event IS DISTINCT FROM "event1"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (`event` IS DISTINCT FROM \\"event1\\")'
        )
        filter = 'event IN ("event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: (`event` IN (\\"event2\\"))')
        filter = 'event NOT IN ("event1", "event3")'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (NOT (`event` IN (\\"event1\\", \\"event3\\")))'
        )
        filter = 'event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: (`event` IN (\\"1\\"')
        filter = ' event IS DISTINCT FROM data AND event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((`event` IS DISTINCT FROM `data`) AND (`event` IN (\\"1\\"',
        )
        filter = ' IF(event = "event2", event IS DISTINCT FROM data, FALSE)'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: IF((`event` = \\"event2\\"), (`event` IS DISTINCT FROM `data`), FALSE)',
        )
        filter = ' nested REGEXP ".*abc.*"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (CAST(`nested` AS String) REGEXP \\".*abc.*\\")'
        )
        filter = ' CAST(nested AS String) REGEXP ".*abc.*"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (CAST(`nested` AS String) REGEXP \\".*abc.*\\")'
        )
        filter = 'event LIKE "event2%"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: StartsWith(`event`, \\"event2\\")')
        filter = 'event LIKE "%event2"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: EndsWith(`event`, \\"event2\\")')
        filter = 'event LIKE "%event2%"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: String::Contains(`event`, \\"event2\\")'
        )
        filter = ' (data = "hello2") IS NOT DISTINCT FROM true'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((`data` = \\"hello2\\") IS NOT DISTINCT FROM TRUE)',
        )
        filter = ' (data REGEXP ".*hello2.*") IS NOT DISTINCT FROM true'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((`data` REGEXP \\".*hello2.*\\") IS NOT DISTINCT FROM TRUE)',
        )
        filter = ' (CAST(data AS Utf8) REGEXP ".*hello2.*") IS NOT DISTINCT FROM true'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            R'predicate: ((IF((CAST(`data` AS Utf8?) IS NOT NULL), CAST(CAST(`data` AS Utf8?) AS String), NULL) REGEXP \".*hello2.*\") IS NOT DISTINCT FROM TRUE)',
        )  # YQ-5727
        filter = ' CAST(`time` AS Date) > Date("1970-04-12")'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            R'predicate: (CAST(`time` AS Date?) > Date(\"1970-04-12\"))',
        )
        filter = ' CAST(`time` AS Timestamp) > Timestamp("1970-01-01T00:00:00.000101Z")'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            R'predicate: (CAST(`time` AS Timestamp?) > Timestamp(\"1970-01-01T00:00:00.000101Z\"))',
        )  # YQ-5738
        filter = ' MIN_OF(`time`, `time` + 5) >= 102 AND MAX_OF(`time`, `time` - 5) < 103'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            R'predicate: ((MIN_OF(`time`, (`time` + 5)) >= 102) AND (MAX_OF(`time`, (`time` - 5)) < 103))',
        )
        filter = ' `time` IN (102, 103, 104)'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            R'predicate: (`time` IN (102, 103, 104))',
        )
        filter = ' CAST(`time` AS Interval) > Interval("PT0.000101S")'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            R'predicate: (CAST(`time` AS Interval?) > Interval(\"PT0.000101S\"))',
        )
        filter = ' CAST(`time` AS Timestamp) - Interval("-PT0.000001S") > Timestamp("1970-01-01T00:00:00.000102Z")'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            R'predicate: ((CAST(`time` AS Timestamp?) - Interval(\"-PT0.000001S\")) > Timestamp(\"1970-01-01T00:00:00.000102Z\"))',
        )

    @yq_v1
    def test_filters_optional_field(self, kikimr, client):
        self.init(client, "test_filters_optional_field")

        sql = (
            CONSUMER_PRAGMA
            + Rf'''
            PRAGMA AnsiLike;
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String, event String, flag Bool, field1 UInt8, field2 Int64, nested Json)) WHERE '''
        )
        data = [
            '{"time": 101, "data": "hello1", "event": "event1", "flag": false, "field1": 5, "field2": 5, "nested": {"xyz": "key"}}',
            '{"time": 102, "data": "hello2", "event": "event2", "flag": true, "field1": 5, "field2": 1005, "nested": ["abc", "key"]}',
        ]
        expected = ['102']
        filter = 'data = "hello2"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: (`data` = \\"hello2\\")')
        filter = 'flag'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: `flag`')
        filter = 'time * (field2 - field1) != 0'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: ((`time` * (`field2` - `field1`)) <> 0)'
        )
        filter = '(field1 % field2) / 5 = 1'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: (((`field1` % `field2`) / 5) = 1)')
        filter = ' event IS NOT DISTINCT FROM "event2"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (`event` IS NOT DISTINCT FROM \\"event2\\")'
        )
        filter = ' event IS DISTINCT FROM "event1"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (`event` IS DISTINCT FROM \\"event1\\")'
        )
        filter = ' field1 IS DISTINCT FROM field2'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (`field1` IS DISTINCT FROM `field2`)'
        )
        filter = 'time == 102 OR (field2 IS NOT DISTINCT FROM 1005 AND Random(field1) < 10.0)'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((`time` = 102) OR (`field2` IS NOT DISTINCT FROM 1005))',
        )
        filter = 'event IN ("event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: (`event` IN (\\"event2\\"))')
        filter = 'event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: (`event` IN (\\"1\\"')
        filter = ' event IS DISTINCT FROM data AND event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((`event` IS DISTINCT FROM `data`) AND COALESCE((`event` IN (\\"1\\"',
        )
        filter = ' IF(event == "event2", event IS DISTINCT FROM data, FALSE)'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: IF(COALESCE((`event` = \\"event2\\"), FALSE), (`event` IS DISTINCT FROM `data`), FALSE)',
        )
        filter = ' COALESCE(event = "event2", TRUE)'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: COALESCE((`event` = \\"event2\\"), TRUE)'
        )
        filter = ' COALESCE(event = "event2", data = "hello2", TRUE)'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: COALESCE((`event` = \\"event2\\"), (`data` = \\"hello2\\"), TRUE)',
        )
        filter = " event ?? '' REGEXP @@e.*e.*t2@@"
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (COALESCE(`event`, \\"\\") REGEXP \\"e.*e.*t2\\")'
        )
        filter = " event ?? '' NOT REGEXP @@e.*e.*t1@@"
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: (NOT (COALESCE(`event`, \\"\\") REGEXP \\"e.*e.*t1\\"))',
        )
        filter = " event ?? '' REGEXP data ?? '' OR time = 102"
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((COALESCE(`event`, \\"\\") REGEXP COALESCE(`data`, \\"\\")) OR (`time` = 102))',
        )
        filter = ' nested REGEXP ".*abc.*"'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: (IF((`nested` IS NOT NULL), CAST(`nested` AS String), NULL) REGEXP \\".*abc.*\\")',
        )
        filter = ' CAST(nested AS String) REGEXP ".*abc.*"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: (CAST(`nested` AS String?) REGEXP \\".*abc.*\\")'
        )
        filter = 'event LIKE "event2%"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: StartsWith(`event`, \\"event2\\")')
        filter = 'event LIKE "%event2"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: EndsWith(`event`, \\"event2\\")')
        filter = 'event LIKE "%event2%"'
        self.run_and_check(
            kikimr, client, sql + filter, data, expected, 'predicate: String::Contains(`event`, \\"event2\\")'
        )
        filter = ' (data = "hello2") IS NOT DISTINCT FROM true'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((`data` = \\"hello2\\") IS NOT DISTINCT FROM TRUE)',
        )
        filter = ' (data REGEXP ".*hello2.*") IS NOT DISTINCT FROM true'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((`data` REGEXP \\".*hello2.*\\") IS NOT DISTINCT FROM TRUE)',
        )
        filter = ' (data in ("hello2", "hello3")) IS NOT DISTINCT FROM true'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: ((`data` IN (\\"hello2\\", \\"hello3\\")) IS NOT DISTINCT FROM TRUE)',
        )
        # YQ-5708
        filter = ' COALESCE(event, data) IS DISTINCT FROM "event1"'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: (COALESCE(`event`, `data`) IS DISTINCT FROM \\"event1\\")',
        )
        filter = ' UNWRAP(coalesce(event, data)) IS distinct FROM "event1"'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: (Unwrap(COALESCE(`event`, `data`)) IS DISTINCT FROM \\"event1\\")',
        )
        filter = ' tobytes(coalesce(cast(event as Utf8), cast(data as utf8))) IS distinct FROM "event1"'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            'predicate: (CAST(COALESCE(CAST(`event` AS Utf8?), CAST(`data` AS Utf8?)) AS String) IS DISTINCT FROM \\"event1\\")',
        )
        filter = ' (CAST(data AS Utf8) REGEXP ".*hello2.*") IS NOT DISTINCT FROM true'
        self.run_and_check(
            kikimr,
            client,
            sql + filter,
            data,
            expected,
            R'predicate: ((IF((CAST(`data` AS Utf8?) IS NOT NULL), CAST(CAST(`data` AS Utf8?) AS String), NULL) REGEXP \".*hello2.*\") IS NOT DISTINCT FROM TRUE)',
        )  # YQ-5727

    @yq_v1
    def test_filter_missing_fields(self, kikimr, client):
        self.init(client, "test_filter_missing_fields")

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, `data@data` String, event String NOT NULL))
                WHERE `data@data` IS NULL;'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "event": "event1"}',
            '{"time": 102, "data@data": null, "event": "event2"}',
            '{"time": 103, "data@data": "", "event": "event2"}',
            '{"time": 104, "data@data": "null", "event": "event2"}',
        ]

        self.write_stream(data)
        expected = ['101', '102']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        stop_yds_query(client, query_id)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert "Row dispatcher will use the predicate:" in issues, "Incorrect Issues: " + issues

    def test_filter_use_unsupported_predicate(self, ydb_topics):
        io = ydb_topics('test_filter_use_unsupported_predicate', shared=True)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String NOT NULL, event String NOT NULL))
                WHERE  event LIKE 'event2%';'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 102, "data": "hello2", "event": "event2"}',
        ]

        io.write(data)
        assert io.read(1, topic_path=io.output_topic) == ['102']
        io.wait_actor_count("DQ_PQ_READ_ACTOR", 1)
        io.stop(query_id)

    @yq_v1
    def test_filter_with_mr(self, kikimr, client):
        self.init(client, "test_filter_with_mr")

        sql = CONSUMER_PRAGMA + Rf'''
            pragma FeatureR010="prototype";
            pragma config.flags("TimeOrderRecoverDelay", "-10");
            pragma config.flags("TimeOrderRecoverAhead", "10");

            $data =
                SELECT * FROM {YDS_CONNECTION}.`{self.input_topic}`
                    WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, event_class String NOT NULL, event_type UInt64 NOT NULL))
                    WHERE event_class = "event_class2";

            $match =
                SELECT * FROM $data
                MATCH_RECOGNIZE(
                    ORDER BY CAST(time as Timestamp)
                    MEASURES
                        LAST(M1.event_type) as event_type
                    ONE ROW PER MATCH
                    PATTERN ( M1 )
                    DEFINE
                        M1 as
                            M1.event_class = "event_class2"
                );

            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $match;
            '''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 100, "event_class": "event_class1", "event_type": 1}',
            '{"time": 105, "event_class": "event_class2", "event_type": 2}',
            '{"time": 110, "event_class": "event_class2", "event_type": 3}',
            '{"time": 116, "event_class": "event_class2", "event_type": 4}',
        ]

        self.write_stream(data)
        expected = ['{"event_type":2}']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        stop_yds_query(client, query_id)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert "Row dispatcher will use the predicate: (`event_class` =" in issues, "Incorrect Issues: " + issues

    def test_start_new_query(self, ydb_topics):
        io = ydb_topics('test_start_new_query', shared=True)
        create_input_consumer(io)
        external = io.kikimr.external_ydb_client
        output_topic1 = io.output_topic
        output_topic2 = io.name + '_output2'
        output_topic3 = io.name + '_output3'
        extra_topics = [output_topic2, output_topic3]
        try:
            for topic in extra_topics:
                external.query(f'CREATE TOPIC `{topic}`;')
                external.query(f'ALTER TOPIC `{topic}` ADD CONSUMER `{io.consumer_name}`;')

            sql1 = CONSUMER_PRAGMA + Rf'''
                INSERT INTO `{io.name}`.`{output_topic1}`
                SELECT Cast(time as String) FROM {io.input}
                    WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''
            sql2 = CONSUMER_PRAGMA + Rf'''
                INSERT INTO `{io.name}`.`{output_topic2}`
                SELECT Cast(time as String) FROM {io.input}
                    WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''
            query_id1 = io.start(sql1)
            query_id2 = io.start(sql2)
            io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 1)

            data = [
                '{"time": 101, "data": "hello1", "event": "event1"}',
                '{"time": 102, "data": "hello2", "event": "event2"}',
            ]
            io.write(data)
            expected = ['101', '102']
            assert io.read(len(expected), topic_path=output_topic1) == expected
            assert io.read(len(expected), topic_path=output_topic2) == expected
            io.wait_actor_count('DQ_PQ_READ_ACTOR', 2)
            assert not io.read(1, topic_path=output_topic1, timeout=1)
            assert not io.read(1, topic_path=output_topic2, timeout=1)

            sql3 = CONSUMER_PRAGMA + Rf'''
                INSERT INTO `{io.name}`.`{output_topic3}`
                SELECT event FROM {io.input}
                    WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, event String NOT NULL));'''
            query_id3 = io.start(sql3)

            data = [
                '{"time": 103, "data": "hello3", "event": "event3"}',
                '{"time": 104, "data": "hello4", "event": "event4"}',
            ]
            io.write(data)
            expected12 = ['103', '104']
            expected3 = ['event3', 'event4']
            assert io.read(len(expected12), topic_path=output_topic1) == expected12
            assert io.read(len(expected12), topic_path=output_topic2) == expected12
            assert io.read(len(expected3), topic_path=output_topic3) == expected3
            io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 1)
            for topic in [output_topic1, output_topic2, output_topic3]:
                assert not io.read(1, topic_path=topic, timeout=1)

            io.stop(query_id1)
            io.stop(query_id2)
            io.stop(query_id3)
            io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)
        finally:
            for query in reversed(io.queries):
                io.kikimr.ydb_client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;')
            for topic in extra_topics:
                external.query(f'DROP TOPIC IF EXISTS `{topic}`;')

    @yq_v1
    def test_stop_start(self, kikimr, client):
        self.init(client, "test_stop_start", 10)

        sql1 = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql1)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 10)

        data = ['{"time": 101}', '{"time": 102}']
        self.write_stream(data)
        expected = ['101', '102']
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == sorted(expected)

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2
        )
        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

        client.modify_query(
            query_id,
            "simple",
            sql1,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        data = ['{"time": 103}', '{"time": 104}']

        self.write_stream(data)
        expected = ['103', '104']
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == sorted(expected)

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_stop_start2(self, kikimr, client):
        self.init(client, "test_stop_start")
        sql1 = CONSUMER_PRAGMA + Rf'''INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
                    SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                        WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
        query_id1 = start_yds_query(kikimr, client, sql1)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)
        self.write_stream(['{"time": 101}', '{"time": 102}'])
        expected = ['101', '102']
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == sorted(expected)

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id1, kikimr.compute_plane.get_completed_checkpoints(query_id1) + 2
        )
        stop_yds_query(client, query_id1)

        client.modify_query(
            query_id1,
            "simple",
            sql1,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id1, fq.QueryMeta.RUNNING)
        query_id2 = start_yds_query(kikimr, client, sql1)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        time.sleep(10)
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id1, kikimr.compute_plane.get_completed_checkpoints(query_id1) + 2
        )
        stop_yds_query(client, query_id1)
        stop_yds_query(client, query_id2)

        client.modify_query(
            query_id1,
            "simple",
            sql1,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id1, fq.QueryMeta.RUNNING)
        client.modify_query(
            query_id2,
            "simple",
            sql1,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id2, fq.QueryMeta.RUNNING)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        self.write_stream(['{"time": 103}', '{"time": 104}'])
        expected = ['103', '104', '103', '104']
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == sorted(expected)

        stop_yds_query(client, query_id1)
        stop_yds_query(client, query_id2)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_stop_start_with_filter(self, kikimr, client):
        self.init(client, "test_stop_start_with_filter")

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL))
                WHERE time > 200;'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101}', '{"time": 102}']
        self.write_stream(data)

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id,
            kikimr.compute_plane.get_completed_checkpoints(query_id)
            + 10,  # long sleep to send status from topic_session to read_actor
        )
        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL));'''

        client.modify_query(
            query_id,
            "simple",
            sql,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        data = ['{"time": 203}', '{"time": 204}']
        self.write_stream(data)
        expected = ['203', '204']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @pytest.mark.parametrize('ydb_cluster', [{'compute_nodes': 3}], indirect=True)
    def test_restart_compute_node(self, ydb_topics):
        io = ydb_topics('test_restart_compute_node', shared=True, partitions=4)
        create_input_consumer(io)
        node_ids = sorted(io.kikimr.cluster.slots)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 4)

        io.write([Rf'''{{"time": {c}}}''' for c in range(100, 102)], partition_key="partition_key1")
        io.write([Rf'''{{"time": {c}}}''' for c in range(102, 104)], partition_key="partition_key2")
        io.write([Rf'''{{"time": {c}}}''' for c in range(104, 106)], partition_key="partition_key3")
        io.write([Rf'''{{"time": {c}}}''' for c in range(106, 108)], partition_key="partition_key4")

        expected = [Rf'''{c}''' for c in range(100, 108)]
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == expected

        io.checkpoint(query_id, count=3)

        node_index = 2
        logging.debug("Restart compute node {}".format(node_index))
        io.restart_node(io.kikimr, node_ids[node_index - 1])

        io.checkpoint(query_id, count=3)

        io.write([Rf'''{{"time": {c}}}''' for c in range(108, 110)], partition_key="partition_key1")
        io.write([Rf'''{{"time": {c}}}''' for c in range(110, 112)], partition_key="partition_key2")
        io.write([Rf'''{{"time": {c}}}''' for c in range(112, 114)], partition_key="partition_key3")
        io.write([Rf'''{{"time": {c}}}''' for c in range(114, 116)], partition_key="partition_key4")

        expected = [Rf'''{c}''' for c in range(108, 116)]
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == expected

        io.checkpoint(query_id, count=3)

        node_index = 1
        logging.debug("Restart compute node {}".format(node_index))
        io.restart_node(io.kikimr, node_ids[node_index - 1])

        io.checkpoint(query_id, count=3)

        io.write([Rf'''{{"time": {c}}}''' for c in range(116, 118)], partition_key="partition_key1")
        io.write([Rf'''{{"time": {c}}}''' for c in range(118, 120)], partition_key="partition_key2")
        io.write([Rf'''{{"time": {c}}}''' for c in range(120, 122)], partition_key="partition_key3")
        io.write([Rf'''{{"time": {c}}}''' for c in range(122, 124)], partition_key="partition_key4")

        expected = [Rf'''{c}''' for c in range(116, 124)]
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == expected

        io.checkpoint(query_id, count=3)

        node_index = 3
        logging.debug("Restart compute node {}".format(node_index))
        io.restart_node(io.kikimr, node_ids[node_index - 1])

        io.checkpoint(query_id, count=3)

        io.write([Rf'''{{"time": {c}}}''' for c in range(124, 126)], partition_key="partition_key1")
        io.write([Rf'''{{"time": {c}}}''' for c in range(126, 128)], partition_key="partition_key2")
        io.write([Rf'''{{"time": {c}}}''' for c in range(128, 130)], partition_key="partition_key3")
        io.write([Rf'''{{"time": {c}}}''' for c in range(130, 132)], partition_key="partition_key4")

        expected = [Rf'''{c}''' for c in range(124, 132)]
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == expected

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_3_sessions(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_3_session", create_output=False)
        create_read_rule(self.input_topic, ROW_DISPATCHER_CONSUMER)

        output_topic1 = "test_3_session1"
        output_topic2 = "test_3_session2"
        output_topic3 = "test_3_session3"
        create_stream(output_topic1, partitions_count=1)
        create_read_rule(output_topic1, self.consumer_name)

        create_stream(output_topic2, partitions_count=1)
        create_read_rule(output_topic2, self.consumer_name)

        create_stream(output_topic3, partitions_count=1)
        create_read_rule(output_topic3, self.consumer_name)

        sql1 = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic1}`
            SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
        sql2 = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic2}`
            SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        sql3 = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic3}`
            SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
        query_id1 = start_yds_query(kikimr, client, sql1)
        query_id2 = start_yds_query(kikimr, client, sql2)
        query_id3 = start_yds_query(kikimr, client, sql3)

        data = ['{"time":101}', '{"time":102}']

        self.write_stream(data)
        expected = data
        assert self.read_stream(len(expected), topic_path=output_topic1) == expected
        assert self.read_stream(len(expected), topic_path=output_topic2) == expected
        assert self.read_stream(len(expected), topic_path=output_topic3) == expected
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id1, kikimr.compute_plane.get_completed_checkpoints(query_id1) + 2
        )
        stop_yds_query(client, query_id1)

        data = ['{"time":103}', '{"time":104}']
        self.write_stream(data)
        expected = data
        assert not read_stream(output_topic1, 1, True, self.consumer_name, timeout=1)
        assert self.read_stream(len(expected), topic_path=output_topic2) == expected
        assert self.read_stream(len(expected), topic_path=output_topic3) == expected

        client.modify_query(
            query_id1,
            "simple",
            sql1,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id1, fq.QueryMeta.RUNNING)

        assert self.read_stream(len(expected), topic_path=output_topic1) == expected

        data = ['{"time":105}', '{"time":106}']
        self.write_stream(data)
        expected = data
        assert self.read_stream(len(expected), topic_path=output_topic1) == expected
        assert self.read_stream(len(expected), topic_path=output_topic2) == expected
        assert self.read_stream(len(expected), topic_path=output_topic3) == expected

        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        stop_yds_query(client, query_id1)
        stop_yds_query(client, query_id2)
        stop_yds_query(client, query_id3)

        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    def test_many_partitions(self, ydb_topics):
        io = ydb_topics('test_many_partitions', shared=True, partitions=4)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 4)

        input_messages1 = [Rf'''{{"time": {c}}}''' for c in range(100, 110)]
        io.write(input_messages1, partition_key="partition_key1")

        input_messages2 = [Rf'''{{"time": {c}}}''' for c in range(110, 120)]
        io.write(input_messages2, partition_key="partition_key2")

        input_messages3 = [Rf'''{{"time": {c}}}''' for c in range(120, 130)]
        io.write(input_messages3, partition_key="partition_key3")

        input_messages4 = [Rf'''{{"time": {c}}}''' for c in range(130, 140)]
        io.write(input_messages4, partition_key="partition_key4")

        expected = [Rf'''{c}''' for c in range(100, 140)]
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == expected

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_2_connection(self, kikimr, client):
        client.create_yds_connection("name1", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True)
        client.create_yds_connection("name2", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True)
        self.init_topics("test_2_connection", partitions_count=2)
        create_read_rule(self.input_topic, ROW_DISPATCHER_CONSUMER + '_1')
        create_read_rule(self.input_topic, ROW_DISPATCHER_CONSUMER + '_2')

        sql1 = (
            f'PRAGMA pq.Consumer="{ROW_DISPATCHER_CONSUMER}_1";\n'
            + Rf'''
            INSERT INTO `name1`.`{self.output_topic}`
            SELECT Cast(time as String) FROM `name1`.`{self.input_topic}` WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
        )
        sql2 = (
            f'PRAGMA pq.Consumer="{ROW_DISPATCHER_CONSUMER}_2";\n'
            + Rf'''
            INSERT INTO `name2`.`{self.output_topic}`
            SELECT Cast(time as String) FROM `name2`.`{self.input_topic}` WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
        )
        query_id1 = start_yds_query(kikimr, client, sql1)
        query_id2 = start_yds_query(kikimr, client, sql2)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 4)

        input_messages1 = [Rf'''{{"time": {c}}}''' for c in range(100, 110)]
        write_stream(self.input_topic, input_messages1, "partition_key1")

        input_messages2 = [Rf'''{{"time": {c}}}''' for c in range(110, 120)]
        write_stream(self.input_topic, input_messages2, "partition_key2")

        expected = [Rf'''{c}''' for c in range(100, 120)]
        expected = [item for item in expected for i in range(2)]
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == expected

        stop_yds_query(client, query_id1)
        stop_yds_query(client, query_id2)

        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_sensors(self, kikimr, client):
        self.init(client, "test_sensors")

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)

        self.write_stream(['{"time": 101}'])
        assert self.read_stream(1, topic_path=self.output_topic) == ['101']

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_COMPILE_SERVICE", COMPUTE_NODE_COUNT)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_FORMAT_HANDLER", 1)
        wait_row_dispatcher_sensor_value(kikimr, "ClientsCount", 1)
        wait_row_dispatcher_sensor_value(kikimr, "RowsSent", 1, exact_match=False)
        wait_row_dispatcher_sensor_value(kikimr, "IncomingRequests", 1, exact_match=False)

        wait_public_sensor_value(kikimr, query_id, "query.input_filtered_bytes", 1)
        wait_public_sensor_value(kikimr, query_id, "query.source_input_filtered_records", 1)
        wait_public_sensor_value(kikimr, query_id, "query.input_queued_bytes", 0)
        wait_public_sensor_value(kikimr, query_id, "query.source_input_queued_records", 0)
        stop_yds_query(client, query_id)

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 0)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)
        wait_row_dispatcher_sensor_value(kikimr, "ClientsCount", 0)

        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)
        filtered_bytes = stat['Graph=0']['IngressFilteredBytes']['sum']
        filtered_rows = stat['Graph=0']['IngressFilteredRows']['sum']
        assert filtered_bytes > 1 and filtered_rows > 0

    @yq_v1
    @pytest.mark.skip(reason="Is not implemented")
    def test_group_by_hop_restart_query(self, kikimr, client):
        self.init(client, "test_group_by_hop_restart")

        sql1 = CONSUMER_PRAGMA + Rf'''
            $data = SELECT * FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time String NOT NULL, project String NOT NULL));
            $hop_data = SELECT
                    CAST(HOP_END() as String) as time,
                    COUNT(*) as count
                FROM $data
                GROUP BY
                    HOP(CAST(time as TimeStamp), "PT10S", "PT10S", "PT0S");
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $hop_data;'''

        query_id = start_yds_query(kikimr, client, sql1)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": "2025-04-23T09:00:00.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:01.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:02.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:03.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:04.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:15.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:16.000000Z", "project": "project1"}',
        ]
        self.write_stream(data)
        expected = ['{"count":5,"time":"2025-04-23T09:00:10Z"}']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2
        )
        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

        data = [
            '{"time": "2025-04-23T09:00:17.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:18.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:21.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:25.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:31.000000Z", "project": "project1"}',
        ]
        self.write_stream(data)

        client.modify_query(
            query_id,
            "simple",
            sql1,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.FROM_LAST_CHECKPOINT,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        expected = ['{"count":4,"time":"2025-04-23T09:00:20Z"}', '{"count":2,"time":"2025-04-23T09:00:30Z"}']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    def test_group_by_hop_restart_node(self, ydb_topics):
        io = ydb_topics('test_group_by_hop_restart_node', shared=True)
        create_input_consumer(io)

        sql1 = CONSUMER_PRAGMA + Rf'''
            $data = SELECT * FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time String NOT NULL, project String NOT NULL));
            $hop_data = SELECT
                    project,
                    CAST(HOP_END() as String) as time,
                    COUNT(*) as count
                FROM $data
                GROUP BY
                    HOP(CAST(time as TimeStamp), "PT10S", "PT10S", "PT0S"), project;
            INSERT INTO {io.output}
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $hop_data;'''

        query_id = io.start(sql1)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": "2025-04-23T09:00:00.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:04.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:05.000000Z", "project": "project2"}',
            '{"time": "2025-04-23T09:00:15.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:16.000000Z", "project": "project2"}',
        ]
        io.write(data)
        expected = [
            '{"count":2,"project":"project1","time":"2025-04-23T09:00:10Z"}',
            '{"count":1,"project":"project2","time":"2025-04-23T09:00:10Z"}',
        ]
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.checkpoint(query_id, count=3)

        node_id = next(
            node_id
            for node_id in io.kikimr.cluster.slots
            if io.get_actor_count(io.kikimr, node_id, 'DQ_PQ_READ_ACTOR') > 0
        )
        node = io.kikimr.cluster.slots[node_id]
        node.stop()

        data = [
            '{"time": "2025-04-23T09:00:17.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:18.000000Z", "project": "project2"}',
            '{"time": "2025-04-23T09:00:21.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:25.000000Z", "project": "project2"}',
        ]
        try:
            io.write(data)
        finally:
            node.set_log_file_prefix('logfile_restarted_')
            node.start()
        io.checkpoint(query_id, count=3)

        expected = [
            '{"count":2,"project":"project1","time":"2025-04-23T09:00:20Z"}',
            '{"count":2,"project":"project2","time":"2025-04-23T09:00:20Z"}',
        ]
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_huge_messages(self, kikimr, client):
        self.init(client, "test_huge")

        sql = CONSUMER_PRAGMA + Rf'''
            $data = SELECT * FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, sql String NOT NULL, event String NOT NULL))
                WHERE event = "event1" or event = "event2";

            $data = SELECT time, LENGTH(sql) AS len FROM $data;

            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $data;
                '''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        large_string = "abcdefghjkl1234567890"
        huge_string = large_string * (1000 * 1000 // len(large_string) + 2)
        assert len(huge_string) > 1000 * 1000

        data = [
            '{"time": 101, "sql":"' + large_string + '", "event": "event1"}',
            '{"time": 102, "sql":"' + huge_string + '", "event": "event2"}',
            '{"time": 103, "sql": "' + large_string + '", "event": "event3"}',
            '{"time": 104, "sql": "' + huge_string + '", "event": "event4"}',
            '{"time": 105, "sql":"' + large_string + '", "event": "event1"}',
            '{"time": 106, "sql":"' + huge_string + '", "event": "event2"}',
        ]

        self.write_stream(data)
        expected = [
            f'{{"len":{len(large_string)},"time":101}}',
            f'{{"len":{len(huge_string)},"time":102}}',
            f'{{"len":{len(large_string)},"time":105}}',
            f'{{"len":{len(huge_string)},"time":106}}',
        ]
        # TODO: normalize json and tolerate order mismatch

        received = self.read_stream(len(expected), topic_path=self.output_topic)

        # wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        stop_yds_query(client, query_id)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert "Row dispatcher will use the predicate:" in issues, "Incorrect Issues: " + issues
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "Failed to parse json message for offset" not in issues, "Incorrect Issues: " + issues

        assert received == expected

    @yq_v1
    @pytest.mark.parametrize("use_binding", [False, True], ids=["with_option", "bindings"])
    def test_json_errors(self, kikimr, client, use_binding):
        connection_response = client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics(f"test_json_errors_{use_binding}", create_input=True, create_output=True, partitions_count=1)
        create_read_rule(self.input_topic, ROW_DISPATCHER_CONSUMER)

        time_type = ydb_value.Column(name="time", type=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.INT32))
        data_type = ydb_value.Column(name="data", type=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.STRING))
        null_type = ydb_value.Column(
            name="null_field",
            type=ydb_value.Type(
                optional_type=ydb_value.OptionalType(item=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.STRING))
            ),
        )

        if use_binding:
            client.create_yds_binding(
                name="my_binding",
                stream=self.input_topic,
                format="json_each_row",
                connection_id=connection_response.result.connection_id,
                columns=[time_type, data_type, null_type],
                format_setting={"skip.json.errors": "true"},
            )

        if use_binding:
            sql = CONSUMER_PRAGMA + Rf'''
                INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
                SELECT data || COALESCE(cast(null_field as String), "_null") FROM bindings.`my_binding`;'''
        else:
            sql = (
                CONSUMER_PRAGMA
                + Rf'''
                INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
                SELECT data || COALESCE(cast(null_field as String), "_null") FROM {YDS_CONNECTION}.`{self.input_topic}`
                    WITH (format=json_each_row, `skip.json.errors` = "true", SCHEMA (time Int32 NOT NULL, data String NOT NULL, null_field String));'''
            )

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": "hello1"}',
            '{"time": 102, "data": 7777}',
            '{"time": 103, "data": "hello2"}',
            '{"time": 104, "data": "hello3", "null_field": null}',
        ]

        self.write_stream(data, partition_key="key")
        expected = ['hello1_null', 'hello2_null', 'hello3_null']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        deadline = time.time() + 30
        while True:
            count = 0
            for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
                value = kikimr.compute_plane.get_sensors(node_index, "yq").find_sensor(
                    {
                        "subsystem": "row_dispatcher",
                        "partition": "0",
                        "format": "json_each_row",
                        "sensor": "ParsingErrors",
                    }
                )
                count += value if value is not None else 0
            if count > 0:
                break
            assert time.time() < deadline, f"Waiting sensor ParsingErrors value failed, current count {count}"
            time.sleep(1)

        while True:
            count = 0
            for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
                value = kikimr.compute_plane.get_sensors(node_index, "yq").find_sensor(
                    {"subsystem": "row_dispatcher", "topic": f"{self.input_topic}", "sensor": "JsonParsingErrors"}
                )
                count += value if value is not None else 0
            if count > 0:
                break
            assert time.time() < deadline, f"Waiting sensor JsonParsingErrors value failed, current count {count}"
            time.sleep(1)
        stop_yds_query(client, query_id)

    @yq_v1
    def test_delete_topic(self, kikimr, client):
        self.init(client, "test_delete_topic")

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": "hello1", "event": "event1"}',
            '{"time": 102, "data": "hello2", "event": "event2"}',
            '{"time": 103, "data": "hello3", "event": "event3"}',
        ]

        self.write_stream(data)
        expected = ['101', '102', '103']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2
        )
        stop_yds_query(client, query_id)

        delete_stream(self.input_topic)
        create_stream(self.input_topic)
        create_read_rule(self.input_topic, ROW_DISPATCHER_CONSUMER)

        client.modify_query(
            query_id,
            "simple",
            sql,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )

        data = [
            '{"time": 101, "data": "hello1", "event": "event1"}',
            '{"time": 102, "data": "hello2", "event": "event2"}',
            '{"time": 103, "data": "hello3", "event": "event3"}',
            '{"time": 104, "data": "hello4", "event": "event4"}',
        ]

        self.write_stream(data)
        expected = ['104']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

    @yq_v1
    @pytest.mark.parametrize("kikimr", [Param(rebalancing_timeout_sec=5)], indirect=["kikimr"])
    @pytest.mark.parametrize("single_node", [False, True])
    def test_redistribute_partition_after_timeout(self, kikimr, client, single_node):
        partitions_count = 10
        self.init(client, "redistribute", partitions=partitions_count)
        wait_row_dispatcher_sensor_value(kikimr, "KnownRowDispatchers", 2 * COMPUTE_NODE_COUNT - 1)

        single_node = R'''PRAGMA dq.Scheduler=@@{{"type": "single_node"}}@@;'''

        sql = CONSUMER_PRAGMA + Rf'''
            {single_node if single_node else ""}
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT data FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        session_node_index = wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", partitions_count)
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2
        )

        message_count = 10
        expected = "hello"
        for i in range(message_count):
            self.write_stream(['{"time": 100, "data": "hello"}'], topic_path=None, partition_key=str(i))
        assert self.read_stream(message_count, topic_path=self.output_topic) == [expected] * message_count
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2
        )

        logging.debug(f"Stopping node: {session_node_index}")
        kikimr.compute_plane.kikimr_cluster.nodes[session_node_index].stop()

        expected = "Relativitätstheorie"
        for i in range(message_count):
            self.write_stream(['{"time": 101, "data": "Relativitätstheorie"}'], topic_path=None, partition_key=str(i))
        assert self.read_stream(message_count, topic_path=self.output_topic) == [expected] * message_count
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", partitions_count)

    def test_big_text_query_size(self, ydb_topics):
        io = ydb_topics('test_big_text_query_size', shared=True)
        create_input_consumer(io)

        text_size = 1000000
        element_size = 100
        filter = " OR ".join(
            [
                'data = "' + ''.join(random.choices(string.ascii_uppercase, k=element_size - 13)) + '"'
                for c in range(int(text_size / element_size))
            ]
        )

        sql = CONSUMER_PRAGMA + Rf'''INSERT INTO {io.output}
                    SELECT * FROM {io.input}
                    WITH (format=json_each_row, SCHEMA (data String NOT NULL))
                    WHERE data = "100" OR {filter};'''

        logging.debug(f"query text size: {str(len(sql))}")
        query_id = io.start(sql, timeout=300)
        data = ['{"data": "100"}', '{"data": "101"}']
        expected = ["100"]

        io.write(data)
        assert io.read(len(expected), topic_path=io.output_topic) == expected
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)
        io.stop(query_id)
