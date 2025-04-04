#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import logging
import time
import json

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig

from ydb.tests.tools.datastreams_helpers.control_plane import list_read_rules
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, create_read_rule
from ydb.tests.tools.datastreams_helpers.data_plane import read_stream, write_stream
from ydb.tests.tools.fq_runner.fq_client import StreamingDisposition

import ydb.public.api.protos.draft.fq_pb2 as fq

YDS_CONNECTION = "yds"
COMPUTE_NODE_COUNT = 3


@pytest.fixture
def kikimr(request):
    kikimr_conf = StreamingOverKikimrConfig(
        cloud_mode=True, node_count={"/cp": TenantConfig(1), "/compute": TenantConfig(COMPUTE_NODE_COUNT)}
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.compute_plane.fq_config['row_dispatcher']['enabled'] = True
    kikimr.compute_plane.fq_config['row_dispatcher']['without_consumer'] = True
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop_mvp_mock_server()
    kikimr.stop()


def start_yds_query(kikimr, client, sql) -> str:
    query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
    client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
    kikimr.compute_plane.wait_zero_checkpoint(query_id)
    return query_id


def stop_yds_query(client, query_id):
    client.abort_query(query_id)
    client.wait_query(query_id)


def wait_actor_count(kikimr, activity, expected_count):
    deadline = time.time() + 60
    while True:
        count = 0
        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            count = count + kikimr.compute_plane.get_actor_count(node_index, activity)
        if count == expected_count:
            break
        assert time.time() < deadline, f"Waiting actor {activity} count failed, current count {count}"
        time.sleep(1)
    pass


def wait_row_dispatcher_sensor_value(kikimr, sensor, expected_count, exact_match=True):
    deadline = time.time() + 60
    while True:
        count = 0
        for node_index in kikimr.compute_plane.kikimr_cluster.nodes:
            value = kikimr.compute_plane.get_sensors(node_index, "yq").find_sensor(
                {"subsystem": "row_dispatcher", "sensor": sensor})
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
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": sensor})
            count += value if value is not None else 0
        if count >= expected_value:
            break
        assert time.time() < deadline, f"Waiting sensor {sensor} value failed, current count {count}"
        time.sleep(1)
    pass


class TestPqRowDispatcher(TestYdsBase):

    def run_and_check(self, kikimr, client, sql, input, output, expected_predicate):
        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        self.write_stream(input)
        assert self.read_stream(len(output), topic_path=self.output_topic) == output

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert expected_predicate in issues, "Incorrect Issues: " + issues

    @yq_v1
    def test_read_raw_format_with_row_dispatcher(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        connections = client.list_connections(fq.Acl.Visibility.PRIVATE).result.connection
        assert len(connections) == 1
        assert connections[0].content.setting.data_streams.shared_reading

        self.init_topics("test_read_raw_format_with_row_dispatcher", create_output=False)
        output_topic = "pq_test_pq_read_write_output"
        create_stream(output_topic, partitions_count=1)
        create_read_rule(output_topic, self.consumer_name)

        sql1 = Rf'''INSERT INTO {YDS_CONNECTION}.`{output_topic}`
                    SELECT * FROM {YDS_CONNECTION}.`{self.input_topic}` WITH (format=raw, SCHEMA (data String NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql1)
        data = ['{"time" = 101;}', '{"time" = 102;}']

        self.write_stream(data)
        assert self.read_stream(len(data), topic_path=output_topic) == data
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)
        stop_yds_query(client, query_id)

        sql2 = Rf'''INSERT INTO {YDS_CONNECTION}.`{output_topic}`
                    SELECT * FROM {YDS_CONNECTION}.`{self.input_topic}` WITH (format=raw, SCHEMA (data String NOT NULL))
                    WHERE data != "romashka";'''

        query_id = start_yds_query(kikimr, client, sql2)
        data = ['{"time" = 103;}', '{"time" = 104;}']

        self.write_stream(data)
        assert self.read_stream(len(data), topic_path=output_topic) == data
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)
        stop_yds_query(client, query_id)

    @yq_v1
    def test_simple_not_null(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_simple_not_null")

        sql = Rf'''
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

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        stop_yds_query(client, query_id)
        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_metadatafields(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_metadatafields")

        # Its not completely clear why metadatafields appear in this request(
        sql = Rf'''
            PRAGMA FeatureR010="prototype";
            PRAGMA config.flags("TimeOrderRecoverDelay", "-10");
            PRAGMA config.flags("TimeOrderRecoverAhead", "10");
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL))
                MATCH_RECOGNIZE(
                    ORDER BY CAST(time as Timestamp)
                    MEASURES LAST(A.time) as b_key
                    PATTERN (A )
                    DEFINE A as A.time > 4
                );'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 100}',
            '{"time": 109}',
            '{"time": 118}',
        ]
        self.write_stream(data)
        assert len(self.read_stream(1, topic_path=self.output_topic)) == 1
        stop_yds_query(client, query_id)

    @yq_v1
    def test_simple_optional(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_simple_optional")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101, "data": "hello1", "event": "event1"}', '{"time": 102, "event": "event2"}']

        self.write_stream(data)
        expected = ['101', '102']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)

        stop_yds_query(client, query_id)
        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_scheme_error(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_scheme_error")

        sql = Rf'''
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
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_nested_types")

        sql = Rf'''
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
            '"' + large_string + '"'
        ]
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        stop_yds_query(client, query_id)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert "Row dispatcher will use the predicate:" in issues, "Incorrect Issues: " + issues

    @yq_v1
    def test_nested_types_without_predicate(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_nested_types_without_predicate")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT data FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data Json NOT NULL, event String NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": {"key": "value"}, "event": "event1"}',
            '{"time": 102, "data": ["key1", "key2"], "event": "event2"}'
        ]

        self.write_stream(data)
        expected = [
            '{"key": "value"}',
            '["key1", "key2"]'
        ]
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        stop_yds_query(client, query_id)

    @yq_v1
    def test_filters_non_optional_field(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_filters_non_optional_field")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String NOT NULL, event String NOT NULL, nested Json NOT NULL)) WHERE '''
        data = [
            '{"time": 101, "data": "hello1", "event": "event1", "nested": {"xyz": "key"}}',
            '{"time": 102, "data": "hello2", "event": "event2", "nested": ["abc", "key"]}']
        filter = "time > 101;"
        expected = ['102']
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`time` > 101)')
        filter = 'data = "hello2"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`data` = \\"hello2\\")')
        filter = ' event IS NOT DISTINCT FROM "event2"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`event` IS NOT DISTINCT FROM \\"event2\\")')
        filter = ' event IS DISTINCT FROM "event1"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`event` IS DISTINCT FROM \\"event1\\")')
        filter = 'event IN ("event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`event` IN (\\"event2\\"))')
        filter = 'event NOT IN ("event1", "event3")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (NOT (`event` IN (\\"event1\\", \\"event3\\")))')
        filter = 'event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`event` IN (\\"1\\"')
        filter = ' event IS DISTINCT FROM data AND event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE ((`event` IS DISTINCT FROM `data`) AND (`event` IN (\\"1\\"')
        filter = ' IF(event = "event2", event IS DISTINCT FROM data, FALSE)'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE IF((`event` = \\"event2\\"), (`event` IS DISTINCT FROM `data`), FALSE)')
        filter = ' nested REGEXP ".*abc.*"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (CAST(`nested` AS String) REGEXP \\".*abc.*\\")')
        filter = ' CAST(nested AS String) REGEXP ".*abc.*"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (CAST(`nested` AS String) REGEXP \\".*abc.*\\")')

    @yq_v1
    def test_filters_optional_field(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_filters_optional_field")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String, event String, flag Bool, field1 UInt8, field2 Int64, nested Json)) WHERE '''
        data = [
            '{"time": 101, "data": "hello1", "event": "event1", "flag": false, "field1": 5, "field2": 5, "nested": {"xyz": "key"}}',
            '{"time": 102, "data": "hello2", "event": "event2", "flag": true, "field1": 5, "field2": 1005, "nested": ["abc", "key"]}']
        expected = ['102']
        filter = 'data = "hello2"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`data` = \\"hello2\\")')
        filter = 'flag'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE `flag`')
        filter = 'time * (field2 - field1) != 0'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE ((`time` * (`field2` - `field1`)) <> 0)')
        filter = '(field1 % field2) / 5 = 1'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (((`field1` % `field2`) / 5) = 1)')
        filter = ' event IS NOT DISTINCT FROM "event2"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`event` IS NOT DISTINCT FROM \\"event2\\")')
        filter = ' event IS DISTINCT FROM "event1"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`event` IS DISTINCT FROM \\"event1\\")')
        filter = ' field1 IS DISTINCT FROM field2'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`field1` IS DISTINCT FROM `field2`)')
        filter = 'time == 102 OR (field2 IS NOT DISTINCT FROM 1005 AND Random(field1) < 10.0)'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE ((`time` = 102) OR (`field2` IS NOT DISTINCT FROM 1005))')
        filter = 'event IN ("event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`event` IN (\\"event2\\"))')
        filter = 'event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (`event` IN (\\"1\\"')
        filter = ' event IS DISTINCT FROM data AND event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE ((`event` IS DISTINCT FROM `data`) AND COALESCE((`event` IN (\\"1\\"')
        filter = ' IF(event == "event2", event IS DISTINCT FROM data, FALSE)'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE IF(COALESCE((`event` = \\"event2\\"), FALSE), (`event` IS DISTINCT FROM `data`), FALSE)')
        filter = ' COALESCE(event = "event2", TRUE)'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE COALESCE((`event` = \\"event2\\"), TRUE)')
        filter = ' COALESCE(event = "event2", data = "hello2", TRUE)'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE COALESCE((`event` = \\"event2\\"), (`data` = \\"hello2\\"), TRUE)')
        filter = " event ?? '' REGEXP @@e.*e.*t2@@"
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (COALESCE(`event`, \\"\\") REGEXP \\"e.*e.*t2\\")')
        filter = " event ?? '' NOT REGEXP @@e.*e.*t1@@"
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (NOT (COALESCE(`event`, \\"\\") REGEXP \\"e.*e.*t1\\"))')
        filter = " event ?? '' REGEXP data ?? '' OR time = 102"
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE ((COALESCE(`event`, \\"\\") REGEXP COALESCE(`data`, \\"\\")) OR (`time` = 102))')
        filter = ' nested REGEXP ".*abc.*"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (IF((`nested` IS NOT NULL), CAST(`nested` AS String), NULL) REGEXP \\".*abc.*\\")')
        filter = ' CAST(nested AS String) REGEXP ".*abc.*"'
        self.run_and_check(kikimr, client, sql + filter, data, expected, 'predicate: WHERE (CAST(`nested` AS String?) REGEXP \\".*abc.*\\")')

    @yq_v1
    def test_filter_missing_fields(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_filter_missing_fields")

        sql = Rf'''
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

    @yq_v1
    def test_filter_use_unsupported_predicate(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_filter_use_unsupported_predicate")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String NOT NULL, event String NOT NULL))
                WHERE  event LIKE 'event2%';'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 102, "data": "hello2", "event": "event2"}',
        ]

        self.write_stream(data)
        assert self.read_stream(1, topic_path=self.output_topic) == ['102']
        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        stop_yds_query(client, query_id)

    @yq_v1
    def test_filter_with_mr(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_filter_with_mr")

        sql = Rf'''
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
            '{"time": 116, "event_class": "event_class2", "event_type": 4}'
        ]

        self.write_stream(data)
        expected = ['{"event_type":2}']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        stop_yds_query(client, query_id)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert "Row dispatcher will use the predicate: WHERE (`event_class` =" in issues, "Incorrect Issues: " + issues

    @yq_v1
    def test_start_new_query(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_start_new_query", create_output=False)

        output_topic1 = "pq_test_pq_read_write_output1"
        output_topic2 = "pq_test_pq_read_write_output2"
        output_topic3 = "pq_test_pq_read_write_output3"
        create_stream(output_topic1, partitions_count=1)
        create_read_rule(output_topic1, self.consumer_name)

        create_stream(output_topic2, partitions_count=1)
        create_read_rule(output_topic2, self.consumer_name)

        create_stream(output_topic3, partitions_count=1)
        create_read_rule(output_topic3, self.consumer_name)

        sql1 = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic1}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''
        sql2 = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic2}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''
        query_id1 = start_yds_query(kikimr, client, sql1)
        query_id2 = start_yds_query(kikimr, client, sql2)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": "hello1", "event": "event1"}',
            '{"time": 102, "data": "hello2", "event": "event2"}',
        ]

        self.write_stream(data)
        expected = ['101', '102']
        assert self.read_stream(len(expected), topic_path=output_topic1) == expected
        assert self.read_stream(len(expected), topic_path=output_topic2) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 2)

        # nothing unnecessary...
        assert not read_stream(output_topic1, 1, True, self.consumer_name, timeout=1)
        assert not read_stream(output_topic2, 1, True, self.consumer_name, timeout=1)

        sql3 = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic3}`
            SELECT event FROM {YDS_CONNECTION}.`{self.input_topic}`
            WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, event String NOT NULL));'''
        query_id3 = start_yds_query(kikimr, client, sql3)

        data = [
            '{"time": 103, "data": "hello3", "event": "event3"}',
            '{"time": 104, "data": "hello4", "event": "event4"}',
        ]

        self.write_stream(data)
        expected12 = ['103', '104']
        expected3 = ['event3', 'event4']
        assert self.read_stream(len(expected), topic_path=output_topic1) == expected12
        assert self.read_stream(len(expected), topic_path=output_topic2) == expected12
        assert self.read_stream(len(expected), topic_path=output_topic3) == expected3

        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        assert not read_stream(output_topic1, 1, True, self.consumer_name, timeout=1)
        assert not read_stream(output_topic2, 1, True, self.consumer_name, timeout=1)
        assert not read_stream(output_topic3, 1, True, self.consumer_name, timeout=1)

        stop_yds_query(client, query_id1)
        stop_yds_query(client, query_id2)
        stop_yds_query(client, query_id3)

        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_stop_start(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_stop_start", create_output=False)

        output_topic = "test_stop_start"
        create_stream(output_topic, partitions_count=1)
        create_read_rule(output_topic, self.consumer_name)

        sql1 = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql1)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101}', '{"time": 102}']
        self.write_stream(data)
        expected = ['101', '102']
        assert self.read_stream(len(expected), topic_path=output_topic) == expected

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2
        )
        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

        client.modify_query(
            query_id,
            "continue",
            sql1,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        data = ['{"time": 103}', '{"time": 104}']

        self.write_stream(data)
        expected = ['103', '104']
        assert self.read_stream(len(expected), topic_path=output_topic) == expected

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_stop_start_with_filter(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_stop_start_with_filter", create_output=False)

        output_topic = "test_stop_start_with_filter"
        create_stream(output_topic, partitions_count=1)
        create_read_rule(output_topic, self.consumer_name)

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL))
                WHERE time > 200;'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101}', '{"time": 102}']
        self.write_stream(data)

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 10     # long sleep to send status from topic_session to read_actor
        )
        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL));'''

        client.modify_query(
            query_id,
            "continue",
            sql,
            type=fq.QueryContent.QueryType.STREAMING,
            state_load_mode=fq.StateLoadMode.EMPTY,
            streaming_disposition=StreamingDisposition.from_last_checkpoint(),
        )
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        data = ['{"time": 203}', '{"time": 204}']
        self.write_stream(data)
        expected = ['203', '204']
        assert self.read_stream(len(expected), topic_path=output_topic) == expected

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_restart_compute_node(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_restart_compute_node", partitions_count=4)

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 4)

        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(100, 102)], "partition_key1")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(102, 104)], "partition_key2")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(104, 106)], "partition_key3")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(106, 108)], "partition_key4")

        expected = [Rf'''{c}''' for c in range(100, 108)]
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == expected

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2)

        node_index = 2
        logging.debug("Restart compute node {}".format(node_index))
        kikimr.compute_plane.kikimr_cluster.nodes[node_index].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[node_index].start()
        kikimr.compute_plane.wait_bootstrap(node_index)

        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(108, 110)], "partition_key1")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(110, 112)], "partition_key2")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(112, 114)], "partition_key3")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(114, 116)], "partition_key4")

        expected = [Rf'''{c}''' for c in range(108, 116)]
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == expected

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2
        )

        node_index = 1
        logging.debug("Restart compute node {}".format(node_index))
        kikimr.compute_plane.kikimr_cluster.nodes[node_index].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[node_index].start()
        kikimr.compute_plane.wait_bootstrap(node_index)

        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(116, 118)], "partition_key1")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(118, 120)], "partition_key2")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(120, 122)], "partition_key3")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(122, 124)], "partition_key4")

        expected = [Rf'''{c}''' for c in range(116, 124)]
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == expected

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 2
        )

        node_index = 3
        logging.debug("Restart compute node {}".format(node_index))
        kikimr.compute_plane.kikimr_cluster.nodes[node_index].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[node_index].start()
        kikimr.compute_plane.wait_bootstrap(node_index)

        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(124, 126)], "partition_key1")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(126, 128)], "partition_key2")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(128, 130)], "partition_key3")
        write_stream(self.input_topic, [Rf'''{{"time": {c}}}''' for c in range(130, 132)], "partition_key4")

        expected = [Rf'''{c}''' for c in range(124, 132)]
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == expected

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_3_sessions(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_3_session", create_output=False)

        output_topic1 = "test_3_session1"
        output_topic2 = "test_3_session2"
        output_topic3 = "test_3_session3"
        create_stream(output_topic1, partitions_count=1)
        create_read_rule(output_topic1, self.consumer_name)

        create_stream(output_topic2, partitions_count=1)
        create_read_rule(output_topic2, self.consumer_name)

        create_stream(output_topic3, partitions_count=1)
        create_read_rule(output_topic3, self.consumer_name)

        sql1 = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic1}`
            SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
        sql2 = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic2}`
            SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        sql3 = Rf'''
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
            "continue",
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

    @yq_v1
    def test_many_partitions(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_many_partitions", partitions_count=4)

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 4)

        input_messages1 = [Rf'''{{"time": {c}}}''' for c in range(100, 110)]
        write_stream(self.input_topic, input_messages1, "partition_key1")

        input_messages2 = [Rf'''{{"time": {c}}}''' for c in range(110, 120)]
        write_stream(self.input_topic, input_messages2, "partition_key2")

        input_messages3 = [Rf'''{{"time": {c}}}''' for c in range(120, 130)]
        write_stream(self.input_topic, input_messages3, "partition_key3")

        input_messages4 = [Rf'''{{"time": {c}}}''' for c in range(130, 140)]
        write_stream(self.input_topic, input_messages4, "partition_key4")

        expected = [Rf'''{c}''' for c in range(100, 140)]
        assert sorted(self.read_stream(len(expected), topic_path=self.output_topic)) == expected

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "FQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_2_connection(self, kikimr, client):
        client.create_yds_connection("name1", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True)
        client.create_yds_connection("name2", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True)
        self.init_topics("test_2_connection", partitions_count=2)
        create_read_rule(self.input_topic, "best")

        sql1 = Rf'''
            INSERT INTO `name1`.`{self.output_topic}`
            SELECT Cast(time as String) FROM `name1`.`{self.input_topic}` WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
        sql2 = Rf'''
            INSERT INTO `name2`.`{self.output_topic}`
            SELECT Cast(time as String) FROM `name2`.`{self.input_topic}` WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
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
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), shared_reading=True
        )
        self.init_topics("test_sensors")

        sql = Rf'''
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
