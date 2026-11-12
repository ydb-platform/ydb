#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import logging
import json
import random
import string
import re
from html import unescape
from urllib.parse import parse_qs, urlsplit

import requests

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.fq.streaming_common.common import counter_nodes, get_sensors, monitoring_endpoint
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase


from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, delete_stream

COMPUTE_NODE_COUNT = 3
ROW_DISPATCHER_CONSUMER = 'row_dispatcher_consumer'
CONSUMER_PRAGMA = f'PRAGMA pq.Consumer="{ROW_DISPATCHER_CONSUMER}";\n'


def create_input_consumer(io):
    io.kikimr.external_ydb_client.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{ROW_DISPATCHER_CONSUMER}`;')


def query_state(io, query):
    rows = io.kikimr.ydb_client.query(f'''
        SELECT Status, Run, RetryCount, Issues
        FROM `.sys/streaming_queries` WHERE Path = '{query}';
    ''')[0].rows
    assert len(rows) == 1, rows
    return rows[0]


def query_issues(io, query):
    def messages(value):
        if isinstance(value, dict):
            if 'message' in value:
                yield value['message']
            for nested in value.values():
                yield from messages(nested)
        elif isinstance(value, list):
            for nested in value:
                yield from messages(nested)

    return '\n'.join(messages(json.loads(query_state(io, query)['Issues'])))


def assert_row_dispatcher_predicate(io, query, expected=''):
    rows = io.kikimr.ydb_client.query(f"""
        SELECT Plan FROM `.sys/streaming_queries` WHERE Path = '{query}';
    """)[0].rows
    assert len(rows) == 1, rows
    plan = json.loads(rows[0]['Plan'])

    def predicates(node):
        if isinstance(node, dict):
            if node.get('SourceType') == 'pq' and 'Filter (shared reading)' in node:
                yield node['Filter (shared reading)']
            for value in node.values():
                yield from predicates(value)
        elif isinstance(node, list):
            for value in node:
                yield from predicates(value)

    actual = list(predicates(plan))
    assert any(predicate and expected in predicate for predicate in actual), (expected, actual, plan)


def wait_ydb_row_dispatcher_sensor(io, labels, expected, exact_match=True):
    counts = {}

    def matches():
        counts.clear()
        for node_id in counter_nodes(io.kikimr.cluster):
            value = get_sensors(io.kikimr.cluster, node_id, 'kqp').find_sensor(
                {
                    'subsystem': 'row_dispatcher',
                    **labels,
                }
            )
            counts[node_id] = value or 0
        total = sum(counts.values())
        return total == expected if exact_match else total >= expected

    assert wait_for(matches, timeout_seconds=60, step_seconds=0.5), (labels, counts, expected)


def read_compute_ingress_stats(io):
    # These four values exist in DQ but are not exported by KQP's six streaming
    # query counters or its JSON plan serializer. The compute actor's diagnostic
    # page exposes the actual source statistics, without parsing server logs.
    result = {}
    for node_id in counter_nodes(io.kikimr.cluster):
        url = monitoring_endpoint(io.kikimr.cluster, node_id) + '/actors/kqp_node'
        response = requests.get(url, headers={'Authorization': 'root@builtin'}, timeout=5)
        response.raise_for_status()
        actors = set()
        for link in re.findall(r'href=["\']?([^"\'\s>]+)', response.text, re.IGNORECASE):
            actors.update(parse_qs(urlsplit(unescape(link)).query).get('ca', []))
        for actor in actors:
            response = requests.get(
                url, params={'ca': actor, 'view': 'dump'}, headers={'Authorization': 'root@builtin'}, timeout=5
            )
            response.raise_for_status()
            for name, value in re.findall(
                r'AsyncInput\.IngressStats\.(FilteredBytes|FilteredRows|QueuedBytes|QueuedRows):\s*(\d+)',
                response.text,
            ):
                result[name] = result.get(name, 0) + int(value)
    return result


class TestPqRowDispatcher(TestYdsBase):

    def run_and_check(self, io, sql, input, output, expected_predicate):
        query_id = io.start(sql)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 1)
        io.write(input)
        assert io.read(len(output)) == output
        assert_row_dispatcher_predicate(io, query_id, expected_predicate)
        io.stop(query_id)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)

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
        io.wait_read_session_closed(io.input_topic, ROW_DISPATCHER_CONSUMER)

    @pytest.mark.skip(reason="YDB bug: MATCH_RECOGNIZE metadata crashes compute with Value is not a dict")
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
        io.wait_read_session_closed(io.input_topic, ROW_DISPATCHER_CONSUMER)

    def test_scheme_error(self, ydb_topics):
        io = ydb_topics('test_scheme_error', shared=True)
        create_input_consumer(io)
        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));
        '''
        query_id = io.start(sql)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 1)
        initial_retry_count = query_state(io, query_id)['RetryCount']
        io.write(['{"this": "is", not json}', '{"noch einmal / nicht json"}'])

        def retries_bad_json():
            state = query_state(io, query_id)
            return (
                state['RetryCount'] > initial_retry_count
                and state['Run']
                and 'Failed to parse json message for offset' in state['Issues']
            )

        assert wait_for(retries_bad_json, timeout_seconds=120, step_seconds=0.5), query_state(io, query_id)
        io.stop(query_id)
        io.wait_actor_count('DQ_PQ_READ_ACTOR', 0)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)

        query_id = io.start(sql)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 1)
        io.write(['{"time": 101, "data": "hello1", "event": "event1"}'])
        assert io.read(1) == ['101']
        io.stop(query_id)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)

    def test_nested_types(self, ydb_topics):
        io = ydb_topics("test_nested_types", shared=True)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT data FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data Json NOT NULL, event String NOT NULL))
                WHERE event = "event1" or event = "event2" or event = "event4";'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        large_string = "abcdefghjkl1234567890+abcdefghjkl1234567890"
        data = [
            '{"time": 101, "data": {"key": "value", "second_key":"' + large_string + '"}, "event": "event1"}',
            '{"time": 102, "data": ["key1", "key2", "' + large_string + '"], "event": "event2"}',
            '{"time": 103, "data": ["' + large_string + '"], "event": "event3"}',
            '{"time": 104, "data": "' + large_string + '", "event": "event4"}',
        ]

        io.write(data)
        expected = [
            '{"key": "value", "second_key":"' + large_string + '"}',
            '["key1", "key2", "' + large_string + '"]',
            '"' + large_string + '"',
        ]
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.wait_actor_count("DQ_PQ_READ_ACTOR", 1)
        assert_row_dispatcher_predicate(io, query_id, '')
        io.stop(query_id)

    def test_nested_structured_types(self, ydb_topics):
        io = ydb_topics("test_nested_structured_types", shared=True)
        create_input_consumer(io)

        large_string = "abcdefghjkl1234567890+abcdefghjkl1234567890"
        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(data))))
             FROM {io.input}
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

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": {"key": "value", "second_key":[1, false]}, "event": "event1"}',
            '{"time": 102, "data": {"second_key":[2], "key":"' + large_string + '"}, "event": "event2"}',
            '{"time": 103, "data": {"key":"ev2", "second_key":[3]}, "event": "event3"}',
            '{"time": 104, "data": {"key":"ev2", "second_key":[42, true]}, "event": "event4"}',
            '{"time": 105, "data": {"key":"ev2", "second_key":[42, false]}, "event": "event5"}',
        ]

        io.write(data)
        expected = [
            '{"key":"value","second_key":[1,false]}',
            '{"key":"' + large_string + '","second_key":[2,null]}',
            '{"key":"ev2","second_key":[42,false]}',
        ]
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.wait_actor_count("DQ_PQ_READ_ACTOR", 1)
        assert_row_dispatcher_predicate(io, query_id, 'second_key')
        io.stop(query_id)

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

    def test_filters_non_optional_field(self, ydb_topics):
        io = ydb_topics("test_filters_non_optional_field", shared=True)
        create_input_consumer(io)

        sql = (
            CONSUMER_PRAGMA
            + Rf'''
            PRAGMA AnsiLike;
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String NOT NULL, event String NOT NULL, nested Json NOT NULL)) WHERE '''
        )
        data = [
            '{"time": 101, "data": "hello1", "event": "event1", "nested": {"xyz": "key"}}',
            '{"time": 102, "data": "hello2", "event": "event2", "nested": ["abc", "key"]}',
        ]
        expected = ['102']
        filter = "time > 101;"
        self.run_and_check(io, sql + filter, data, expected, '(`time` > 101)')
        filter = 'data = "hello2"'
        self.run_and_check(io, sql + filter, data, expected, '(`data` = "hello2")')
        filter = ' event IS NOT DISTINCT FROM "event2"'
        self.run_and_check(io, sql + filter, data, expected, '(`event` IS NOT DISTINCT FROM "event2")')
        filter = ' event IS DISTINCT FROM "event1"'
        self.run_and_check(io, sql + filter, data, expected, '(`event` IS DISTINCT FROM "event1")')
        filter = 'event IN ("event2")'
        self.run_and_check(io, sql + filter, data, expected, '(`event` IN ("event2"))')
        filter = 'event NOT IN ("event1", "event3")'
        self.run_and_check(io, sql + filter, data, expected, '(NOT (`event` IN ("event1", "event3")))')
        filter = 'event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(io, sql + filter, data, expected, '(`event` IN ("1"')
        filter = ' event IS DISTINCT FROM data AND event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(io, sql + filter, data, expected, '((`event` IS DISTINCT FROM `data`) AND (`event` IN ("1"')
        filter = ' IF(event = "event2", event IS DISTINCT FROM data, FALSE)'
        self.run_and_check(
            io, sql + filter, data, expected, 'IF((`event` = "event2"), (`event` IS DISTINCT FROM `data`), FALSE)'
        )
        filter = ' nested REGEXP ".*abc.*"'
        self.run_and_check(io, sql + filter, data, expected, '(CAST(`nested` AS String) REGEXP ".*abc.*")')
        filter = ' CAST(nested AS String) REGEXP ".*abc.*"'
        self.run_and_check(io, sql + filter, data, expected, '(CAST(`nested` AS String) REGEXP ".*abc.*")')
        filter = 'event LIKE "event2%"'
        self.run_and_check(io, sql + filter, data, expected, 'StartsWith(`event`, "event2")')
        filter = 'event LIKE "%event2"'
        self.run_and_check(io, sql + filter, data, expected, 'EndsWith(`event`, "event2")')
        filter = 'event LIKE "%event2%"'
        self.run_and_check(io, sql + filter, data, expected, 'String::Contains(`event`, "event2")')
        filter = ' (data = "hello2") IS NOT DISTINCT FROM true'
        self.run_and_check(io, sql + filter, data, expected, '((`data` = "hello2") IS NOT DISTINCT FROM TRUE)')
        filter = ' (data REGEXP ".*hello2.*") IS NOT DISTINCT FROM true'
        self.run_and_check(io, sql + filter, data, expected, '((`data` REGEXP ".*hello2.*") IS NOT DISTINCT FROM TRUE)')
        filter = ' (CAST(data AS Utf8) REGEXP ".*hello2.*") IS NOT DISTINCT FROM true'
        self.run_and_check(
            io,
            sql + filter,
            data,
            expected,
            '((IF((CAST(`data` AS Utf8?) IS NOT NULL), CAST(CAST(`data` AS Utf8?) AS String), NULL) REGEXP ".*hello2.*") IS NOT DISTINCT FROM TRUE)',
        )  # YQ-5727
        filter = ' CAST(`time` AS Date) > Date("1970-04-12")'
        self.run_and_check(io, sql + filter, data, expected, '(CAST(`time` AS Date?) > Date("1970-04-12"))')
        filter = ' CAST(`time` AS Timestamp) > Timestamp("1970-01-01T00:00:00.000101Z")'
        self.run_and_check(
            io, sql + filter, data, expected, '(CAST(`time` AS Timestamp?) > Timestamp("1970-01-01T00:00:00.000101Z"))'
        )  # YQ-5738
        filter = ' MIN_OF(`time`, `time` + 5) >= 102 AND MAX_OF(`time`, `time` - 5) < 103'
        self.run_and_check(
            io,
            sql + filter,
            data,
            expected,
            '((MIN_OF(`time`, (`time` + 5)) >= 102) AND (MAX_OF(`time`, (`time` - 5)) < 103))',
        )
        filter = ' `time` IN (102, 103, 104)'
        self.run_and_check(io, sql + filter, data, expected, '(`time` IN (102, 103, 104))')
        filter = ' CAST(`time` AS Interval) > Interval("PT0.000101S")'
        self.run_and_check(io, sql + filter, data, expected, '(CAST(`time` AS Interval?) > Interval("PT0.000101S"))')
        filter = ' CAST(`time` AS Timestamp) - Interval("-PT0.000001S") > Timestamp("1970-01-01T00:00:00.000102Z")'
        self.run_and_check(
            io,
            sql + filter,
            data,
            expected,
            '((CAST(`time` AS Timestamp?) - Interval("-PT0.000001S")) > Timestamp("1970-01-01T00:00:00.000102Z"))',
        )

    def test_filters_optional_field(self, ydb_topics):
        io = ydb_topics("test_filters_optional_field", shared=True)
        create_input_consumer(io)

        sql = (
            CONSUMER_PRAGMA
            + Rf'''
            PRAGMA AnsiLike;
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String, event String, flag Bool, field1 UInt8, field2 Int64, nested Json)) WHERE '''
        )
        data = [
            '{"time": 101, "data": "hello1", "event": "event1", "flag": false, "field1": 5, "field2": 5, "nested": {"xyz": "key"}}',
            '{"time": 102, "data": "hello2", "event": "event2", "flag": true, "field1": 5, "field2": 1005, "nested": ["abc", "key"]}',
        ]
        expected = ['102']
        filter = 'data = "hello2"'
        self.run_and_check(io, sql + filter, data, expected, '(`data` = "hello2")')
        filter = 'flag'
        self.run_and_check(io, sql + filter, data, expected, '`flag`')
        filter = 'time * (field2 - field1) != 0'
        self.run_and_check(io, sql + filter, data, expected, '((`time` * (`field2` - `field1`)) <> 0)')
        filter = '(field1 % field2) / 5 = 1'
        self.run_and_check(io, sql + filter, data, expected, '(((`field1` % `field2`) / 5) = 1)')
        filter = ' event IS NOT DISTINCT FROM "event2"'
        self.run_and_check(io, sql + filter, data, expected, '(`event` IS NOT DISTINCT FROM "event2")')
        filter = ' event IS DISTINCT FROM "event1"'
        self.run_and_check(io, sql + filter, data, expected, '(`event` IS DISTINCT FROM "event1")')
        filter = ' field1 IS DISTINCT FROM field2'
        self.run_and_check(io, sql + filter, data, expected, '(`field1` IS DISTINCT FROM `field2`)')
        filter = 'time == 102 OR (field2 IS NOT DISTINCT FROM 1005 AND Random(field1) < 10.0)'
        self.run_and_check(io, sql + filter, data, expected, '((`time` = 102) OR (`field2` IS NOT DISTINCT FROM 1005))')
        filter = 'event IN ("event2")'
        self.run_and_check(io, sql + filter, data, expected, '(`event` IN ("event2"))')
        filter = 'event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(io, sql + filter, data, expected, '(`event` IN ("1"')
        filter = ' event IS DISTINCT FROM data AND event IN ("1", "2", "3", "4", "5", "6", "7", "event2")'
        self.run_and_check(
            io, sql + filter, data, expected, '((`event` IS DISTINCT FROM `data`) AND COALESCE((`event` IN ("1"'
        )
        filter = ' IF(event == "event2", event IS DISTINCT FROM data, FALSE)'
        self.run_and_check(
            io,
            sql + filter,
            data,
            expected,
            'IF(COALESCE((`event` = "event2"), FALSE), (`event` IS DISTINCT FROM `data`), FALSE)',
        )
        filter = ' COALESCE(event = "event2", TRUE)'
        self.run_and_check(io, sql + filter, data, expected, 'COALESCE((`event` = "event2"), TRUE)')
        filter = ' COALESCE(event = "event2", data = "hello2", TRUE)'
        self.run_and_check(
            io, sql + filter, data, expected, 'COALESCE((`event` = "event2"), (`data` = "hello2"), TRUE)'
        )
        filter = " event ?? '' REGEXP @@e.*e.*t2@@"
        self.run_and_check(io, sql + filter, data, expected, '(COALESCE(`event`, "") REGEXP "e.*e.*t2")')
        filter = " event ?? '' NOT REGEXP @@e.*e.*t1@@"
        self.run_and_check(io, sql + filter, data, expected, '(NOT (COALESCE(`event`, "") REGEXP "e.*e.*t1"))')
        filter = " event ?? '' REGEXP data ?? '' OR time = 102"
        self.run_and_check(
            io, sql + filter, data, expected, '((COALESCE(`event`, "") REGEXP COALESCE(`data`, "")) OR (`time` = 102))'
        )
        filter = ' nested REGEXP ".*abc.*"'
        self.run_and_check(
            io,
            sql + filter,
            data,
            expected,
            '(IF((`nested` IS NOT NULL), CAST(`nested` AS String), NULL) REGEXP ".*abc.*")',
        )
        filter = ' CAST(nested AS String) REGEXP ".*abc.*"'
        self.run_and_check(io, sql + filter, data, expected, '(CAST(`nested` AS String?) REGEXP ".*abc.*")')
        filter = 'event LIKE "event2%"'
        self.run_and_check(io, sql + filter, data, expected, 'StartsWith(`event`, "event2")')
        filter = 'event LIKE "%event2"'
        self.run_and_check(io, sql + filter, data, expected, 'EndsWith(`event`, "event2")')
        filter = 'event LIKE "%event2%"'
        self.run_and_check(io, sql + filter, data, expected, 'String::Contains(`event`, "event2")')
        filter = ' (data = "hello2") IS NOT DISTINCT FROM true'
        self.run_and_check(io, sql + filter, data, expected, '((`data` = "hello2") IS NOT DISTINCT FROM TRUE)')
        filter = ' (data REGEXP ".*hello2.*") IS NOT DISTINCT FROM true'
        self.run_and_check(io, sql + filter, data, expected, '((`data` REGEXP ".*hello2.*") IS NOT DISTINCT FROM TRUE)')
        filter = ' (data in ("hello2", "hello3")) IS NOT DISTINCT FROM true'
        self.run_and_check(
            io, sql + filter, data, expected, '((`data` IN ("hello2", "hello3")) IS NOT DISTINCT FROM TRUE)'
        )
        # YQ-5708
        filter = ' COALESCE(event, data) IS DISTINCT FROM "event1"'
        self.run_and_check(io, sql + filter, data, expected, '(COALESCE(`event`, `data`) IS DISTINCT FROM "event1")')
        filter = ' UNWRAP(coalesce(event, data)) IS distinct FROM "event1"'
        self.run_and_check(
            io, sql + filter, data, expected, '(Unwrap(COALESCE(`event`, `data`)) IS DISTINCT FROM "event1")'
        )
        filter = ' tobytes(coalesce(cast(event as Utf8), cast(data as utf8))) IS distinct FROM "event1"'
        self.run_and_check(
            io,
            sql + filter,
            data,
            expected,
            '(CAST(COALESCE(CAST(`event` AS Utf8?), CAST(`data` AS Utf8?)) AS String) IS DISTINCT FROM "event1")',
        )
        filter = ' (CAST(data AS Utf8) REGEXP ".*hello2.*") IS NOT DISTINCT FROM true'
        self.run_and_check(
            io,
            sql + filter,
            data,
            expected,
            '((IF((CAST(`data` AS Utf8?) IS NOT NULL), CAST(CAST(`data` AS Utf8?) AS String), NULL) REGEXP ".*hello2.*") IS NOT DISTINCT FROM TRUE)',
        )  # YQ-5727

    def test_filter_missing_fields(self, ydb_topics):
        io = ydb_topics("test_filter_missing_fields", shared=True)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, `data@data` String, event String NOT NULL))
                WHERE `data@data` IS NULL;'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "event": "event1"}',
            '{"time": 102, "data@data": null, "event": "event2"}',
            '{"time": 103, "data@data": "", "event": "event2"}',
            '{"time": 104, "data@data": "null", "event": "event2"}',
        ]

        io.write(data)
        expected = ['101', '102']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.wait_actor_count("DQ_PQ_READ_ACTOR", 1)
        assert_row_dispatcher_predicate(io, query_id, '')
        io.stop(query_id)

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

    def test_filter_with_mr(self, ydb_topics):
        io = ydb_topics("test_filter_with_mr", shared=True)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            pragma FeatureR010="prototype";
            pragma config.flags("TimeOrderRecoverDelay", "-10");
            pragma config.flags("TimeOrderRecoverAhead", "10");

            $data =
                SELECT * FROM {io.input}
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

            INSERT INTO {io.output}
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $match;
            '''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 100, "event_class": "event_class1", "event_type": 1}',
            '{"time": 105, "event_class": "event_class2", "event_type": 2}',
            '{"time": 110, "event_class": "event_class2", "event_type": 3}',
            '{"time": 116, "event_class": "event_class2", "event_type": 4}',
        ]

        io.write(data)
        expected = ['{"event_type":2}']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        assert_row_dispatcher_predicate(io, query_id, '(`event_class` =')
        io.stop(query_id)

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
                create_stream(topic, default_endpoint=io.endpoint)
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
            io.wait_read_session_closed(io.input_topic, ROW_DISPATCHER_CONSUMER)
        finally:
            for query in reversed(io.queries):
                io.kikimr.ydb_client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;')
            for topic in extra_topics:
                external.query(f'DROP TOPIC IF EXISTS `{topic}`;')

    def test_stop_start(self, ydb_topics):
        io = ydb_topics("test_stop_start", shared=True, partitions=10)
        create_input_consumer(io)

        sql1 = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = io.start(sql1)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 10)

        data = ['{"time": 101}', '{"time": 102}']
        io.write(data)
        expected = ['101', '102']
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == sorted(expected)

        io.checkpoint(query_id, count=2)
        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

        io.resume(query_id, disposition='FROM_CHECKPOINT')

        data = ['{"time": 103}', '{"time": 104}']

        io.write(data)
        expected = ['103', '104']
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == sorted(expected)

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

    def test_stop_start2(self, ydb_topics):
        io = ydb_topics("test_stop_start", shared=True)
        create_input_consumer(io)
        sql1 = CONSUMER_PRAGMA + Rf'''INSERT INTO {io.output}
                    SELECT Cast(time as String) FROM {io.input}
                        WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
        query_id1 = io.start(sql1)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)
        io.write(['{"time": 101}', '{"time": 102}'])
        expected = ['101', '102']
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == sorted(expected)

        io.checkpoint(query_id1, count=2)
        io.stop(query_id1)

        io.resume(query_id1, disposition='FROM_CHECKPOINT')
        query_id2 = io.start(sql1)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        io.checkpoint(query_id1, count=2)
        io.checkpoint(query_id2)
        io.stop(query_id1)
        io.stop(query_id2)

        io.resume(query_id1, disposition='FROM_CHECKPOINT')
        io.resume(query_id2, disposition='FROM_CHECKPOINT')
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        io.write(['{"time": 103}', '{"time": 104}'])
        expected = ['103', '104', '103', '104']
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == sorted(expected)

        io.stop(query_id1)
        io.stop(query_id2)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

    def test_stop_start_with_filter(self, ydb_topics):
        io = ydb_topics("test_stop_start_with_filter", shared=True)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL))
                WHERE time > 200;'''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101}', '{"time": 102}']
        io.write(data)

        # YDB RD reports offsets every 10 s (YQ used 2 s), in addition to
        # the topic-session statistics timer. Cross several reporting periods
        # before stopping a query whose entire input was filtered out.
        io.checkpoint(query_id, count=30)
        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL));'''

        io.kikimr.ydb_client.query(f'''
            ALTER STREAMING QUERY `{query_id}` SET (
                RUN = TRUE, FORCE = TRUE, STREAMING_DISPOSITION = FROM_CHECKPOINT
            ) AS DO BEGIN
                {sql}
            END DO;
        ''')
        io.checkpoint(query_id)

        data = ['{"time": 203}', '{"time": 204}']
        io.write(data)
        expected = ['203', '204']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

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

    def test_3_sessions(self, ydb_topics):
        io = ydb_topics('test_3_sessions', shared=True)
        create_input_consumer(io)
        output_topic1 = io.output_topic
        output_topic2 = io.name + '_output2'
        output_topic3 = io.name + '_output3'
        external = io.kikimr.external_ydb_client
        extra_topics = [output_topic2, output_topic3]
        try:
            for topic in extra_topics:
                create_stream(topic, default_endpoint=io.endpoint)
                external.query(f'ALTER TOPIC `{topic}` ADD CONSUMER `{io.consumer_name}`;')

            sql1 = CONSUMER_PRAGMA + Rf'''
                INSERT INTO `{io.name}`.`{output_topic1}`
                SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) FROM {io.input}
                    WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
            sql2 = CONSUMER_PRAGMA + Rf'''
                INSERT INTO `{io.name}`.`{output_topic2}`
                SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) FROM {io.input}
                    WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

            sql3 = CONSUMER_PRAGMA + Rf'''
                INSERT INTO `{io.name}`.`{output_topic3}`
                SELECT Unwrap(Json::SerializeJson(Yson::From(TableRow()))) FROM {io.input}
                    WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''
            query_id1 = io.start(sql1)
            query_id2 = io.start(sql2)
            query_id3 = io.start(sql3)

            data = ['{"time":101}', '{"time":102}']

            io.write(data)
            expected = data
            assert io.read(len(expected), topic_path=output_topic1) == expected
            assert io.read(len(expected), topic_path=output_topic2) == expected
            assert io.read(len(expected), topic_path=output_topic3) == expected
            io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

            io.checkpoint(query_id1, count=2)
            io.stop(query_id1)

            data = ['{"time":103}', '{"time":104}']
            io.write(data)
            expected = data
            assert not io.read(1, topic_path=output_topic1, timeout=1)
            assert io.read(len(expected), topic_path=output_topic2) == expected
            assert io.read(len(expected), topic_path=output_topic3) == expected

            io.resume(query_id1, disposition='FROM_CHECKPOINT')

            assert io.read(len(expected), topic_path=output_topic1) == expected

            data = ['{"time":105}', '{"time":106}']
            io.write(data)
            expected = data
            assert io.read(len(expected), topic_path=output_topic1) == expected
            assert io.read(len(expected), topic_path=output_topic2) == expected
            assert io.read(len(expected), topic_path=output_topic3) == expected

            io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

            io.stop(query_id1)
            io.stop(query_id2)
            io.stop(query_id3)

            io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

        finally:
            for query in reversed(io.queries):
                io.kikimr.ydb_client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;')
            for topic in extra_topics:
                external.query(f'DROP TOPIC IF EXISTS `{topic}`;')

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

    def test_2_connection(self, ydb_topics):
        io = ydb_topics('test_2_connection', shared=True, partitions=2)
        second_source = io.name + '_second'
        client = io.kikimr.ydb_client
        external = io.kikimr.external_ydb_client
        client.query(f'''
            CREATE EXTERNAL DATA SOURCE `{second_source}` WITH (
                SOURCE_TYPE = 'Ydb', LOCATION = '{io.endpoint.endpoint}',
                DATABASE_NAME = '{io.endpoint.database}', AUTH_METHOD = 'NONE',
                SHARED_READING = 'TRUE', SHARED_READING_GROUP = '{second_source}'
            );
        ''')
        try:
            queries = []
            for index, source in enumerate((io.name, second_source), 1):
                consumer = ROW_DISPATCHER_CONSUMER + '_' + str(index)
                external.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{consumer}`;')
                queries.append(io.start(f'''
                    PRAGMA pq.Consumer = "{consumer}";
                    INSERT INTO `{source}`.`{io.output_topic}`
                    SELECT Cast(time as String) FROM `{source}`.`{io.input_topic}`
                        WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));
                '''))
            io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 4)
            for start, partition_key in ((100, 'partition_key1'), (110, 'partition_key2')):
                io.write([f'{{"time": {value}}}' for value in range(start, start + 10)], partition_key=partition_key)
            expected = [str(value) for value in range(100, 120) for _ in range(2)]
            assert sorted(io.read(len(expected))) == expected
            for query in queries:
                io.stop(query)
            io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)
        finally:
            for query in reversed(io.queries):
                client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;')
            client.query(f'DROP EXTERNAL DATA SOURCE IF EXISTS `{second_source}`;')

    @pytest.mark.parametrize('ydb_cluster', [{'compute_nodes': COMPUTE_NODE_COUNT}], indirect=True)
    def test_sensors(self, ydb_topics):
        io = ydb_topics('test_sensors', shared=True)
        create_input_consumer(io)
        query = io.start(CONSUMER_PRAGMA + f'''
            INSERT INTO {io.output}
            SELECT Cast(time as String) FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));
        ''')

        io.write(['{"time": 101}'])
        assert io.read(1) == ['101']

        io.wait_actor_count('DQ_PQ_READ_ACTOR', 1)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 1)
        io.wait_actor_count('FQ_ROW_DISPATCHER_COMPILE_SERVICE', COMPUTE_NODE_COUNT)
        io.wait_actor_count('FQ_ROW_DISPATCHER_FORMAT_HANDLER', 1)
        wait_ydb_row_dispatcher_sensor(io, {'sensor': 'ClientsCount'}, 1)
        wait_ydb_row_dispatcher_sensor(io, {'sensor': 'RowsSent'}, 1, exact_match=False)
        wait_ydb_row_dispatcher_sensor(io, {'sensor': 'IncomingRequests'}, 1, exact_match=False)
        last_stats = {}

        def source_statistics_reported():
            nonlocal last_stats
            last_stats = read_compute_ingress_stats(io)
            return (
                {'FilteredBytes', 'FilteredRows', 'QueuedBytes', 'QueuedRows'} <= last_stats.keys()
                and last_stats['FilteredBytes'] > 1
                and last_stats['FilteredRows'] > 0
                and last_stats['QueuedBytes'] >= 0
                and last_stats['QueuedRows'] >= 0
            )

        assert wait_for(source_statistics_reported, timeout_seconds=120, step_seconds=0.5), last_stats

        io.stop(query)
        io.wait_actor_count('DQ_PQ_READ_ACTOR', 0)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)
        wait_ydb_row_dispatcher_sensor(io, {'sensor': 'ClientsCount'}, 0)

    def test_group_by_hop_restart_query(self, ydb_topics):
        io = ydb_topics("test_group_by_hop_restart", shared=True)
        create_input_consumer(io)

        sql1 = CONSUMER_PRAGMA + Rf'''
            $data = SELECT * FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time String NOT NULL, project String NOT NULL));
            $hop_data = SELECT
                    CAST(HOP_END() as String) as time,
                    COUNT(*) as count
                FROM $data
                GROUP BY
                    HOP(CAST(time as TimeStamp), "PT10S", "PT10S", "PT0S");
            INSERT INTO {io.output}
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $hop_data;'''

        query_id = io.start(sql1)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": "2025-04-23T09:00:00.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:01.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:02.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:03.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:04.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:15.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:16.000000Z", "project": "project1"}',
        ]
        io.write(data)
        expected = ['{"count":5,"time":"2025-04-23T09:00:10Z"}']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.checkpoint(query_id, count=15)
        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

        data = [
            '{"time": "2025-04-23T09:00:17.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:18.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:21.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:25.000000Z", "project": "project1"}',
            '{"time": "2025-04-23T09:00:31.000000Z", "project": "project1"}',
        ]
        io.write(data)

        io.resume(query_id, disposition='FROM_CHECKPOINT')

        expected = ['{"count":4,"time":"2025-04-23T09:00:20Z"}', '{"count":2,"time":"2025-04-23T09:00:30Z"}']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

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
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == sorted(expected)

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
        assert sorted(io.read(len(expected), topic_path=io.output_topic)) == sorted(expected)

        io.stop(query_id)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 0)

    def test_huge_messages(self, ydb_topics):
        io = ydb_topics("test_huge", shared=True)
        create_input_consumer(io)

        sql = CONSUMER_PRAGMA + Rf'''
            $data = SELECT * FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, sql String NOT NULL, event String NOT NULL))
                WHERE event = "event1" or event = "event2";

            $data = SELECT time, LENGTH(sql) AS len FROM $data;

            INSERT INTO {io.output}
            SELECT ToBytes(Unwrap(Json::SerializeJson(Yson::From(TableRow())))) FROM $data;
                '''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

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

        io.write(data)
        expected = [
            f'{{"len":{len(large_string)},"time":101}}',
            f'{{"len":{len(huge_string)},"time":102}}',
            f'{{"len":{len(large_string)},"time":105}}',
            f'{{"len":{len(huge_string)},"time":106}}',
        ]
        # TODO: normalize json and tolerate order mismatch

        received = io.read(len(expected), topic_path=io.output_topic)

        issues = query_issues(io, query_id)
        assert "Failed to parse json message for offset" not in issues, "Incorrect Issues: " + issues
        assert_row_dispatcher_predicate(io, query_id, '')
        io.stop(query_id)
        assert received == expected

    @pytest.mark.parametrize("use_binding", [False, True], ids=["with_option", "bindings"])
    def test_json_errors(self, ydb_topics, use_binding):
        io = ydb_topics(f'test_json_errors_{use_binding}', shared=True)
        create_input_consumer(io)
        # Both former binding and inline cases now specify the schema in SQL.
        schema = (
            'SCHEMA = (time Int32 NOT NULL, data String NOT NULL, null_field String)'
            if use_binding
            else ('SCHEMA (time Int32 NOT NULL, data String NOT NULL, null_field String)')
        )
        sql = CONSUMER_PRAGMA + Rf'''
            INSERT INTO {io.output}
            SELECT data || COALESCE(cast(null_field as String), "_null") FROM {io.input}
                WITH (format=json_each_row, `skip.json.errors` = "true", {schema});
        '''

        query_id = io.start(sql)
        io.wait_actor_count("FQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": "hello1"}',
            '{"time": 102, "data": 7777}',
            '{"time": 103, "data": "hello2"}',
            '{"time": 104, "data": "hello3", "null_field": null}',
        ]

        io.write(data, partition_key="key")
        expected = ['hello1_null', 'hello2_null', 'hello3_null']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        wait_ydb_row_dispatcher_sensor(
            io,
            {
                'partition': '0',
                'format': 'json_each_row',
                'sensor': 'ParsingErrors',
            },
            1,
            exact_match=False,
        )
        wait_ydb_row_dispatcher_sensor(
            io,
            {
                'topic': io.input_topic,
                'sensor': 'JsonParsingErrors',
            },
            1,
            exact_match=False,
        )
        io.stop(query_id)

    def test_delete_topic(self, ydb_topics):
        io = ydb_topics("test_delete_topic", shared=True)
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
        io.checkpoint(query_id, count=2)
        io.stop(query_id)

        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)
        delete_stream(io.input_topic, default_endpoint=io.endpoint)
        create_stream(io.input_topic, default_endpoint=io.endpoint)
        create_input_consumer(io)

        io.resume(query_id, disposition='FROM_CHECKPOINT')

        data = [
            '{"time": 101, "data": "hello1", "event": "event1"}',
            '{"time": 102, "data": "hello2", "event": "event2"}',
            '{"time": 103, "data": "hello3", "event": "event3"}',
            '{"time": 104, "data": "hello4", "event": "event4"}',
        ]

        io.write(data)
        expected = ['104']
        assert io.read(len(expected), topic_path=io.output_topic) == expected

        io.stop(query_id)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)

    @pytest.mark.parametrize(
        'ydb_cluster', [{'compute_nodes': COMPUTE_NODE_COUNT, 'rebalancing_timeout_ms': '5000'}], indirect=True
    )
    @pytest.mark.parametrize('single_node', [False, True])
    def test_redistribute_partition_after_timeout(self, ydb_topics, single_node):
        partitions_count = 10
        io = ydb_topics('redistribute', partitions=partitions_count, shared=True)
        create_input_consumer(io)
        wait_ydb_row_dispatcher_sensor(io, {'sensor': 'KnownRowDispatchers'}, 2 * COMPUTE_NODE_COUNT - 1)

        scheduler = "PRAGMA ydb.MaxTasksPerStage = '1';" if single_node else ''
        query = io.start(CONSUMER_PRAGMA + f'''
            {scheduler}
            INSERT INTO {io.output}
            SELECT data FROM {io.input}
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));
        ''')
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', partitions_count)
        io.checkpoint(query)
        for index in range(partitions_count):
            io.write(['{"time": 100, "data": "hello"}'], partition_key=str(index))
        assert io.read(partitions_count) == ['hello'] * partitions_count
        # RD reports offsets periodically; include them in a checkpoint before stopping its node.
        io.checkpoint(query, count=15)

        nodes = counter_nodes(io.kikimr.cluster)
        # Isolate RD rebalancing from recovery of the query compute actors.
        session_node_id = next(
            node_id
            for node_id in nodes
            if io.get_actor_count(io.kikimr, node_id, 'FQ_ROW_DISPATCHER_SESSION') > 0
            and io.get_actor_count(io.kikimr, node_id, 'KQP_COMPUTE_ACTOR') == 0
        )
        session_node = nodes[session_node_id]
        alive_nodes = {node_id: node for node_id, node in nodes.items() if node_id != session_node_id}
        logging.debug('Stopping row dispatcher node: %s', session_node_id)
        session_node.stop()
        try:
            io.kikimr.recreate_driver(next(iter(alive_nodes)))
            for index in range(partitions_count):
                io.write(['{"time": 101, "data": "Relativitätstheorie"}'], partition_key=str(index))
            assert io.read(partitions_count, timeout=120) == ['Relativitätstheorie'] * partitions_count
            assert wait_for(
                lambda: sum(
                    io.get_actor_count(io.kikimr, node_id, 'FQ_ROW_DISPATCHER_SESSION') for node_id in alive_nodes
                )
                == partitions_count,
                timeout_seconds=120,
                step_seconds=0.5,
            ), 'Partitions were not reassigned after the row dispatcher timeout'
        finally:
            session_node.start()
            io.kikimr.wait_kqp_node_count(COMPUTE_NODE_COUNT)
        io.stop(query)
        io.wait_actor_count('FQ_ROW_DISPATCHER_SESSION', 0)

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
