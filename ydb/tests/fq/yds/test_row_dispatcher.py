#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import logging
import time

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig

from ydb.tests.tools.datastreams_helpers.control_plane import list_read_rules
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, create_read_rule
from ydb.tests.tools.datastreams_helpers.data_plane import read_stream
from ydb.tests.tools.fq_runner.fq_client import StreamingDisposition

import ydb.public.api.protos.draft.fq_pb2 as fq

YDS_CONNECTION = "yds"


@pytest.fixture
def kikimr(request):
    kikimr_conf = StreamingOverKikimrConfig(
        cloud_mode=True, node_count={"/cp": TenantConfig(1), "/compute": TenantConfig(2)}
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.compute_plane.fq_config['row_dispatcher']['enabled'] = True
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


class TestPqRowDispatcher(TestYdsBase):

    @yq_v1
    def test_read_raw_format_without_row_dispatcher(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
        )
        self.init_topics("test_read_raw_format_without_row_dispatcher", create_output=False)

        output_topic = "pq_test_pq_read_write_output"

        create_stream(output_topic, partitions_count=1)
        create_read_rule(output_topic, self.consumer_name)

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{output_topic}`
            SELECT * FROM {YDS_CONNECTION}.`{self.input_topic}`;'''

        query_id = start_yds_query(kikimr, client, sql)
        data = ['{"time" = 101;}', '{"time" = 102;}']

        self.write_stream(data)
        expected = data
        assert self.read_stream(len(expected), topic_path=output_topic) == expected

        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)
        stop_yds_query(client, query_id)

    @yq_v1
    def test_simple_not_null(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
        )
        self.init_topics("test_simple_not_null")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)
        time.sleep(10)

        data = [
            '{"time": 101, "data": "hello1", "event": "event1"}',
            '{"time": 102, "data": "hello2", "event": "event2"}',
            '{"time": 103, "data": "hello3", "event": "event3"}',
        ]

        self.write_stream(data)
        expected = ['101', '102', '103']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        stop_yds_query(client, query_id)
        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    @pytest.mark.skip(reason="Is not implemented")
    def test_simple_optional(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
        )
        self.init_topics("test_simple_optional")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101, "data": "hello1", "event": "event1"}', '{"time": 102, "event": "event2"}']

        self.write_stream(data)
        expected = ['101', '102']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)

        stop_yds_query(client, query_id)
        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_scheme_error(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
        )
        self.init_topics("test_scheme_error")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"this": "is", not json}']
        self.write_stream(data)

        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)
        assert "Failed to unwrap empty optional" in issues, "Incorrect Issues: " + issues

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 0)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)
        data = ['{"time": 101, "data": "hello1", "event": "event1"}']
        self.write_stream(data)
        expected = ['101']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected
        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_filter(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
        )
        self.init_topics("test_filter")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time UInt64 NOT NULL, data String NOT NULL, event String NOT NULL))
                WHERE time > 101UL or event = "event2";'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        data = [
            '{"time": 101, "data": "hello1", "event": "event1"}',
            '{"time": 102, "data": "hello2", "event": "event2"}',
        ]

        self.write_stream(data)
        expected = ['102']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)

        stop_yds_query(client, query_id)
        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

        issues = str(client.describe_query(query_id).result.query.transient_issue)
        assert "Row dispatcher will use the predicate: WHERE (time > 101" in issues, "Incorrect Issues: " + issues

    @yq_v1
    def test_start_new_query(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
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
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

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
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
            WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL, data String NOT NULL));'''
        query_id3 = start_yds_query(kikimr, client, sql3)

        data = [
            '{"time": 103, "data": "hello3", "event": "event3"}',
            '{"time": 104, "data": "hello4", "event": "event4"}',
        ]

        self.write_stream(data)
        expected = ['103', '104']

        assert self.read_stream(len(expected), topic_path=output_topic1) == expected
        assert self.read_stream(len(expected), topic_path=output_topic2) == expected
        assert self.read_stream(len(expected), topic_path=output_topic3) == expected

        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        assert not read_stream(output_topic1, 1, True, self.consumer_name, timeout=1)
        assert not read_stream(output_topic2, 1, True, self.consumer_name, timeout=1)
        assert not read_stream(output_topic3, 1, True, self.consumer_name, timeout=1)

        stop_yds_query(client, query_id1)
        stop_yds_query(client, query_id2)
        stop_yds_query(client, query_id3)

        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_stop_start(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
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
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101}', '{"time": 102}']
        self.write_stream(data)
        expected = ['101', '102']
        assert self.read_stream(len(expected), topic_path=output_topic) == expected

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )
        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

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

        stop_yds_query(client, query_id)  # TODO?
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_restart_compute_node(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
        )
        self.init_topics("test_restart_compute_node")

        sql = Rf'''
            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT Cast(time as String) FROM {YDS_CONNECTION}.`{self.input_topic}`
                WITH (format=json_each_row, SCHEMA (time Int32 NOT NULL));'''

        query_id = start_yds_query(kikimr, client, sql)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        data = ['{"time": 101, "data": "hello1"}', '{"time": 102, "data": "hello2"}']

        self.write_stream(data)
        expected = ['101', '102']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        wait_actor_count(kikimr, "DQ_PQ_READ_ACTOR", 1)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        node_index = 2
        logging.debug("Restart compute node {}".format(node_index))
        kikimr.compute_plane.kikimr_cluster.nodes[node_index].stop()
        kikimr.compute_plane.kikimr_cluster.nodes[node_index].start()
        kikimr.compute_plane.wait_bootstrap(node_index)

        data = ['{"time": 103, "data": "hello3"}', '{"time": 104, "data": "hello4"}']
        self.write_stream(data)
        expected = ['103', '104']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        node_index = 1
        logging.debug("Restart compute node {}".format(node_index))
        kikimr.control_plane.kikimr_cluster.nodes[node_index].stop()
        kikimr.control_plane.kikimr_cluster.nodes[node_index].start()
        kikimr.control_plane.wait_bootstrap(node_index)

        data = ['{"time": 105, "data": "hello5"}', '{"time": 106, "data": "hello6"}']
        self.write_stream(data)
        expected = ['105', '106']
        assert self.read_stream(len(expected), topic_path=self.output_topic) == expected

        stop_yds_query(client, query_id)
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)

    @yq_v1
    def test_3_session(self, kikimr, client):
        client.create_yds_connection(
            YDS_CONNECTION, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"), use_row_dispatcher=True
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
        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        kikimr.compute_plane.wait_completed_checkpoints(
            query_id1, kikimr.compute_plane.get_completed_checkpoints(query_id1) + 1
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

        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 1)

        stop_yds_query(client, query_id1)
        stop_yds_query(client, query_id2)
        stop_yds_query(client, query_id3)

        wait_actor_count(kikimr, "YQ_ROW_DISPATCHER_SESSION", 0)
