#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import time

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb_value


class TestYqStreaming(TestYdsBase):
    @yq_v1
    def test_yq_streaming(self, kikimr, client, yq_version):
        self.init_topics(f"pq_yq_streaming_{yq_version}")

        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";
            INSERT INTO myyds.`{output_topic}`
                SELECT STREAM (Data || Data) ?? "" As Data FROM myyds.`{input_topic}`;
            ''' \
            .format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )

        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        data = [
            '{"Data" = "hello";}',
        ]
        self.write_stream(data)

        expected = [
            '{"Data" = "hello";}{"Data" = "hello";}'
        ]
        assert self.read_stream(len(expected)) == expected

        client.describe_query(query_id)

        client.abort_query(query_id)

        client.wait_query(query_id)

        describe_response = client.describe_query(query_id)
        status = describe_response.result.query.meta.status
        assert not describe_response.issues, str(describe_response.issues)
        assert status == fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.ComputeStatus.Name(status)

    @yq_v1
    def test_yq_streaming_read_from_binding(self, kikimr, client, yq_version):
        self.init_topics(f"pq_yq_read_from_binding_{yq_version}")

        #  Consumer and topics to create are written in ya.make file.
        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO myyds2.`{output_topic}`
                SELECT STREAM key ?? "" FROM bindings.my_binding;
            ''' \
            .format(
            output_topic=self.output_topic
        )

        connection_response = client.create_yds_connection("myyds2", os.getenv("YDB_DATABASE"),
                                                           os.getenv("YDB_ENDPOINT"))
        logging.debug(str(connection_response))
        assert not connection_response.issues, str(connection_response.issues)

        keyColumn = ydb_value.Column(name="key", type=ydb_value.Type(
            optional_type=ydb_value.OptionalType(item=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.STRING))))

        binding_response = client.create_yds_binding(name="my_binding",
                                                     stream=self.input_topic,
                                                     format="json_each_row",
                                                     connection_id=connection_response.result.connection_id,
                                                     columns=[keyColumn])
        logging.debug(str(binding_response))
        assert not binding_response.issues, str(binding_response.issues)

        response = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING)
        assert not response.issues, str(response.issues)
        logging.debug(str(response.result))
        client.wait_query_status(response.result.query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(response.result.query_id)

        #  Write to input.
        data = [
            '{"key": "abc"}',
            '{"key": "xxx"}'
        ]

        self.write_stream(data)
        logging.info("Data was written: {}".format(data))

        # Read from output.
        expected = [
            'abc',
            'xxx'
        ]

        read_data = self.read_stream(len(expected))
        logging.info("Data was read: {}".format(read_data))

        assert read_data == expected

        describe_response = client.describe_query(response.result.query_id)
        assert not describe_response.issues, str(describe_response.issues)
        logging.debug(str(describe_response.result))

        client.abort_query(response.result.query_id)

        client.wait_query(response.result.query_id)

    @yq_v1
    def test_yq_streaming_read_from_binding_date_time(self, kikimr, client, yq_version):
        self.init_topics(f"pq_yq_read_from_binding_date_time_{yq_version}")

        #  Consumer and topics to create are written in ya.make file.
        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO myyds4.`{output_topic}`
                SELECT STREAM Unwrap(key || CAST(value as String)) as data FROM bindings.my_binding4;
            ''' \
            .format(
            output_topic=self.output_topic
        )

        connection_response = client.create_yds_connection("myyds4", os.getenv("YDB_DATABASE"),
                                                           os.getenv("YDB_ENDPOINT"))
        logging.debug(str(connection_response))
        assert not connection_response.issues, str(connection_response.issues)

        keyColumn = ydb_value.Column(name="key", type=ydb_value.Type(
            optional_type=ydb_value.OptionalType(item=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.STRING))))

        valueColumn = ydb_value.Column(name="value",
                                       type=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.DATETIME))

        binding_response = client.create_yds_binding(name="my_binding4",
                                                     stream=self.input_topic,
                                                     format="json_each_row",
                                                     connection_id=connection_response.result.connection_id,
                                                     columns=[keyColumn, valueColumn])
        logging.debug(str(binding_response))
        assert not binding_response.issues, str(binding_response.issues)

        response = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING)
        assert not response.issues, str(response.issues)
        logging.debug(str(response.result))
        client.wait_query_status(response.result.query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(response.result.query_id)

        #  Write to input.
        data = [
            '{"key": "abc", "value": "2022-10-19 16:40:47"}',
            '{"key": "xxx", "value": "2022-10-19 16:40:48"}'
        ]

        self.write_stream(data)
        logging.info("Data was written: {}".format(data))

        # Read from output.
        expected = [
            'abc2022-10-19T16:40:47Z',
            'xxx2022-10-19T16:40:48Z'
        ]

        read_data = self.read_stream(len(expected))
        logging.info("Data was read: {}".format(read_data))

        assert read_data == expected

        describe_response = client.describe_query(response.result.query_id)
        assert not describe_response.issues, str(describe_response.issues)
        logging.debug(str(describe_response.result))

        client.abort_query(response.result.query_id)

        client.wait_query(response.result.query_id)

    @yq_v1
    def test_yq_streaming_read_date_time_format(self, kikimr, client, yq_version):
        self.init_topics(f"pq_yq_read_from_binding_dt_format_settings_{yq_version}")

        #  Consumer and topics to create are written in ya.make file.
        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO myyds3.`{output_topic}`
                SELECT STREAM Unwrap(key || CAST(value as String)) as data FROM bindings.my_binding3;
            ''' \
            .format(
            output_topic=self.output_topic
        )

        connection_response = client.create_yds_connection("myyds3", os.getenv("YDB_DATABASE"),
                                                           os.getenv("YDB_ENDPOINT"))
        logging.debug(str(connection_response))
        assert not connection_response.issues, str(connection_response.issues)

        keyColumn = ydb_value.Column(name="key", type=ydb_value.Type(
            optional_type=ydb_value.OptionalType(item=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.STRING))))

        valueColumn = ydb_value.Column(name="value",
                                       type=ydb_value.Type(type_id=ydb_value.Type.PrimitiveTypeId.DATETIME))

        binding_response = client.create_yds_binding(name="my_binding3",
                                                     stream=self.input_topic,
                                                     format="json_each_row",
                                                     connection_id=connection_response.result.connection_id,
                                                     columns=[keyColumn, valueColumn],
                                                     format_setting={"data.datetime.format_name": "ISO"})
        logging.debug(str(binding_response))
        assert not binding_response.issues, str(binding_response.issues)

        response = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING)
        assert not response.issues, str(response.issues)
        logging.debug(str(response.result))
        client.wait_query_status(response.result.query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(response.result.query_id)

        #  Write to input.
        data = [
            '{"key": "abc", "value": "2022-10-19T16:40:47Z"}',
            '{"key": "xxx", "value": "2022-10-19T16:40:48Z"}'
        ]

        self.write_stream(data)
        logging.info("Data was written: {}".format(data))

        # Read from output.
        expected = [
            'abc2022-10-19T16:40:47Z',
            'xxx2022-10-19T16:40:48Z'
        ]

        read_data = self.read_stream(len(expected))
        logging.info("Data was read: {}".format(read_data))

        assert read_data == expected

        describe_response = client.describe_query(response.result.query_id)
        assert not describe_response.issues, str(describe_response.issues)
        logging.debug(str(describe_response.result))

        client.abort_query(response.result.query_id)

        client.wait_query(response.result.query_id)

    @yq_v1
    def test_state_load_mode(self, kikimr, client, yq_version):
        self.init_topics(f"pq_test_state_load_mode_{yq_version}")

        sql = R'''
            INSERT INTO myyds1.`{output_topic}`
                SELECT STREAM (Data || Data) ?? "" As Data FROM myyds1.`{input_topic}`;
            ''' \
            .format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )

        client.create_yds_connection("myyds1", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        name = "simple"
        create_query_result = client.create_query(name, sql, type=fq.QueryContent.QueryType.STREAMING).result
        query_id = create_query_result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        data = [
            '{"Data" = "hello";}',
        ]
        self.write_stream(data)

        expected = [
            '{"Data" = "hello";}{"Data" = "hello";}'
        ]
        assert self.read_stream(len(expected)) == expected

        client.describe_query(query_id)

        client.abort_query(query_id)

        client.wait_query(query_id)

        describe_response = client.describe_query(query_id)
        status = describe_response.result.query.meta.status
        assert not describe_response.issues, str(describe_response.issues)
        assert status == fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.ComputeStatus.Name(status)

        self.init_topics("test_state_load_mode2")
        new_sql = R'''
            INSERT INTO myyds1.`{output_topic}`
                SELECT STREAM (Data || CAST(COUNT(*) as string)) ?? "" as cnt
                FROM myyds1.`{input_topic}`
                GROUP BY Data, HOP(Just(CurrentUtcTimestamp(TableRow())), "PT1S", "PT1S", "PT1S")
                ;
            ''' \
            .format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )
        client.modify_query(query_id, name, new_sql, type=fq.QueryContent.QueryType.STREAMING,
                            state_load_mode=fq.StateLoadMode.EMPTY)
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_completed_checkpoints(query_id,
                                                        kikimr.compute_plane.get_completed_checkpoints(query_id) + 1)

        modified_data = [
            "hello new query"
        ]
        modified_expected = [
            "hello new query1"
        ]
        self.write_stream(modified_data)
        time.sleep(5)
        self.write_stream(modified_data)
        assert self.read_stream(len(modified_expected)) == modified_expected

        client.abort_query(query_id)
        client.wait_query(query_id)
