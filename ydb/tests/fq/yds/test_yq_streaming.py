#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import time

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestYqStreaming(TestYdsBase):
    def test_yq_streaming(self, ydb_topics):
        io = ydb_topics('pq_yq_streaming')
        query = io.start(f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            INSERT INTO {io.output} SELECT (Data || Data) ?? '' AS Data FROM {io.input};
        """)
        data = ['{"Data" = "hello";}']
        io.write(data)
        assert io.read(1) == ['{"Data" = "hello";}{"Data" = "hello";}']
        assert io.state(query)['Status'] == 'RUNNING'
        io.stop(query)
        assert io.state(query)['Status'] == 'STOPPED'

    def test_yq_streaming_read_from_binding(self, ydb_topics):
        io = ydb_topics('pq_yq_read_from_binding')
        query = io.start(f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            INSERT INTO {io.output}
            SELECT key ?? '' AS Data FROM {io.input} WITH (
                FORMAT = json_each_row, SCHEMA = (key String)
            );
        """)
        data = ['{"key": "abc"}', '{"key": "xxx"}']
        io.write(data)
        expected = ['abc', 'xxx']
        assert io.read(len(expected)) == expected
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)
        io.stop(query)

    def test_yq_streaming_read_from_binding_date_time(self, ydb_topics):
        io = ydb_topics('pq_yq_read_from_binding_date_time')
        query = io.start(f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            INSERT INTO {io.output}
            SELECT Unwrap(key || CAST(value AS String)) AS Data FROM {io.input} WITH (
                FORMAT = json_each_row,
                SCHEMA = (key String, value Datetime NOT NULL),
                `data.datetime.format` = '%Y-%m-%d %H:%M:%S'
            );
        """)
        data = ['{"key": "abc", "value": "2022-10-19 16:40:47"}', '{"key": "xxx", "value": "2022-10-19 16:40:48"}']
        io.write(data)
        expected = ['abc2022-10-19T16:40:47Z', 'xxx2022-10-19T16:40:48Z']
        assert io.read(len(expected)) == expected
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)
        io.stop(query)

    def test_yq_streaming_read_date_time_format(self, ydb_topics):
        io = ydb_topics('pq_yq_read_from_binding_dt_format_settings')
        query = io.start(f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            INSERT INTO {io.output}
            SELECT Unwrap(key || CAST(value AS String)) AS Data FROM {io.input} WITH (
                FORMAT = json_each_row,
                SCHEMA = (key String, value Datetime NOT NULL),
                `data.datetime.format_name` = 'ISO'
            );
        """)
        data = ['{"key": "abc", "value": "2022-10-19T16:40:47Z"}', '{"key": "xxx", "value": "2022-10-19T16:40:48Z"}']
        io.write(data)
        expected = ['abc2022-10-19T16:40:47Z', 'xxx2022-10-19T16:40:48Z']
        assert io.read(len(expected)) == expected
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)
        io.stop(query)

    @yq_v1
    def test_state_load_mode(self, kikimr, client, yq_version):
        self.init_topics(f"pq_test_state_load_mode_{yq_version}")

        sql = R'''
            INSERT INTO myyds1.`{output_topic}`
                SELECT STREAM (Data || Data) ?? "" As Data FROM myyds1.`{input_topic}`;
            '''.format(
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

        expected = ['{"Data" = "hello";}{"Data" = "hello";}']
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
            '''.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )
        client.modify_query(
            query_id, name, new_sql, type=fq.QueryContent.QueryType.STREAMING, state_load_mode=fq.StateLoadMode.EMPTY
        )
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_completed_checkpoints(
            query_id, kikimr.compute_plane.get_completed_checkpoints(query_id) + 1
        )

        modified_data = ["hello new query"]
        modified_expected = ["hello new query1"]
        self.write_stream(modified_data)
        time.sleep(5)
        self.write_stream(modified_data)
        assert self.read_stream(len(modified_expected)) == modified_expected

        client.abort_query(query_id)
        client.wait_query(query_id)

    def test_early_finish_case1(self, ydb_topics):
        io = ydb_topics('pq_test_early_finish_case1')
        sql = f"""
            $a = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t1 FROM {io.input} WITH (STREAMING="TRUE") limit 100;
            $b = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t2 FROM {io.output} WITH (STREAMING="TRUE") limit 3;
            select * from $a as a
            join $b as b ON a.t1 = b.t2 limit 1;
        """
        future = io.start_select(sql, [io.input_topic, io.output_topic])
        for i in range(10):
            data = ['{{"key": "{}"}}'.format(i)]
            io.write(data, topic_path=io.output_topic)

        for i in range(105):
            data = ['{{"key": "{}"}}'.format(i + 100)]
            io.write(data)
        result = future.result(timeout=120)
        assert len(result) == 1

    def test_early_finish_case2(self, ydb_topics):
        io = ydb_topics('pq_test_early_finish_case2')
        sql = f"""
            $a = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t1 FROM {io.input} WITH (STREAMING="TRUE") limit 100;
            $b = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t2 FROM {io.output} WITH (STREAMING="TRUE") limit 3;
            select * from $a as a
            join $b as b ON a.t1 = b.t2;
        """
        future = io.start_select(sql, [io.input_topic, io.output_topic])
        for i in range(10):
            data = ['{{"key": "{}"}}'.format(i)]
            io.write(data, topic_path=io.output_topic)

        for i in range(105):
            data = ['{{"key": "{}"}}'.format(i + 100)]
            io.write(data)
        result = future.result(timeout=120)
        assert len(result) == 1

    def test_early_finish_case3(self, ydb_topics):
        io = ydb_topics('pq_test_early_finish_case3')
        sql = f"""
            $a = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t1 FROM {io.input} WITH (STREAMING="TRUE") limit 3;
            $b = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t2 FROM {io.output} WITH (STREAMING="TRUE") limit 3;
            select * from $a as a
            join $b as b ON a.t1 = b.t2
            limit 3;
        """
        future = io.start_select(sql, [io.input_topic, io.output_topic])
        for i in range(10):
            data = ['{{"key": "{}"}}'.format(i)]
            io.write(data, topic_path=io.output_topic)

        for i in range(10):
            data = ['{{"key": "{}"}}'.format(i)]
            io.write(data)
        result = future.result(timeout=120)
        assert len(result) == 1
        assert len(result[0].rows) == 3

    def test_early_finish_case4(self, ydb_topics):
        io = ydb_topics('pq_test_early_finish_case4')
        sql = f"""
            $a = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t1 FROM {io.input} WITH (STREAMING="TRUE");
            $b = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t2 FROM {io.output} WITH (STREAMING="TRUE") limit 10;
            select * from $a as a
            left join $b as b ON a.t1 = b.t2
            limit 3;
        """
        future = io.start_select(sql, [io.input_topic, io.output_topic])
        for i in range(10):
            data = ['{{"key": "{}"}}'.format(i)]
            io.write(data, topic_path=io.output_topic)

        for i in range(10):
            data = ['{{"key": "{}"}}'.format(i)]
            io.write(data)
        result = future.result(timeout=120)
        assert len(result) == 1
        assert len(result[0].rows) == 3

    def test_early_finish_case5(self, ydb_topics):
        io = ydb_topics('pq_test_early_finish_case5')
        sql = f"""
            $a = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t1 FROM {io.input} WITH (STREAMING="TRUE");
            $b = SELECT JSON_VALUE(Cast(Data as Json), "$.key") as t2 FROM {io.output} WITH (STREAMING="TRUE") limit 10;
            select * from $a as a
            left join $b as b ON a.t1 = b.t2
            limit 3;
        """
        future = io.start_select(sql, [io.input_topic, io.output_topic])
        for i in range(10):
            data = ['{{"key": "{}"}}'.format(i)]
            io.write(data, topic_path=io.output_topic)

        for i in range(20):
            data = ['{{"key": "{}"}}'.format(i + 20)]
            io.write(data)
            time.sleep(1)

        for i in range(10):
            data = ['{{"key": "{}"}}'.format(i)]
            io.write(data)
        result = future.result(timeout=120)
        assert len(result) == 1
        assert len(result[0].rows) == 3

    def test_match_recognize_sink(self, ydb_topics):
        io = ydb_topics('pq_test_match_recognize_sink')
        sql = R'''
            PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;
            PRAGMA FeatureR010="prototype";
            PRAGMA config.flags("TimeOrderRecoverAhead", "20000000");
            PRAGMA config.flags("TimeOrderRecoverDelay", "-20000000");
            $parsed = SELECT JSON_VALUE(Cast(Data as Json), "$.event_class") as event_class, JSON_VALUE(Cast(Data as Json), "$.time") as time FROM {input_topic} WITH (FORMAT = raw, SCHEMA = (Data String NOT NULL));

            $tojson = ($data) -> (ToBytes(Unwrap(Yson2::SerializeJson(Yson::From($data)))));

            INSERT INTO {output_topic}
            (SELECT $tojson(`result`) AS data FROM
                (SELECT TableRow() AS `result` FROM (
            SELECT "bank" AS _service, "finish" AS action_state, "success" AS action_status, "present" AS action_type,
                "MK8SCreateNamespace" AS event_class, "normalized" AS event_type, "host" AS object_type, "vmhost" AS reporter_type, "mk8s" AS source, "user" AS subject_type
            FROM (
                SELECT * FROM ((
                SELECT * FROM $parsed MATCH_RECOGNIZE(
                    ORDER BY CAST(time as Timestamp)
                    ONE ROW PER MATCH
                    PATTERN (
                        (MK8SCNMK8SCN24h0m0s)
                    )
                    DEFINE
                        MK8SCNMK8SCN24h0m0s as ((MK8SCNMK8SCN24h0m0s.event_class IS NOT DISTINCT FROM "ManagedKubernetesEvent"))
                )))) AS MATCHED
            ))
            )
            '''.format(input_topic=io.input, output_topic=io.output)
        query_id = io.start(sql)
        data = [
            '{"event_class": "ManagedKubernetesEvent", "time": "2024-01-03T00:00:00Z"}',
            '{"event_class": "ManagedKubernetesEvent", "time": "2024-01-03T00:00:15Z"}',
            '{"event_class": "ManagedKubernetesEvent", "time": "2024-01-03T00:00:30Z"}',
            '{"event_class": "ManagedKubernetesEvent", "time": "2024-01-03T00:00:45Z"}',
            '{"event_class": "ManagedKubernetesEvent", "time": "2024-01-03T00:01:00Z"}',
        ]
        io.write(data)
        read_data = io.read(3)
        logging.info("Data was read: {}".format(read_data))
        assert len(read_data) == 3
        io.stop(query_id)
