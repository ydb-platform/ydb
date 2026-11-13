#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase


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

    def test_state_load_mode(self, ydb_topics):
        first = ydb_topics('state_load_mode')
        second = ydb_topics('state_load_mode_new')
        query = first.start(f"""
            INSERT INTO {first.output} SELECT (Data || Data) ?? '' AS Data FROM {first.input};
        """)
        try:
            first.write(['hello'])
            assert first.read(1) == ['hellohello']
            first.checkpoint(query, count=3)
            first.stop(query)
            # FORCE with a new query text and FRESH disposition discards the
            # incompatible old state, like YQ's StateLoadMode.EMPTY.
            first.kikimr.ydb_client.query(f"""
                ALTER STREAMING QUERY `{query}` SET (
                    RUN = TRUE, FORCE = TRUE, STREAMING_DISPOSITION = FRESH
                ) AS DO BEGIN
                    INSERT INTO {second.output}
                    SELECT Unwrap(data || CAST(COUNT(*) AS String)) AS Data
                    FROM {second.input} WITH (
                        FORMAT = json_each_row, SCHEMA = (time Int64 NOT NULL, data String NOT NULL)
                    )
                    GROUP BY data, HOP(CAST(time AS Timestamp), 'PT1S', 'PT1S', 'PT1S');
                END DO;
            """)
            first.checkpoint(query, count=3)
            assert first.state(query)['Status'] == 'RUNNING', first.state(query)
            second.write(['{"time":1,"data":"hello new query"}', '{"time":2000000,"data":"close"}'])
            assert second.read(1) == ['hello new query1']
            first.stop(query)
        finally:
            # The query refers to the second helper's source and must be dropped first.
            first.kikimr.ydb_client.query(f'DROP STREAMING QUERY IF EXISTS `{query}`;')

    @pytest.mark.skip(reason="Join of two streaming inputs is not supported in YDB")
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

    @pytest.mark.skip(reason="Join of two streaming inputs is not supported in YDB")
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

    @pytest.mark.skip(reason="Join of two streaming inputs is not supported in YDB")
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

    @pytest.mark.skip(reason="Streaming right input is not supported for LEFT join in YDB")
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

    @pytest.mark.skip(reason="Streaming right input is not supported for LEFT join in YDB")
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
            $parsed = SELECT
                JSON_VALUE(Cast(Data as Json), "$.event_class") as event_class,
                JSON_VALUE(Cast(Data as Json), "$.time") as time
            FROM {input_topic} WITH (FORMAT = raw, SCHEMA = (Data String NOT NULL));

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
