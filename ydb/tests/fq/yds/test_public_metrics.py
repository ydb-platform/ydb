#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import time

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestPublicMetrics(TestYdsBase):
    @yq_v1
    def test_select_limit(self, kikimr, client):
        self.init_topics("public_select_limit", create_output=False)

        sql = R'''
            SELECT * FROM myyds1.`{input_topic}` LIMIT 2;
            '''.format(
            input_topic=self.input_topic,
        )

        cloud_id = "mock_cloud"
        folder_id = "my_folder"

        client.create_yds_connection("myyds1", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        self.write_stream(["A", "B"])
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        metrics = kikimr.compute_plane.get_sensors(1, "yq_public")
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.running_tasks"}
            )
            == 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.cpu_usage_us"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.memory_usage_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.input_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.output_bytes"}
            )
            is None
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.uptime_seconds"}
            )
            >= 0
        )

    @yq_v1
    @pytest.mark.parametrize("stats_mode", ["STATS_MODE_FULL"])
    def test_select_unlimited(self, kikimr, client):
        self.init_topics("public_select_unlimited")

        sql = R'''
            INSERT INTO myyds2.`{output_topic}`
            SELECT * FROM myyds2.`{input_topic}`;
            '''.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )

        cloud_id = "mock_cloud"
        folder_id = "my_folder"

        client.create_yds_connection("myyds2", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        data = ["A", "B", "C", "D"]
        self.write_stream(data)
        assert self.read_stream(len(data)) == data

        time.sleep(5)  # TODO: fix and remove
        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)

        metrics = kikimr.compute_plane.get_sensors(1, "yq_public")
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.running_tasks"}
            )
            == 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.cpu_usage_us"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.memory_usage_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.input_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.output_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.uptime_seconds"}
            )
            >= 0
        )
        # TODO: rows must be fixed in YQ-2592
        # assert metrics.find_sensor({"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id,
        #                             "name": "query.source_input_records"}) > 0
        # assert metrics.find_sensor({"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id,
        #                             "name": "query.sink_output_records"}) > 0
        # assert metrics.find_sensor(
        #     {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.late_events"}) == 0
