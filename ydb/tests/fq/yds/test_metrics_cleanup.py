#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestCleanup(TestYdsBase):
    @yq_v1
    def test_cleanup(self, kikimr, client):
        sql = "SELECT 1;"
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        assert kikimr.compute_plane.get_task_count(1, query_id) == 0
        deadline = time.time() + yatest_common.plain_or_under_sanitizer(120, 500)
        while True:
            value = kikimr.compute_plane.get_sensors(1, "yq").find_sensor(
                {"query_id": query_id, "subsystem": "task_controller", "Stage": "Total", "sensor": "Tasks"}
            )
            if value is None:
                break
            assert time.time() < deadline, "TaskCount was not cleaned"
            time.sleep(0.5)

    @yq_v1
    def test_keep(self, kikimr, client):
        self.init_topics("cleanup_keep", create_output=False)
        client.create_yds_connection("myyds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))

        query_id = client.create_query("simple", "SELECT 1;", type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = R'''
            SELECT * FROM myyds.`{input_topic}`;
            '''.format(
            input_topic=self.input_topic,
        )
        client.modify_query(query_id, "simple", sql, type=fq.QueryContent.QueryType.STREAMING)

        deadline = time.time() + 90  # x1.5 of 60 sec
        while True:
            value = kikimr.compute_plane.get_sensors(1, "yq").find_sensor(
                {"query_id": query_id, "subsystem": "task_controller", "Stage": "Total", "sensor": "Tasks"}
            )
            assert value is not None, "Tasks was cleaned"
            if time.time() > deadline:
                break
            else:
                time.sleep(0.5)
