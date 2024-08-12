#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import pytest
import time

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestCpuQuota(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_cpu_quota(self, kikimr, client):
        self.init_topics("cpu_quota")

        sql = R'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO yds.`{output_topic}`
            SELECT Unwrap(ListConcat(ListReplicate(Data, 100000))) AS Data
            FROM yds.`{input_topic}`;'''.format(
            input_topic=self.input_topic,
            output_topic=self.output_topic,
        )

        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")
        query_id = client.create_query(
            "simple", sql, type=fq.QueryContent.QueryType.STREAMING, vcpu_time_limit=1
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        data = [
            'ABC',
            'DEF',
            'GHI',
        ]
        self.write_stream(data)
        self.read_stream(len(data))

        # One more time after pause
        time.sleep(0.5)
        self.write_stream(data)
        self.read_stream(len(data))

        time.sleep(0.5)
        sensors = kikimr.compute_plane.get_sensors(1, "dq_tasks")

        def calc_histogram_counter(sensor):
            s = 0
            for i in sensor["hist"]["buckets"]:
                s += i
            s += sensor["hist"]["inf"]
            return s

        get_quota_latency = 0
        quota_wait_delay = 0

        for sensor in sensors.data:
            labels = sensor["labels"]
            if labels.get("operation") != query_id:
                continue

            if labels.get("sensor") == "CpuTimeGetQuotaLatencyMs":
                get_quota_latency += calc_histogram_counter(sensor)
            if labels.get("sensor") == "CpuTimeQuotaWaitDelayMs":
                quota_wait_delay += calc_histogram_counter(sensor)

        assert get_quota_latency > 0
        assert quota_wait_delay > 0

        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)

        logging.debug("Sensors: {}".format(sensors))
