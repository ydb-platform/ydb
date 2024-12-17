#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import pytest
import time

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestKillPqBill(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_do_not_bill_pq(self, kikimr, client):
        self.init_topics("no_pq_bill")

        sql = f'''
            PRAGMA dq.MaxTasksPerStage="2";

            INSERT INTO yds.`{self.output_topic}`
            SELECT Data AS Data
            FROM yds.`{self.input_topic}`;'''

        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")
        query_id = client.create_query(
            "simple", sql, type=fq.QueryContent.QueryType.STREAMING, vcpu_time_limit=1
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.compute_plane.wait_zero_checkpoint(query_id)

        data_1mb = ['1' * 1024 * 1024]
        message_count = 15
        for _ in range(0, message_count):
            self.write_stream(data_1mb)
        self.read_stream(message_count)

        # TODO: fix this place. We need to correct to account for the ingress bytes in case of an aborted query.
        for _ in range(20):
            stat = json.loads(client.describe_query(query_id).result.query.statistics.json)
            graph_name = "Graph=0"
            if graph_name in stat and "IngressBytes" in stat[graph_name]:
                break
            time.sleep(1)

        client.abort_query(query_id)
        client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER)

        stat = json.loads(client.describe_query(query_id).result.query.statistics.json)

        graph_name = "Graph=0"
        ingress_bytes = stat[graph_name]["IngressBytes"]["sum"]

        assert ingress_bytes >= 15 * 1024 * 1024, "Ingress must be >= 15MB"
        assert sum(kikimr.control_plane.get_metering(1)) == 10
