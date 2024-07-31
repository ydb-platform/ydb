#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import os
import time

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
import ydb.tests.library.common.yatest_common as yatest_common
import ydb.public.api.protos.draft.fq_pb2 as fq


class TestSelectTimings(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("success", [True, False], ids=["finished", "aborted"])
    @pytest.mark.parametrize(
        "query_type",
        [fq.QueryContent.QueryType.ANALYTICS, fq.QueryContent.QueryType.STREAMING],
        ids=["analytics", "streaming"],
    )
    def test_select_timings(self, kikimr, client, success, query_type):
        suffix = (str(query_type)[0] + str(success)[0]).lower()
        self.init_topics("select_timings_" + suffix, create_output=False)
        connection = "myyds_" + suffix

        # TBD auto-create consumer and read_rule in analytics query
        sql = "SELECT * FROM {connection}.`{input_topic}` LIMIT 1;".format(
            connection=connection,
            input_topic=self.input_topic,
        )

        client.create_yds_connection(connection, os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=query_type).result.query_id

        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

        meta = client.describe_query(query_id).result.query.meta
        assert meta.started_at.seconds != 0, "Must set started_at"
        assert meta.finished_at.seconds == 0, "Must not set finished_at"
        seconds = meta.started_at.seconds

        # streaming query should continue after cluster restart
        if query_type == fq.QueryContent.QueryType.STREAMING:
            kikimr.compute_plane.kikimr_cluster.nodes[1].stop()
            kikimr.compute_plane.kikimr_cluster.nodes[1].start()
            kikimr.compute_plane.wait_bootstrap(1)

            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
            kikimr.compute_plane.wait_zero_checkpoint(query_id, expect_counters_exist=False)

            meta = client.describe_query(query_id).result.query.meta
            assert meta.started_at.seconds == seconds, "Must not change started_at"
            assert meta.finished_at.seconds == 0, "Must not set finished_at"

        # will wait 30 sec for lease expiration, reduce timeout when is congifurable
        wait_query_timeout = yatest_common.plain_or_under_sanitizer(120, 300)
        if success:
            time.sleep(2)  # Workaround race between write and read "from now". Remove when YQ-589 will be done.
            self.write_stream(["Message"])
            client.wait_query_status(query_id, fq.QueryMeta.COMPLETED, timeout=wait_query_timeout)
        else:
            client.abort_query(query_id)
            client.wait_query_status(query_id, fq.QueryMeta.ABORTED_BY_USER, timeout=wait_query_timeout)

        meta = client.describe_query(query_id).result.query.meta
        assert meta.started_at.seconds == seconds, "Must not change started_at (meta={})".format(meta)
        assert meta.finished_at.seconds >= seconds, "Must set finished_at (meta={})".format(meta)
