#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import pytest
import time

import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.control_plane import delete_stream


class TestRestartQuery(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize(
        "query_type",
        [fq.QueryContent.QueryType.ANALYTICS, fq.QueryContent.QueryType.STREAMING],
        ids=["analytics", "streaming"],
    )
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_restart_runtime_errors(self, kikimr_many_retries, client_many_retries, query_type):
        streaming_query = query_type == fq.QueryContent.QueryType.STREAMING
        unique_suffix = "_1" if streaming_query else "_2"

        connection_name = "yds" + unique_suffix
        client_many_retries.create_yds_connection(name=connection_name, database_id="FakeDatabaseId")

        self.init_topics("restart" + unique_suffix)

        sql = Rf'''
            INSERT INTO {connection_name}.`{self.output_topic}`
            SELECT Data FROM {connection_name}.`{self.input_topic}`;'''

        query_id = client_many_retries.create_query("restart-query", sql, type=query_type).result.query_id
        client_many_retries.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        if streaming_query:
            kikimr_many_retries.compute_plane.wait_zero_checkpoint(query_id)
        else:
            # this ugly delay is needed to wait for ca source init until we pass correct read disposition param
            time.sleep(3)

        # Start work
        data = ["data"]
        self.write_stream(["data"])
        assert self.read_stream(len(data)) == data

        delete_stream(self.output_topic)

        def assert_has_transient_issues(text, deadline):
            describe_result = client_many_retries.describe_query(query_id).result
            logging.debug("Describe result: {}".format(describe_result))
            for issue in describe_result.query.transient_issue:
                if text in issue.message:
                    return True
            assert (
                deadline and deadline > time.time()
            ), f'Text "{text}" is expected to be found in transient issues, but was not found. Describe query result:\n{describe_result}'
            return False

        deadline = time.time() + yatest_common.plain_or_under_sanitizer(60, 300)
        while not assert_has_transient_issues("SCHEME_ERROR", deadline):
            time.sleep(0.3)

        describe_result = client_many_retries.describe_query(query_id).result
        assert describe_result.query.meta.status == fq.QueryMeta.RUNNING
