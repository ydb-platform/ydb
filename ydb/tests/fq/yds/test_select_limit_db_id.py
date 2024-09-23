#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestSelectLimitWithDbId(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_select_same_with_id(self, kikimr, client):
        pytest.skip("Skip until streaming disposition is implemented YQ-589")

        self.init_topics("select_same_with_id", create_output=False)

        sql = R'''
            SELECT * FROM yds1.`{input_topic}` LIMIT 2;
            SELECT * FROM yds1.`{input_topic}` LIMIT 2;
            '''.format(
            input_topic=self.input_topic,
        )

        client.create_yds_connection(name="yds1", database_id="FakeDatabaseId")
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
        kikimr.wait_zero_checkpoint(query_id)

        messages = ["A", "B", "C", "D", "E"]
        self.write_stream(messages)

        client.wait_query(query_id, 30)

        # by default selects use different consumers, so they will read the same
        # messages - first 2 of them

        rs = client.get_result_data(query_id, result_set_index=0).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 2
        assert len(rs.columns) == 1
        assert rs.rows[0].items[0].bytes_value == messages[0]
        assert rs.rows[1].items[0].bytes_value == messages[1]

        rs = client.get_result_data(query_id, result_set_index=1).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 2
        assert len(rs.columns) == 1
        assert rs.rows[0].items[0].bytes_value == messages[0]
        assert rs.rows[1].items[0].bytes_value == messages[1]
