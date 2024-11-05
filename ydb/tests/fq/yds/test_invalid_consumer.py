#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestConsumer(TestYdsBase):
    @yq_v1
    def test_invalid(self, kikimr, client):
        self.init_topics("invalid_consumer", create_output=False)

        sql = R'''
            PRAGMA pq.Consumer="InvalidConsumerName";
            SELECT * FROM yds.`{input_topic}` LIMIT 1;
            '''.format(
            input_topic=self.input_topic,
        )

        client.create_yds_connection("yds", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT"))
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "Query failed with code BAD_REQUEST" in describe_string
        assert "no read rule provided for consumer" in describe_string
