#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import pytest
import time

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestBadSyntax(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize(
        "query_type",
        [fq.QueryContent.QueryType.ANALYTICS, fq.QueryContent.QueryType.STREAMING],
        ids=["analytics", "streaming"],
    )
    @pytest.mark.parametrize("after_modify", [True, False], ids=["modify", "create"])
    @pytest.mark.parametrize(
        "with_read_rules", [True, False], ids=["with_created_read_rules", "without_created_read_rules"]
    )
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_bad_syntax(self, kikimr, client, query_type, after_modify, with_read_rules):
        if with_read_rules and not after_modify:
            return

        correct_sql = "SELECT 42;"
        incorrect_sql = "blablabla"
        if with_read_rules:
            self.init_topics("bad_syntax_{}".format(query_type))
            connection_name = "yds_{}".format(query_type)
            sql_with_rr = "INSERT INTO {connection_name}.`{output_topic}` SELECT * FROM {connection_name}.`{input_topic}` LIMIT 1;".format(
                connection_name=connection_name, output_topic=self.output_topic, input_topic=self.input_topic
            )

        if after_modify:
            if with_read_rules:
                client.create_yds_connection(name=connection_name, database_id="FakeDatabaseId")
                create_response = client.create_query("q", sql_with_rr, type=query_type)
            else:
                create_response = client.create_query("q", correct_sql, type=query_type)
        else:
            create_response = client.create_query("q", incorrect_sql, type=query_type)

        query_id = create_response.result.query_id
        assert not create_response.issues, str(create_response.issues)
        logging.debug(str(create_response.result))

        if after_modify:
            if with_read_rules:
                client.wait_query(query_id, statuses=[fq.QueryMeta.RUNNING])
                if query_type == fq.QueryContent.QueryType.STREAMING:
                    kikimr.compute_plane.wait_zero_checkpoint(query_id)
                else:
                    time.sleep(3)  # TODO: remove it after streaming disposition will be supported
                self.write_stream(["A"])
            client.wait_query(query_id, statuses=[fq.QueryMeta.COMPLETED])
            client.modify_query(query_id, "q", incorrect_sql, type=query_type)
            client.wait_query(query_id, statuses=[fq.QueryMeta.FAILED])
        else:
            client.wait_query(query_id, statuses=[fq.QueryMeta.FAILED])

        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "Internal Error" not in describe_string
        assert "Failed to parse query" in describe_string

    @yq_v1
    def test_require_as(self, client):
        bad_sql = "SELECT 42 a"
        query_id = client.create_query("bad", bad_sql).result.query_id

        client.wait_query(query_id, statuses=[fq.QueryMeta.FAILED])

        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "Expecting mandatory AS here" in describe_string

    @yq_v1
    def test_type_as_column(self, client):
        bad_sql = "select max(DateTime) from AS_TABLE([<|x:1|>]);"
        query_id = client.create_query("bad", bad_sql).result.query_id

        client.wait_query(query_id, statuses=[fq.QueryMeta.FAILED])

        describe_result = client.describe_query(query_id).result
        logging.debug("Describe result: {}".format(describe_result))
        describe_string = "{}".format(describe_result)
        assert "Member not found: DateTime" in describe_string
