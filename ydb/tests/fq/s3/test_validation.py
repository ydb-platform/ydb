#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

import ydb.public.api.protos.draft.fq_pb2 as fq

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryException
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestS3(object):
    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_empty(self, client):
        try:
            client.create_query("simple", "", type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        except FederatedQueryException as e:
            assert "message: \"text\\\'s length is not in [1; 102400]" in e.args[0]
            return
        assert False

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_nested_issues(self, client):
        sql = "$t_0 = SELECT 1 - \"f\" AS data;\n"

        number_rows = 100
        for i in range(number_rows):
            sql += f"$t_{i + 1} = SELECT data FROM $t_{i} WHERE data != {i};\n"

        sql += f"SELECT data FROM $t_{number_rows};"

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        describe_result = client.describe_query(query_id).result
        describe_string = "{}".format(describe_result)
        assert "(skipped levels)" in describe_string, describe_string
        assert "Cannot substract" in describe_string, describe_string

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_nested_type(self, client):
        sql = "$t_0 = SELECT 1 AS data;\n"

        number_rows = 300
        for i in range(number_rows):
            sql += f"$t_{i + 1} = SELECT <|data:data|> AS data FROM $t_{i};\n"

        sql += f"SELECT data FROM $t_{number_rows};"

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        describe_result = client.describe_query(query_id).result
        describe_string = "{}".format(describe_result)
        assert "Invalid result set" in describe_string, describe_string
        assert "Nesting depth of type for result column" in describe_string, describe_string
