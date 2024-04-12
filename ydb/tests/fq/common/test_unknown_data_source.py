#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestUnknownDataSource:
    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_should_fail_unknown_data_source(self, kikimr, client, yq_version):
        kikimr.control_plane.wait_bootstrap(1)
        sql = R'''
            $h = "hahn";

            SELECT
                *
            FROM kikimr:$h.`home/yql/tutorial/users`
            LIMIT 100;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        issues = str(client.describe_query(query_id).result.query.issue)

        if yq_version == 'v1':
            assert "Unknown DataSource: kikimr" in issues, "Incorrect Issues: " + issues
        else:
            assert "Cannot find table" in issues, "Incorrect Issues: " + issues
