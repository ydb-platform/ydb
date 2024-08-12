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
