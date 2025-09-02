#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import os
import pytest
import time

import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.fq_client import CONTROL_PLANE_REQUEST_TIMEOUT, FederatedQueryClient, StreamingDisposition
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase


class TestDisposition(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_disposition_oldest(self, kikimr: StreamingOverKikimr, client: FederatedQueryClient):
        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")
        self.init_topics("disposition_oldest")

        query_name = "disposition-oldest-query"
        sql = Rf'''
            PRAGMA dq.MaxTasksPerStage="1";

            INSERT INTO yds.`{self.output_topic}`
            SELECT Data FROM yds.`{self.input_topic}`;
        '''

        def run(input: list[str], output: list[str], query_id: str | None) -> str:
            self.write_stream(input)

            if query_id is None:
                result: fq.CreateQueryResult = client.create_query(
                    name=query_name,
                    text=sql,
                    type=fq.QueryContent.QueryType.STREAMING,
                    streaming_disposition=StreamingDisposition.oldest(),
                ).result
                query_id = result.query_id
            else:
                client.modify_query(
                    query_id=query_id,
                    name=query_name,
                    text=sql,
                    type=fq.QueryContent.QueryType.STREAMING,
                    streaming_disposition=StreamingDisposition.oldest(),
                )
            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

            time.sleep(5)

            assert self.read_stream(len(output)) == output

            client.abort_query(query_id)
            client.wait_query(query_id)

            return query_id

        query_id = run(["1", "2"], ["1", "2"], None)
        query_id = run(["3", "4"], ["1", "2", "3", "4"], query_id)
        query_id = run(["5", "6"], ["1", "2", "3", "4", "5", "6"], query_id)

    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_disposition_fresh(self, kikimr: StreamingOverKikimr, client: FederatedQueryClient):
        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")
        self.init_topics("disposition_fresh")

        query_name = "disposition-fresh-query"
        sql = Rf'''
            PRAGMA dq.MaxTasksPerStage="1";

            INSERT INTO yds.`{self.output_topic}`
            SELECT Data FROM yds.`{self.input_topic}`;
        '''

        def run(input: list[str], output: list[str], query_id: str | None) -> str:
            self.write_stream(input)

            if query_id is None:
                result: fq.CreateQueryResult = client.create_query(
                    name=query_name,
                    text=sql,
                    type=fq.QueryContent.QueryType.STREAMING,
                    streaming_disposition=StreamingDisposition.fresh(),
                ).result
                query_id = result.query_id
            else:
                client.modify_query(
                    query_id=query_id,
                    name=query_name,
                    text=sql,
                    type=fq.QueryContent.QueryType.STREAMING,
                    streaming_disposition=StreamingDisposition.fresh(),
                )
            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

            time.sleep(5)

            assert self.read_stream(len(output)) == output

            client.abort_query(query_id)
            client.wait_query(query_id)

            return query_id

        query_id = run(["1", "2"], [], None)
        query_id = run(["3", "4"], [], query_id)
        query_id = run(["5", "6"], [], query_id)

    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_disposition_from_time(self, kikimr: StreamingOverKikimr, client: FederatedQueryClient):
        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")
        self.init_topics("disposition_from_time")

        query_name = "disposition-from-time-query"
        sql = Rf'''
            PRAGMA dq.MaxTasksPerStage="1";

            INSERT INTO yds.`{self.output_topic}`
            SELECT Data FROM yds.`{self.input_topic}`;
        '''

        def run(input: list[str], output: list[str], query_id: str | None) -> str:
            from_time = datetime.datetime.now()
            self.write_stream(input)

            if query_id is None:
                result: fq.CreateQueryResult = client.create_query(
                    name=query_name,
                    text=sql,
                    type=fq.QueryContent.QueryType.STREAMING,
                    streaming_disposition=StreamingDisposition.from_time(from_time.timestamp()),
                ).result
                query_id = result.query_id
            else:
                client.modify_query(
                    query_id=query_id,
                    name=query_name,
                    text=sql,
                    type=fq.QueryContent.QueryType.STREAMING,
                    streaming_disposition=StreamingDisposition.from_time(from_time.timestamp()),
                )
            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

            time.sleep(5)

            assert self.read_stream(len(output)) == output

            client.abort_query(query_id)
            client.wait_query(query_id)

            return query_id

        query_id = run(["1", "2"], ["1", "2"], None)
        query_id = run(["3", "4"], ["3", "4"], query_id)
        query_id = run(["5", "6"], ["5", "6"], query_id)

    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_disposition_time_ago(self, kikimr: StreamingOverKikimr, client: FederatedQueryClient):
        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")
        self.init_topics("disposition_time_ago")

        query_name = "disposition-time-ago-query"
        sql = Rf'''
            PRAGMA dq.MaxTasksPerStage="1";

            INSERT INTO yds.`{self.output_topic}`
            SELECT Data FROM yds.`{self.input_topic}`;
        '''

        def run(input: list[str], output: list[str], query_id: str | None) -> str:
            from_time = datetime.datetime.now()
            self.write_stream(input)

            if query_id is None:
                result: fq.CreateQueryResult = client.create_query(
                    name=query_name,
                    text=sql,
                    type=fq.QueryContent.QueryType.STREAMING,
                    streaming_disposition=StreamingDisposition.time_ago(CONTROL_PLANE_REQUEST_TIMEOUT),
                ).result
                query_id = result.query_id
            else:
                client.modify_query(
                    query_id=query_id,
                    name=query_name,
                    text=sql,
                    type=fq.QueryContent.QueryType.STREAMING,
                    streaming_disposition=StreamingDisposition.time_ago(CONTROL_PLANE_REQUEST_TIMEOUT),
                )
            client.wait_query_status(query_id, fq.QueryMeta.RUNNING)

            time.sleep(5)

            assert self.read_stream(len(output)) == output

            client.abort_query(query_id)
            client.wait_query(query_id)

            time.sleep(CONTROL_PLANE_REQUEST_TIMEOUT - (datetime.datetime.now() - from_time).total_seconds())

            return query_id

        query_id = run(["1", "2"], ["1", "2"], None)
        query_id = run(["3", "4"], ["3", "4"], query_id)
        query_id = run(["5", "6"], ["5", "6"], query_id)
