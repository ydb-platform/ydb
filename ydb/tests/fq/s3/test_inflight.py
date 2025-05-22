#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import time

import pytest

import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.public.api.protos.draft.fq_pb2 as fq
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestS3:
    @yq_v1
    @pytest.mark.parametrize("kikimr_params", [{"inflight": 1}, {"inflight": 2}, {"inflight": 5}], indirect=True)
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_inflight(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("bbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        for i in range(100, 1000):
            s3_client.put_object(Body=fruits, Bucket='bbucket', Key='fruits{}.csv'.format(i), ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "big_bucket"
        client.create_storage_connection(storage_connection_name, "bbucket")

        sql = fR'''PRAGMA dq.MaxTasksPerStage="1";
            SELECT count(*) as cnt
            FROM `{storage_connection_name}`.`*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id

        start = time.time()
        deadline = start + 60
        while True:
            inflight = kikimr.compute_plane.get_sensors(1, "yq").find_sensor(
                {"subcomponent": "http_gateway", "sensor": "InFlightStreams"}
            )
            if inflight is None:
                inflight = 0
            assert inflight <= kikimr.inflight, "Inflight {} must not exceed limit of {}".format(
                inflight, kikimr.inflight
            )
            status = client.describe_query(query_id).result.query.meta.status
            if status == fq.QueryMeta.COMPLETED:
                break
            assert time.time() < deadline, "Query {} is not completed for {}s. Query status: {}.".format(
                query_id, time.time() - start, fq.QueryMeta.ComputeStatus.Name(status)
            )

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].uint64_value == (1000 - 100) * 3

        sql = fR'''PRAGMA dq.MaxTasksPerStage="1";
            SELECT count(*) as cnt
            FROM `{storage_connection_name}`.`*`
            WITH (format=raw, SCHEMA (
                Data String NOT NULL
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id

        start = time.time()
        deadline = start + 60
        while True:
            inflight = kikimr.compute_plane.get_sensors(1, "yq").find_sensor(
                {"subcomponent": "http_gateway", "sensor": "InFlight"}
            )
            if inflight is None:
                inflight = 0
            assert inflight <= kikimr.inflight, "Inflight {} must not exceed limit of {}".format(
                inflight, kikimr.inflight
            )
            status = client.describe_query(query_id).result.query.meta.status
            if status == fq.QueryMeta.COMPLETED:
                break
            assert time.time() < deadline, "Query {} is not completed for {}s. Query status: {}.".format(
                query_id, time.time() - start, fq.QueryMeta.ComputeStatus.Name(status)
            )

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].uint64_value == (1000 - 100)

    @yq_v1
    @pytest.mark.parametrize("kikimr_params", [{"inflight": 1, "data_inflight": 1}], indirect=True)
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_data_inflight(self, kikimr, s3, client, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("sbucket")
        bucket.create(ACL='public-read')

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        for i in range(10, 20):
            s3_client.put_object(Body=fruits, Bucket='sbucket', Key='fruits{}.csv'.format(i), ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "slow_bucket"
        client.create_storage_connection(storage_connection_name, "sbucket")

        sql = fR'''PRAGMA dq.MaxTasksPerStage="1";
            SELECT count(*) as cnt
            FROM `{storage_connection_name}`.`*`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id

        start = time.time()
        deadline = start + 60
        while True:
            inflight = kikimr.compute_plane.get_sensors(1, "yq").find_sensor(
                {"subcomponent": "http_gateway", "sensor": "InFlightStreams"}
            )
            if inflight is None:
                inflight = 0
            assert inflight <= 1, "Inflight {} must not exceed 1 with mem back pressure".format(inflight)
            status = client.describe_query(query_id).result.query.meta.status
            if status == fq.QueryMeta.COMPLETED:
                break
            assert time.time() < deadline, "Query {} is not completed for {}s. Query status: {}.".format(
                query_id, time.time() - start, fq.QueryMeta.ComputeStatus.Name(status)
            )

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "cnt"
        assert result_set.columns[0].type.type_id == ydb.Type.UINT64
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].uint64_value == (20 - 10) * 3
