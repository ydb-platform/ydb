#!/usr/bin/env python
# -*- coding: utf-8 -*-
import boto3
import logging
import pytest

from ydb.tests.tools.fq_runner.kikimr_utils import yq_all

import ydb.public.api.protos.draft.fq_pb2 as fq


class TestPublicMetrics:
    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_public_metrics(self, kikimr, s3, client, yq_version, unique_prefix):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("fbucket")
        bucket.create(ACL='public-read')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        fruits = R'''Fruit,Price,Weight
Banana,3,100
Apple,2,22
Pear,15,33'''
        s3_client.put_object(Body=fruits, Bucket='fbucket', Key='fruits.csv', ContentType='text/plain')
        kikimr.control_plane.wait_bootstrap(1)
        storage_connection_name = unique_prefix + "fruitbucket"
        client.create_storage_connection(storage_connection_name, "fbucket")

        cloud_id = "mock_cloud"
        folder_id = "my_folder"

        sql = fR'''
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        metrics = (
            kikimr.compute_plane.get_sensors(1, "yq_public")
            if yq_version == "v1"
            else kikimr.control_plane.get_sensors(1, "yq_public")
        )
        logging.debug(str(metrics))

        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.running_tasks"}
            )
            >= 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.cpu_usage_us"}
            )
            >= 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.memory_usage_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.input_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.uptime_seconds"}
            )
            >= 0
        )
        assert (
            metrics.find_sensor(
                {
                    "cloud_id": cloud_id,
                    "folder_id": folder_id,
                    "query_id": query_id,
                    "name": "query.source_input_records",
                }
            )
            == 3
        )
        if yq_version == "v1":  # v1 reports nothing, v2 report 0
            assert (
                metrics.find_sensor(
                    {
                        "cloud_id": cloud_id,
                        "folder_id": folder_id,
                        "query_id": query_id,
                        "name": "query.sink_output_records",
                    }
                )
                is None
            )
            assert (
                metrics.find_sensor(
                    {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.output_bytes"}
                )
                is None
            )
        else:
            assert (
                metrics.find_sensor(
                    {
                        "cloud_id": cloud_id,
                        "folder_id": folder_id,
                        "query_id": query_id,
                        "name": "query.sink_output_records",
                    }
                )
                == 0
            )
            assert (
                metrics.find_sensor(
                    {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.output_bytes"}
                )
                == 0
            )

        sql = fR'''
            INSERT INTO `{storage_connection_name}`.`/copy/` WITH (format=parquet)
            SELECT *
            FROM `{storage_connection_name}`.`fruits.csv`
            WITH (format=csv_with_names, SCHEMA (
                Fruit String NOT NULL,
                Price Int NOT NULL,
                Weight Int NOT NULL
            ));
            '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        metrics = (
            kikimr.compute_plane.get_sensors(1, "yq_public")
            if yq_version == "v1"
            else kikimr.control_plane.get_sensors(1, "yq_public")
        )
        logging.debug(str(metrics))

        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.running_tasks"}
            )
            >= 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.cpu_usage_us"}
            )
            >= 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.memory_usage_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.input_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.uptime_seconds"}
            )
            >= 0
        )
        assert (
            metrics.find_sensor(
                {"cloud_id": cloud_id, "folder_id": folder_id, "query_id": query_id, "name": "query.output_bytes"}
            )
            > 0
        )
        assert (
            metrics.find_sensor(
                {
                    "cloud_id": cloud_id,
                    "folder_id": folder_id,
                    "query_id": query_id,
                    "name": "query.source_input_records",
                }
            )
            == 3
        )
        assert (
            metrics.find_sensor(
                {
                    "cloud_id": cloud_id,
                    "folder_id": folder_id,
                    "query_id": query_id,
                    "name": "query.sink_output_records",
                }
            )
            > 0
        )
