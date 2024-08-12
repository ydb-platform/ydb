#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestConnection(TestYdsBase):
    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_test_s3_connection(self, kikimr, s3, client):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("testbucket")
        bucket.create(ACL='public-read')

        client.test_storage_connection("testbucket")

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_test_s3_connection_uri(self, kikimr, s3, client):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        bucket = resource.Bucket("testbucket")
        bucket.create(ACL='public-read')

        test_response = client.test_storage_connection(" testbucket", check_issues=False)
        assert "Object Storage: The specified bucket does not exist" in str(test_response.issues)

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_test_s3_connection_error(self, kikimr, s3, client):
        test_response = client.test_storage_connection("errorbucket", check_issues=False)
        assert "Object Storage: The specified bucket does not exist" in str(test_response.issues)
