#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3

import pytest
import time

import ydb.public.api.protos.draft.fq_pb2 as fq

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.tests.tools.fq_runner.kikimr_utils import yq_all


class TestS3(object):
    def get_issues_depth(self, issues):
        deepest_path_size = 0
        for issue in issues:
            deepest_path_size = max(self.get_issues_depth(issue.issues), deepest_path_size)
        return deepest_path_size + 1

    def run_atomic_upload_check_query(self, client, bucket, connection, prefix, format):
        if len(list(bucket.objects.filter(Prefix=prefix))) == 0:
            return 0

        sql = R'''
            select COUNT(idx) from {0}.`{1}` with (format={2}, schema(
                idx Int NOT NULL
            ))
        '''.format(
            connection, prefix, format
        )

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        return data.result.result_set.rows[0].items[0].uint64_value

    @yq_all
    @pytest.mark.parametrize("client", [{"folder_id": "my_folder"}], indirect=True)
    def test_atomic_upload_commit(self, kikimr, s3, client):
        resource = boto3.resource(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        # Creating select content
        bucket = resource.Bucket("insert_bucket")
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        s3_client = boto3.client(
            "s3", endpoint_url=s3.s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key"
        )

        number_rows = 10000000
        body = "idx\n" + "0\n" * number_rows
        s3_client.put_object(Body=body, Bucket="insert_bucket", Key="select/test.csv", ContentType="text/plain")

        # Creating insert query
        client.create_storage_connection("ibucket", "insert_bucket")

        sql = R'''
            pragma s3.AtomicUploadCommit = "true";

            insert into ibucket.`insert/` with (format=csv_with_names)
            select * from ibucket.`select/` with (format=csv_with_names, schema(
                idx Int NOT NULL
            ));
        '''
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id

        # Checking insert query
        timeout = yatest_common.plain_or_under_sanitizer(250, 250 * 5)
        start = time.time()
        deadline = start + timeout
        while True:
            current_number_rows = self.run_atomic_upload_check_query(
                client, bucket, "ibucket", "insert/", "csv_with_names"
            )
            if current_number_rows == number_rows:
                break

            assert current_number_rows == 0, "Unexpected incomplete result in bucket"

            number_uploads = len(list(bucket.multipart_uploads.all()))
            if number_uploads > 0:
                kikimr.compute_plane.kikimr_cluster.restart_nodes()
                break

            assert time.time() < deadline, f"Insert not finished for already {time.time() - start} seconds"
            time.sleep(0.001)

        kikimr.compute_plane.wait_bootstrap()
        client.wait_query(
            query_id,
            statuses=[fq.QueryMeta.COMPLETED, fq.QueryMeta.ABORTED_BY_SYSTEM, fq.QueryMeta.FAILED],
            timeout=timeout,
        )

        final_number_rows = self.run_atomic_upload_check_query(client, bucket, "ibucket", "insert/", "csv_with_names")
        query = client.describe_query(query_id).result.query
        final_status = query.meta.status

        if final_status == fq.QueryMeta.COMPLETED:
            assert final_number_rows == number_rows, "Invalid result in bucket for COMPLETED final status"
        elif final_status == fq.QueryMeta.ABORTED_BY_SYSTEM:
            assert self.get_issues_depth(query.issue) <= 3, str(query.issue)
            assert self.get_issues_depth(query.transient_issue) <= 3, str(query.transient_issue)
            assert "Lease expired" in str(query.issue), str(query.issue)
            assert final_number_rows == 0, "Incomplete final result in bucket"
        else:
            assert self.get_issues_depth(query.issue) <= 3, str(query.issue)
            assert self.get_issues_depth(query.transient_issue) <= 3, str(query.transient_issue)
            assert "Lease expired" in str(query.issue), str(query.issue)
            assert final_number_rows == 0 or final_number_rows == number_rows

        assert len(list(bucket.multipart_uploads.all())) == 0, "Unexpected uncommited upload in bucket"
