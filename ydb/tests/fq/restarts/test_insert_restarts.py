#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import time

import ydb

from ydb.public.api.grpc import ydb_operation_v1_pb2_grpc, ydb_query_v1_pb2_grpc
from ydb.public.api.protos import ydb_operation_pb2, ydb_query_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.fq.streaming_common.common import StreamingTestBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer

logger = logging.getLogger(__name__)


class TestS3(StreamingTestBase):
    def get_issues_depth(self, issues):
        deepest_path_size = 0
        for issue in issues:
            deepest_path_size = max(self.get_issues_depth(issue.issues), deepest_path_size)
        return deepest_path_size + 1

    def run_atomic_upload_check_query(self, client, bucket, connection, prefix, format):
        if not list(bucket.objects.filter(Prefix=prefix)):
            return 0

        sql = f'''
            SELECT COUNT(idx) AS row_count FROM `{connection}`.`{prefix}` WITH (
                FORMAT = {format}, SCHEMA (idx Int NOT NULL)
            );
        '''
        return client.query(sql)[0].rows[0]['row_count']

    def get_script_operation(self, client, operation_id):
        response = client.driver(
            ydb_operation_pb2.GetOperationRequest(id=operation_id),
            ydb_operation_v1_pb2_grpc.OperationServiceStub,
            'GetOperation',
            settings=ydb.BaseRequestSettings().with_timeout(30),
        )
        assert response.operation.id == operation_id, response.operation
        return response.operation

    def test_atomic_upload_commit(self, kikimr, s3, client):
        resource = boto3.resource(
            's3',
            endpoint_url=s3.s3_url,
            aws_access_key_id='key',
            aws_secret_access_key='secret_key',
            region_name='us-east-1',
        )
        bucket = resource.Bucket('insert_bucket')
        bucket.create(ACL='public-read-write')
        bucket.objects.all().delete()

        number_rows = 10000000
        body = 'idx\n' + '0\n' * number_rows
        bucket.put_object(Key='select/test.csv', Body=body, ContentType='text/plain')
        logger.info('Uploaded %s input rows into S3', number_rows)

        client.query(f'''
            CREATE EXTERNAL DATA SOURCE ibucket WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "{s3.s3_url}/insert_bucket/",
                AUTH_METHOD = "NONE"
            );
        ''')
        sql = '''
            PRAGMA s3.AtomicUploadCommit = "true";

            INSERT INTO ibucket.`insert/` WITH (FORMAT = csv_with_names)
            SELECT * FROM ibucket.`select/` WITH (
                FORMAT = csv_with_names, SCHEMA (idx Int NOT NULL)
            );
        '''
        # ExecuteScript persists S3 effects so uploads can be finalized after
        # losing the execution node. Submit once: retrying INSERT could duplicate data.
        timeout = plain_or_under_sanitizer(250, 250 * 5)
        request = ydb_query_pb2.ExecuteScriptRequest(
            exec_mode=ydb_query_pb2.EXEC_MODE_EXECUTE,
            script_content=ydb_query_pb2.QueryContent(text=sql, syntax=ydb_query_pb2.SYNTAX_YQL_V1),
        )
        request.operation_params.operation_mode = ydb_operation_pb2.OperationParams.ASYNC
        request.operation_params.operation_timeout.FromSeconds(timeout)
        operation = client.driver(
            request,
            ydb_query_v1_pb2_grpc.QueryServiceStub,
            'ExecuteScript',
            settings=ydb.BaseRequestSettings().with_timeout(30),
        )
        assert operation.id, operation
        assert operation.status == StatusIds.SUCCESS, operation
        operation_id = operation.id

        deadline = time.monotonic() + timeout
        restarted = False
        while True:
            current_number_rows = self.run_atomic_upload_check_query(
                client, bucket, 'ibucket', 'insert/', 'csv_with_names'
            )
            assert current_number_rows in (0, number_rows), f'Unexpected incomplete result: {current_number_rows}'
            operation = self.get_script_operation(client, operation_id)
            if operation.ready:
                break

            uploads = list(bucket.multipart_uploads.all())
            if uploads and not restarted:
                # Lose the script runner first so the failure exercises lease
                # expiration and persistent S3 cleanup, rather than a graceful abort.
                runner_nodes = [
                    node_id
                    for node_id in kikimr.cluster.slots
                    if self.get_actor_count(kikimr, node_id, 'KQP_RUN_SCRIPT_ACTOR') > 0
                ]
                if not runner_nodes:
                    operation = self.get_script_operation(client, operation_id)
                    if operation.ready:
                        break
                assert len(runner_nodes) == 1, (runner_nodes, operation)
                node_ids = runner_nodes + [node_id for node_id in kikimr.cluster.slots if node_id not in runner_nodes]
                for node_id in node_ids:
                    node = kikimr.cluster.slots[node_id]
                    logger.info('Restarting compute node %s', node_id)
                    node.set_log_file_prefix('logfile_restarted_')
                    node.kill()  # KiKiMRNode.kill() also starts the node again.
                kikimr.wait_kqp_node_count(len(kikimr.cluster.slots))
                kikimr.recreate_driver()
                client = kikimr.ydb_client
                restarted = True

            assert time.monotonic() < deadline, f'Script did not finish within {timeout}s: {operation}'
            time.sleep(0.01)

        final_number_rows = self.run_atomic_upload_check_query(client, bucket, 'ibucket', 'insert/', 'csv_with_names')
        metadata = ydb_query_pb2.ExecuteScriptMetadata()
        assert operation.metadata.Unpack(metadata), operation
        if metadata.exec_status == ydb_query_pb2.EXEC_STATUS_COMPLETED:
            assert operation.status == StatusIds.SUCCESS, operation
            assert final_number_rows == number_rows, f'Invalid result for COMPLETED: {final_number_rows}'
        else:
            assert restarted, operation
            assert metadata.exec_status in (
                ydb_query_pb2.EXEC_STATUS_ABORTED,
                ydb_query_pb2.EXEC_STATUS_FAILED,
            ), operation
            assert operation.status != StatusIds.SUCCESS, operation
            assert self.get_issues_depth(operation.issues) <= 3, operation.issues
            assert 'Lease expired' in str(operation.issues), operation.issues
            if metadata.exec_status == ydb_query_pb2.EXEC_STATUS_ABORTED:
                assert final_number_rows == 0, f'Incomplete result for ABORTED: {final_number_rows}'
            else:
                assert final_number_rows in (0, number_rows), f'Incomplete result for FAILED: {final_number_rows}'

        assert not list(bucket.multipart_uploads.all()), 'Unexpected uncommitted upload in bucket'
