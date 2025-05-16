# -*- coding: utf-8 -*-
import boto3
import tempfile
import pytest
import yatest
import os
import json
import sys
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture


class TestExportS3(MixedClusterFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        output_path = yatest.common.test_output_path()
        self.output_f = open(os.path.join(output_path, "out.log"), "w")
        self.s3_config = self.setup_s3()
        yield from self.setup_cluster()

    @staticmethod
    def setup_s3():
        s3_endpoint = os.getenv("S3_ENDPOINT")
        s3_access_key = "minio"
        s3_secret_key = "minio123"
        s3_bucket = "export_test_bucket"

        resource = boto3.resource("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

        bucket = resource.Bucket(s3_bucket)
        bucket.create()
        bucket.objects.all().delete()

        return s3_endpoint, s3_access_key, s3_secret_key, s3_bucket

    def _execute_command_and_get_result(self, command):
        with tempfile.NamedTemporaryFile(mode='w+', delete=True) as temp_file:
            yatest.common.execute(command, wait=True, stdout=temp_file, stderr=sys.stderr)
            temp_file.flush()
            temp_file.seek(0)
            content = temp_file.read()
            self.output_f.write(content + "\n")
            result = json.loads(content)
            return result

    def test_export(self):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config

        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                for table_num in range(1, 6):
                    table_name = f"sample_table_{table_num}"
                    session.execute_scheme(
                        f"create table `{table_name}` (id Uint64, payload Utf8, PRIMARY KEY(id));"
                    )

                    query = f"""INSERT INTO `{table_name}` (id, payload) VALUES
                        (1, 'Payload 1 for table {table_num}'),
                        (2, 'Payload 2 for table {table_num}'),
                        (3, 'Payload 3 for table {table_num}'),
                        (4, 'Payload 4 for table {table_num}'),
                        (5, 'Payload 5 for table {table_num}');"""
                    session.transaction().execute(
                        query, commit_tx=True
                    )

        export_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "export",
            "s3",
            "--s3-endpoint",
            s3_endpoint,
            "--bucket",
            s3_bucket,
            "--access-key",
            s3_access_key,
            "--secret-key",
            s3_secret_key,
            "--item",
            "src=/Root,dst=.",
            "--format",
            "proto-json-base64"
        ]

        result_export = self._execute_command_and_get_result(export_command)

        export_id = result_export["id"]
        status_export = result_export["status"]
        progress_export = result_export["metadata"]["progress"]

        assert status_export == "SUCCESS"
        assert progress_export in ["PROGRESS_PREPARING", "PROGRESS_DONE"]

        operation_get_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "operation",
            "get",
            "%s" % export_id,
            "--format",
            "proto-json-base64"
        ]

        while progress_export != "PROGRESS_DONE":
            result_get = self._execute_command_and_get_result(operation_get_command)
            progress_export = result_get["metadata"]["progress"]

        s3_resource = boto3.resource("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

        keys_expected = set()
        for table_num in range(1, 6):
            table_name = f"sample_table_{table_num}"
            keys_expected.add(table_name + "/data_00.csv")
            keys_expected.add(table_name + "/metadata.json")
            keys_expected.add(table_name + "/scheme.pb")

        bucket = s3_resource.Bucket(s3_bucket)
        keys = set()
        for x in list(bucket.objects.all()):
            keys.add(x.key)

        assert keys_expected <= keys
