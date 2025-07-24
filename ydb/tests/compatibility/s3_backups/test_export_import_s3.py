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

from ydb.export import ExportClient
from ydb.export import ExportToS3Settings
from ydb.import_client import ImportClient, ImportFromS3Settings


class TestExportImportS3(MixedClusterFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        output_path = yatest.common.test_output_path()
        self.output_f = open(os.path.join(output_path, "out.log"), "w")
        self.prefix = "tables"
        self.prefix_topics = "topics"
        self.s3_config = self.setup_s3()
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config
        self.settings = (
            ExportToS3Settings()
            .with_endpoint(s3_endpoint)
            .with_access_key(s3_access_key)
            .with_secret_key(s3_secret_key)
            .with_bucket(s3_bucket)
        )

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_export_auto_dropping": True
            }
        )

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

    def _create_items(self):
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                for num in range(1, 6):
                    # Tables
                    table_name = f"/Root/{self.prefix}/sample_table_{num}"
                    session.execute_scheme(
                        f"create table `{table_name}` (id Uint64, payload Utf8, PRIMARY KEY(id));"
                    )

                    query = f"""INSERT INTO `{table_name}` (id, payload) VALUES
                        (1, 'Payload 1 for table {num}'),
                        (2, 'Payload 2 for table {num}'),
                        (3, 'Payload 3 for table {num}'),
                        (4, 'Payload 4 for table {num}'),
                        (5, 'Payload 5 for table {num}');"""
                    session.transaction().execute(
                        query, commit_tx=True
                    )
                    self.settings = self.settings.with_source_and_destination(table_name, table_name)

                    # Topics
                    topic_name = f"/Root/{self.prefix_topics}/sample_topic_{num}"
                    session.execute_scheme(
                        f"CREATE TOPIC `{topic_name}` ("
                        f"CONSUMER consumerA_{num}, "
                        f"CONSUMER consumerB_{num}"
                        f");"
                    )

    def _export_check(self):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config
        self.client = ExportClient(self.driver)
        result_export = self.client.export_to_s3(self.settings)

        export_id = result_export.id
        progress_export = result_export.progress.name

        assert progress_export in ["PREPARING", "DONE"]

        while progress_export != "DONE":
            progress_export = self.client.get_export_to_s3_operation(export_id).progress.name

        s3_resource = boto3.resource("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

        keys_expected = set()
        for num in range(1, 6):
            # Tables
            table_name = f"Root/{self.prefix}/sample_table_{num}"
            keys_expected.add(table_name + "/data_00.csv")
            keys_expected.add(table_name + "/metadata.json")
            keys_expected.add(table_name + "/scheme.pb")

            # Topics
            # Topics are not necessarily exported, because in early versions, the export/import of topics is not supported.
            # The test only checks that the export is successful for tables

        bucket = s3_resource.Bucket(s3_bucket)
        keys = set()
        for x in list(bucket.objects.all()):
            keys.add(x.key)

        assert keys_expected <= keys

    def _import_check(self):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config
        imported_prefix = "imported"

        import_settings = (
            ImportFromS3Settings()
            .with_endpoint(s3_endpoint)
            .with_access_key(s3_access_key)
            .with_secret_key(s3_secret_key)
            .with_bucket(s3_bucket)
        )
        for num in range(1, 6):
            table_name = f"Root/{self.prefix}/sample_table_{num}"
            imported_table_name = f"/Root/{imported_prefix}/sample_table_{num}"
            import_settings = import_settings.with_source_and_destination(table_name, imported_table_name)

        import_client = ImportClient(self.driver)
        result_import = import_client.import_from_s3(import_settings)
        import_id = result_import.id
        progress_import = result_import.progress.name
        assert progress_import in ["PREPARING", "DONE"]
        while progress_import != "DONE":
            progress_import = import_client.get_import_from_s3_operation(import_id).progress.name
        assert progress_import == "DONE"

        # Checking that the imported tables are actually created
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                for num in range(1, 6):
                    imported_table_name = f"/Root/{imported_prefix}/sample_table_{num}"
                    desc = session.describe_table(imported_table_name)
                    assert desc is not None, f"Table {imported_table_name} not found after import"

    def test_full_pipeline(self):
        self._create_items()
        self._export_check()
        self._import_check()
