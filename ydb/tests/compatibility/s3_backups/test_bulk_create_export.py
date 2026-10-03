import os
import time

import boto3
import pytest

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    current_binary_path,
    current_name,
)
from ydb.export import ExportClient, ExportToS3Settings
from ydb.import_client import ImportClient, ImportFromS3Settings


class TestBulkCreateExportCompatibility(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        # Only HEAD is known to support the new flag. Do not feed unknown YAML
        # fields to a stable binary, including a stable binary used as "target".
        if current_name != "current" or current_binary_path not in self.all_binary_paths:
            pytest.skip("Requires HEAD and a stable binary")
        if len(set(self.all_binary_paths)) < 2 or min(self.versions) < (25, 1):
            pytest.skip("Requires two different versions with S3 export support")

        self.s3_endpoint = os.environ["S3_ENDPOINT"]
        self.bucket = "hive-bulk-create-compat"
        resource = boto3.resource(
            "s3", endpoint_url=self.s3_endpoint,
            aws_access_key_id="minio", aws_secret_access_key="minio123",
            region_name="us-east-1",
        )
        bucket = resource.Bucket(self.bucket)
        bucket.create()
        bucket.objects.all().delete()

        flags = ["enable_export_auto_dropping"]
        if self.all_binary_paths[0] == current_binary_path:
            flags.append("enable_hive_bulk_create")
        yield from self.setup_cluster(extra_feature_flags=flags)

    def change_cluster_version(self):
        next_index = (self.current_binary_paths_index + 1) % len(self.all_binary_paths)
        flags = self.config.yaml_config["feature_flags"]
        if self.all_binary_paths[next_index] == current_binary_path:
            flags["enable_hive_bulk_create"] = True
        else:
            # Call only after export/import and temporary table cleanup finish.
            flags.pop("enable_hive_bulk_create", None)
        super().change_cluster_version()

    @staticmethod
    def _wait_done(operation, get_operation):
        deadline = time.monotonic() + 300
        while operation.progress.name != "DONE":
            assert operation.progress.name not in ("CANCELLED", "CANCELLATION"), operation
            assert time.monotonic() < deadline, operation
            time.sleep(0.2)
            operation = get_operation(operation.id)

    def _execute(self, query):
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                return session.transaction().execute(query, commit_tx=True)

    def _export(self, source, prefix):
        settings = (
            ExportToS3Settings()
            .with_endpoint(self.s3_endpoint)
            .with_access_key("minio")
            .with_secret_key("minio123")
            .with_bucket(self.bucket)
            .with_source_and_destination(source, prefix)
        )
        client = ExportClient(self.driver)
        self._wait_done(client.export_to_s3(settings), client.get_export_to_s3_operation)

    def _import(self, prefix, destination):
        settings = (
            ImportFromS3Settings()
            .with_endpoint(self.s3_endpoint)
            .with_access_key("minio")
            .with_secret_key("minio123")
            .with_bucket(self.bucket)
            .with_source_and_destination(prefix, destination)
        )
        client = ImportClient(self.driver)
        self._wait_done(client.import_from_s3(settings), client.get_import_from_s3_operation)

    def _check_rows(self, table, expected):
        result = self._execute(f"SELECT key, value FROM `{table}` ORDER BY key;")
        assert [(row.key, row.value) for row in result[0].rows] == expected

    def test_export_import_across_upgrade_and_drained_downgrade(self):
        # Populate every partition, not just the first range of a Uint64 key.
        expected = [(i * (2**64 // 16), f"payload-{i}") for i in range(16)]
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme("""
                    CREATE TABLE `/Root/source` (
                        key Uint64, value Utf8, PRIMARY KEY (key)
                    ) WITH (UNIFORM_PARTITIONS = 4, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
                """)
        values = ", ".join(f"({key}ul, '{value}')" for key, value in expected)
        self._execute(f"UPSERT INTO `/Root/source` (key, value) VALUES {values};")
        self._check_rows("/Root/source", expected)
        self._export("/Root/source", "first-version")

        self.change_cluster_version()
        self._import("first-version", "/Root/restored_first")
        self._check_rows("/Root/restored_first", expected)
        self._export("/Root/restored_first", "second-version")

        self.change_cluster_version()
        self._import("second-version", "/Root/restored_second")
        self._check_rows("/Root/restored_second", expected)
        self._check_rows("/Root/source", expected)
