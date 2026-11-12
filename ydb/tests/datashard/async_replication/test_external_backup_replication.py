import json
import os
from urllib.parse import urlparse

import boto3
import grpc
import yatest.common

try:
    from ydb.public.api.grpc import ydb_export_v1_pb2_grpc, ydb_import_v1_pb2_grpc, ydb_operation_v1_pb2_grpc
    from ydb.public.api.protos import ydb_export_pb2, ydb_import_pb2, ydb_operation_pb2, ydb_status_codes_pb2
except ImportError:
    from contrib.ydb.public.api.grpc import ydb_export_v1_pb2_grpc, ydb_import_v1_pb2_grpc, ydb_operation_v1_pb2_grpc
    from contrib.ydb.public.api.protos import ydb_export_pb2, ydb_import_pb2, ydb_operation_pb2, ydb_status_codes_pb2
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.multicluster_test_base import MulticlusterTestBase
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.sql.lib.test_query import Query


class TestExternalBackupReplication(MulticlusterTestBase):
    @classmethod
    def get_extra_grpc_services(cls):
        return ["replication"]

    @classmethod
    def get_extra_feature_flags(cls):
        return super().get_extra_feature_flags() + ["enable_replication"]

    @classmethod
    def build_cluster(cls):
        return cls.build_cluster_with_external_backup(True)

    @classmethod
    def build_cluster_with_external_backup(cls, enabled):
        config = KikimrConfigGenerator(
            erasure=cls.get_cluster_configuration(),
            extra_feature_flags=cls.get_extra_feature_flags(),
            disabled_feature_flags=["enable_data_shard_direct_part_import"],
            extra_grpc_services=cls.get_extra_grpc_services(),
            query_service_config={"available_external_data_sources": ["ObjectStorage"]},
        )
        # These are immediate controls, not feature flags. Specify them explicitly
        # for both source and destination rather than relying on server defaults.
        config.yaml_config["immediate_controls_config"] = {
            "backup_controls": {
                "s3_controls": {
                    # We bootstrap row replication, not a backed-up controller.
                    "enable_async_replication_export": 0,
                    "enable_async_replication_import": 0,
                    "enable_external_data_source_export": int(enabled),
                    "enable_external_data_source_import": int(enabled),
                    "enable_external_table_export": int(enabled),
                    "enable_external_table_import": int(enabled),
                }
            }
        }
        cluster = KiKiMR(config)
        cluster.start()
        return cluster

    def run_cli(self, cluster, *args):
        return yatest.common.execute([
            self.ydb_cli_path,
            "--endpoint", f"grpc://{self.get_endpoint(cluster)}",
            "--database", self.get_database(),
            *args,
        ]).stdout

    def describe(self, cluster, path):
        return json.loads(self.run_cli(
            cluster, "scheme", "describe", path, "--stats", "--format", "proto-json-base64"))

    def s3_client(self):
        return boto3.client(
            "s3", endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1",
        )

    def run_s3_operation(self, cluster, kind, bucket, mappings):
        # The SDK's expanded --item CLI path does not currently discover EDS or
        # external-table definitions. Call the public API directly with exact
        # items so that the test covers the actual server backup/import path.
        endpoint = urlparse(os.environ["S3_ENDPOINT"])
        assert endpoint.scheme == "http", endpoint
        metadata = (("x-ydb-database", self.get_database()),)
        with grpc.insecure_channel(self.get_endpoint(cluster)) as channel:
            if kind == "export":
                settings = ydb_export_pb2.ExportToS3Settings(
                    endpoint=endpoint.netloc, scheme=ydb_export_pb2.ExportToS3Settings.HTTP,
                    bucket=bucket, access_key="test", secret_key="test", disable_virtual_addressing=True,
                )
                for source_path, destination_prefix in mappings:
                    settings.items.add(source_path=source_path, destination_prefix=destination_prefix)
                request = ydb_export_pb2.ExportToS3Request(settings=settings)
                operation = ydb_export_v1_pb2_grpc.ExportServiceStub(channel).ExportToS3(
                    request, metadata=metadata, timeout=30).operation
            elif kind == "import":
                settings = ydb_import_pb2.ImportFromS3Settings(
                    endpoint=endpoint.netloc, scheme=ydb_import_pb2.ImportFromS3Settings.HTTP,
                    bucket=bucket, access_key="test", secret_key="test", disable_virtual_addressing=True,
                )
                for source_prefix, destination_path in mappings:
                    settings.items.add(source_prefix=source_prefix, destination_path=destination_path)
                request = ydb_import_pb2.ImportFromS3Request(settings=settings)
                operation = ydb_import_v1_pb2_grpc.ImportServiceStub(channel).ImportFromS3(
                    request, metadata=metadata, timeout=30).operation
            else:
                raise ValueError(f"Unsupported S3 operation: {kind}")

            operation_id = operation.id
            operation_stub = ydb_operation_v1_pb2_grpc.OperationServiceStub(channel)

            def completed():
                nonlocal operation
                if operation.ready:
                    return True
                assert operation_id, f"{kind} returned an unfinished operation without an ID"
                operation = operation_stub.GetOperation(
                    ydb_operation_pb2.GetOperationRequest(id=operation_id), metadata=metadata, timeout=30).operation
                return operation.ready

            assert wait_for(completed, timeout_seconds=120), f"{kind} operation {operation_id} did not finish"
            return operation

    @staticmethod
    def create_external_objects(query, source_path, table_path):
        query.query(f"""
            CREATE EXTERNAL DATA SOURCE `{source_path}` WITH (
                SOURCE_TYPE = 'ObjectStorage', LOCATION = 'localhost:1', AUTH_METHOD = 'NONE'
            );
        """)
        query.query(f"""
            CREATE EXTERNAL TABLE `{table_path}` (key Uint64, value Utf8) WITH (
                DATA_SOURCE = '{source_path}', LOCATION = 'records', FORMAT = 'csv_with_names'
            );
        """)

    def test_replication_started_from_backup_with_external_objects(self):
        source_cluster, destination_cluster = self.clusters
        source = Query.create(self.get_database(), self.get_endpoint(source_cluster))
        destination = Query.create(self.get_database(), self.get_endpoint(destination_cluster))
        base = f"backup_external_{self.hash_short}"
        row_name = f"{base}_rows"
        row_path = f"/Root/{row_name}"
        eds_path = f"/Root/{base}_source"
        external_table_path = f"/Root/{base}_external"
        replication_path = f"/Root/{base}_replication"
        source.query(f"CREATE TABLE `{row_path}` (key Uint64, value Utf8, PRIMARY KEY (key))")
        source.query(f"UPSERT INTO `{row_path}` (key, value) VALUES (1u, 'snapshot'), (2u, 'delete-me')")
        self.create_external_objects(source, eds_path, external_table_path)
        original_source = self.describe(source_cluster, eds_path)
        original_external = self.describe(source_cluster, external_table_path)

        destination.query(f"""
            CREATE ASYNC REPLICATION `{replication_path}` FOR `{row_path}` AS `{row_path}` WITH (
                CONNECTION_STRING = 'grpc://{self.get_endpoint(source_cluster)}/?database={self.get_database()}',
                INITIAL_SCAN = FALSE
            )
        """)

        def changefeed_ready():
            description = self.describe(destination_cluster, replication_path)
            assert "error" not in description, description
            items = description.get("items", [])
            if "paused" not in description or len(items) != 1:
                return False
            name = items[0].get("source_changefeed_name")
            table = self.describe(source_cluster, row_path)
            if not any(feed["name"] == name and feed["state"] == "STATE_ENABLED"
                       for feed in table.get("changefeeds", [])):
                return False
            topic = self.describe(source_cluster, f"{row_path}/{name}")
            return any(consumer["name"] == "replicationConsumer" for consumer in topic.get("consumers", []))

        assert wait_for(changefeed_ready, timeout_seconds=100), "Replication changefeed was not ready"
        assert wait_for(lambda: DMLOperations(destination).table_exists(row_name), timeout_seconds=100), \
            "Destination replica placeholder was not created"

        bucket = f"replication-external-{self.hash_short}"
        self.s3_client().create_bucket(Bucket=bucket)
        blocked_replication = self.run_s3_operation(
            destination_cluster, "export", bucket, [(replication_path, f"{base}/controller")])
        assert blocked_replication.status == ydb_status_codes_pb2.StatusIds.UNSUPPORTED, blocked_replication
        assert any("not supported in export" in issue.message and replication_path in issue.message
                   for issue in blocked_replication.issues), blocked_replication
        prefixes = {"row": f"{base}/row", "source": f"{base}/source", "external": f"{base}/external"}
        exported = self.run_s3_operation(source_cluster, "export", bucket, [
            (row_path, prefixes["row"]), (eds_path, prefixes["source"]),
            (external_table_path, prefixes["external"]),
        ])
        assert exported.status == ydb_status_codes_pb2.StatusIds.SUCCESS, exported
        objects = self.s3_client().list_objects_v2(Bucket=bucket).get("Contents", [])
        keys = {obj["Key"] for obj in objects}
        assert {
            f'{prefixes["row"]}/scheme.pb',
            f'{prefixes["source"]}/create_external_data_source.sql',
            f'{prefixes["external"]}/create_external_table.sql',
        } <= keys, keys

        # These writes are absent from the backup. They must arrive from CDC.
        source.query(f"UPSERT INTO `{row_path}` (key, value) VALUES (1u, 'after-backup'), (3u, 'new')")
        source.query(f"DELETE FROM `{row_path}` WHERE key = 2u")
        imported = self.run_s3_operation(destination_cluster, "import", bucket, [
            (prefixes["external"], external_table_path), (prefixes["row"], row_path),
            (prefixes["source"], eds_path),
        ])
        assert imported.status == ydb_status_codes_pb2.StatusIds.SUCCESS, imported

        restored_source = self.describe(destination_cluster, eds_path)
        restored_external = self.describe(destination_cluster, external_table_path)
        assert restored_source["source_type"] == original_source["source_type"] == "ObjectStorage"
        assert restored_source["location"] == original_source["location"] == "localhost:1"
        assert restored_source["properties"]["AUTH_METHOD"] == "NONE"
        assert restored_external["data_source_path"] == original_external["data_source_path"] == eds_path
        for field in ("source_type", "location", "columns", "content"):
            assert restored_external.get(field) == original_external.get(field), field

        def rows():
            return [(row.key, row.value) for row in destination.query(
                f"SELECT key, value FROM `{row_path}` ORDER BY key")]

        assert rows() == [(1, "snapshot"), (2, "delete-me")]
        destination.query(f"ALTER ASYNC REPLICATION `{replication_path}` SET (STATE = 'STANDBY')")
        assert wait_for(lambda: rows() == [(1, "after-backup"), (3, "new")], timeout_seconds=100), \
            "Replication did not apply the writes made after the backup"

    def test_disabled_external_backup_controls_reject_explicit_objects(self):
        source_cluster = self.clusters[0]
        source = Query.create(self.get_database(), self.get_endpoint(source_cluster))
        base = f"backup_disabled_{self.hash_short}"
        source_path = f"/Root/{base}_source"
        table_path = f"/Root/{base}_external"
        self.create_external_objects(source, source_path, table_path)
        bucket = f"replication-disabled-{self.hash_short}"
        self.s3_client().create_bucket(Bucket=bucket)
        original_prefixes = [(source_path, f"{base}/source"), (table_path, f"{base}/external")]
        exported = self.run_s3_operation(source_cluster, "export", bucket, original_prefixes)
        assert exported.status == ydb_status_codes_pb2.StatusIds.SUCCESS, exported
        keys = {obj["Key"] for obj in self.s3_client().list_objects_v2(Bucket=bucket).get("Contents", [])}
        assert {
            f"{base}/source/create_external_data_source.sql",
            f"{base}/external/create_external_table.sql",
        } <= keys, keys

        disabled = self.build_cluster_with_external_backup(False)
        try:
            disabled_query = Query.create(self.get_database(), self.get_endpoint(disabled))
            # The imported table references this exact path. Creating it directly
            # prevents a missing-source error from masking an ignored table flag.
            existing_table = f"/Root/{base}_existing_external"
            self.create_external_objects(disabled_query, source_path, existing_table)
            assert self.describe(disabled, existing_table)["data_source_path"] == source_path
            for path, name in ((source_path, "source"), (existing_table, "external")):
                blocked = self.run_s3_operation(disabled, "export", bucket, [(path, f"blocked/{name}")])
                assert blocked.status == ydb_status_codes_pb2.StatusIds.UNSUPPORTED, blocked
                assert any("not supported in export" in issue.message and path in issue.message
                           for issue in blocked.issues), blocked
            assert not self.s3_client().list_objects_v2(Bucket=bucket, Prefix="blocked/").get("Contents", [])

            for _, name in original_prefixes:
                destination_path = f"/Root/{base}_imported_{name.rsplit('/', 1)[-1]}"
                blocked = self.run_s3_operation(disabled, "import", bucket, [(name, destination_path)])
                assert blocked.status == ydb_status_codes_pb2.StatusIds.CANCELLED, blocked
                assert any("unsupported scheme object type" in issue.message.lower()
                           for issue in blocked.issues), blocked
            children = {entry.name for entry in disabled_query.driver.scheme_client.list_directory("/Root").children}
            assert f"{base}_imported_source" not in children
            assert f"{base}_imported_external" not in children
        finally:
            disabled.stop()
