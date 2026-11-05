import json
import os
from urllib.parse import urlparse

import boto3
import pytest
import yatest.common

from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.multicluster_test_base import MulticlusterTestBase
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.sql.lib.test_query import Query


class TestAsyncReplicationConsistency(MulticlusterTestBase):
    @classmethod
    def get_extra_grpc_services(cls):
        return ["replication"]

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

    def run_s3_operation(self, cluster, operation, bucket, source, destination):
        endpoint = urlparse(os.environ["S3_ENDPOINT"])
        result = json.loads(self.run_cli(
            cluster, operation, "s3",
            "--s3-endpoint", endpoint.netloc, "--scheme", endpoint.scheme,
            "--bucket", bucket, "--access-key", "test", "--secret-key", "test",
            "--use-virtual-addressing", "false",
            "--item", f"Source={source},Destination={destination}",
            "--format", "proto-json-base64"))
        operation_id = result["id"]

        def completed():
            nonlocal result
            result = json.loads(self.run_cli(
                cluster, "operation", "get", operation_id, "--format", "proto-json-base64"))
            if not result.get("ready", False):
                return False
            assert result.get("status") == "SUCCESS", result
            return True

        assert wait_for(completed, timeout_seconds=100), f"{operation} did not finish: {result}"
        self.run_cli(cluster, "operation", "forget", operation_id)

    @pytest.mark.parametrize("from_backup", [False, True], ids=["initial_scan", "backup"])
    @pytest.mark.parametrize("consistency_level", ["ROW", "GLOBAL"])
    def test_consistency(self, from_backup, consistency_level):
        table_name = f"consistency_{self.hash_short}"
        table_path = f"{self.get_database()}/{table_name}"
        replication_path = f"{self.get_database()}/replication_{table_name}"
        source_cluster, destination_cluster = self.clusters
        source = Query.create(self.get_database(), self.get_endpoint(source_cluster))
        destination = Query.create(self.get_database(), self.get_endpoint(destination_cluster))

        source.query(f"""
            CREATE TABLE `{table_path}` (
                key Uint32, value Utf8, optional_value Uint64, PRIMARY KEY (key)
            ) WITH (UNIFORM_PARTITIONS = 1)
        """)

        def write_rows(first, last, phase):
            values = ", ".join(
                f"({key}, '{phase}-{key}', {key if key % 2 else 'NULL'})"
                for key in range(first, last)
            )
            source.query(f"UPSERT INTO `{table_path}` (key, value, optional_value) VALUES {values}")

        write_rows(0, 100, "before")
        commit_interval = ", COMMIT_INTERVAL = Interval('PT30S')" if consistency_level == "GLOBAL" else ""
        destination.query(f"""
            CREATE ASYNC REPLICATION `{replication_path}`
            FOR `{table_path}` AS `{table_path}`
            WITH (
                CONNECTION_STRING = 'grpc://{self.get_endpoint(source_cluster)}/?database={self.get_database()}',
                INITIAL_SCAN = {'FALSE' if from_backup else 'TRUE'},
                CONSISTENCY_LEVEL = '{consistency_level}'{commit_interval}
            )
        """)

        if from_backup:
            def changefeed_ready():
                description = self.describe(destination_cluster, replication_path)
                assert "error" not in description, description
                items = description.get("items", [])
                if "paused" not in description or len(items) != 1:
                    return False
                name = items[0].get("source_changefeed_name")
                table = self.describe(source_cluster, table_path)
                if not any(feed["name"] == name and feed["state"] == "STATE_ENABLED"
                           for feed in table.get("changefeeds", [])):
                    return False
                topic = self.describe(source_cluster, f"{table_path}/{name}")
                return any(consumer["name"] == "replicationConsumer" for consumer in topic.get("consumers", []))

            assert wait_for(changefeed_ready, timeout_seconds=100), "Source changefeed is not ready for backup"
            assert wait_for(lambda: DMLOperations(destination).table_exists(table_name), timeout_seconds=100), \
                "Destination placeholder was not created"
            bucket = f"replication-{self.hash_short}"
            boto3.client(
                "s3", endpoint_url=os.environ["S3_ENDPOINT"],
                aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1",
            ).create_bucket(Bucket=bucket)
            self.run_s3_operation(source_cluster, "export", bucket, table_path, table_name)
            # These changes must be recovered from CDC, since they are absent from the backup.
            write_rows(0, 25, "during_backup")
            self.run_s3_operation(destination_cluster, "import", bucket, table_name, table_path)
            destination.query(f"ALTER ASYNC REPLICATION `{replication_path}` SET (STATE = 'STANDBY')")

        assert wait_for(lambda: DMLOperations(destination).table_exists(table_name), timeout_seconds=100), \
            "Destination table was not created"
        # Update existing rows as well as inserting new ones: row counts alone cannot detect lost updates.
        write_rows(50, 150, "after")
        write_rows(150, 151, "marker")

        # Lag statistics can still show zero from before the writes. In this single-partition
        # table, observing the final CDC record establishes that the preceding writes were applied.
        assert wait_for(lambda: destination.query(
            f"SELECT key FROM `{table_path}` WHERE key = 150 AND value = 'marker-150'"),
            timeout_seconds=100), "Final write was not replicated"

        last_description = None

        def zero_lag():
            nonlocal last_description
            last_description = self.describe(destination_cluster, replication_path)
            assert "error" not in last_description, last_description
            stats = last_description.get("running", {}).get("stats", {})
            # An absent lag is unknown, not zero. Initial scan completion alone is insufficient.
            return stats.get("lag") == "0s" and stats.get("initial_scan_progress", 100) == 100

        assert wait_for(zero_lag, timeout_seconds=100), f"Replication did not reach zero lag: {last_description}"
        checksum_query = f"""
            SELECT COUNT(*) AS row_count, SUM(Digest::CityHash(StablePickle(TableRow()))) AS checksum
            FROM `{table_path}`
        """
        source_checksum = source.query(checksum_query)[0]
        destination_checksum = destination.query(checksum_query)[0]
        assert source_checksum.row_count == 151, source_checksum
        assert source_checksum == destination_checksum, \
            f"Source checksum: {source_checksum}; destination checksum: {destination_checksum}"
