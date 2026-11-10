import json
import os
from urllib.parse import urlparse

import boto3
import pytest
import yatest.common
import ydb

from ydb.tests.sql.lib.test_query import Query
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.datashard.lib.multicluster_test_base import MulticlusterTestBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.test_pg_base import TestPgBase
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, index_first, index_second, \
    index_first_sync, index_second_sync, index_three_sync, index_four_sync, index_zero_sync, \
    pk_pg_types_mixed, non_pk_pg_types_mixed


def issue_contains(issues, text: str) -> bool:
    for issue in issues or ():
        if text in issue.message:
            return True
        if issue_contains(issue.issues, text):
            return True
    return False


class TestAsyncReplicationBase(MulticlusterTestBase):
    def do_test_async_replication(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        dml_cluster_1 = DMLOperations(Query.create(
            self.get_database(), self.get_endpoint(self.clusters[0])))
        dml_cluster_2 = DMLOperations(Query.create(
            self.get_database(), self.get_endpoint(self.clusters[1])))

        dml_cluster_1.create_table(table_name, pk_types, all_types,
                                   index, ttl, unique, sync)
        dml_cluster_1.insert(table_name, all_types, pk_types, index, ttl, batch_size=10)
        dml_cluster_2.query(f"""
                        CREATE ASYNC REPLICATION `replication_{table_name}`
                        FOR `{self.get_database()}/{table_name}` AS `{self.get_database()}/{table_name}`
                        WITH (
                        CONNECTION_STRING = 'grpc://{self.get_endpoint(self.clusters[0])}/?database={self.get_database()}'
                            )
                         """)
        assert wait_for(lambda: dml_cluster_2.table_exists(table_name), timeout_seconds=100), "Table not created"
        expected_rows = len(pk_types) + len(all_types) + len(index) + bool(ttl)
        assert wait_for(self.create_predicate(expected_rows, table_name, dml_cluster_2.query), timeout_seconds=100), "Not all rows replicated after insert"
        dml_cluster_2.select_after_insert(
            table_name, all_types, pk_types, index, ttl)
        dml_cluster_1.query(f"delete from {table_name}")
        assert wait_for(self.create_predicate(0, table_name, dml_cluster_2.query),
                        timeout_seconds=100) is True, "Expected zero rows after delete"
        dml_cluster_1.insert(table_name, all_types, pk_types, index, ttl, batch_size=10)
        assert wait_for(self.create_predicate(expected_rows, table_name,
                        dml_cluster_2.query), timeout_seconds=100), "Not all rows replicated after reinsert"
        dml_cluster_2.select_after_insert(
            table_name, all_types, pk_types, index, ttl)

    @staticmethod
    def replication_done(destination, table_name: str) -> bool:
        try:
            destination.query(
                f'ALTER ASYNC REPLICATION `replication_{table_name}` SET (STATE = "DONE")')
            return True
        except Exception:
            # The replication may still be lagging: retry until it catches up.
            return False

    @staticmethod
    def query_ok(cluster, sql: str) -> bool:
        try:
            cluster.query(sql)
            return True
        except Exception:
            return False

    def write_after_done(self, destination, table_name: str, sql: str) -> None:
        # DONE only switches the replication state: the destination table becomes writable a bit
        # later, when the replication controller clears its read-only replication config.
        def try_write():
            try:
                destination.query(sql)
            except ydb.issues.Error as ex:
                # The top-level issue is a generic "Bad request. Table: ...", the actual
                # reason is in a nested issue.
                if issue_contains(ex.issues, "Can't execute write tx at replicated table"):
                    return False
                raise
            return True
        assert wait_for(lambda: try_write(), timeout_seconds=100), \
            f"The destination table {table_name} did not become writable"

    def run_ydb_cli(self, cluster, args: list[str]) -> None:
        command = [self.ydb_cli_path, "--endpoint", f"grpc://{self.get_endpoint(cluster)}",
                   "--database", self.get_database()] + args
        result = yatest.common.execute(command, check_exit_code=False)
        assert result.exit_code == 0, \
            f"ydb {' '.join(args)} failed: {(result.std_err or b'').decode('utf-8', 'ignore')}"

    def create_predicate(self, expected_rows, table_name, dml_cluster):
        def predicate():
            rows = dml_cluster(
                f"select count(*) as count from {table_name}")
            return len(rows) == 1 and rows[0].count == expected_rows
        return predicate


class TestAsyncReplication(TestAsyncReplicationBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            # ("table_index_4_UNIQUE_SYNC", pk_types, {},
            # index_four_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_3_UNIQUE_SYNC", pk_types, {},
            # index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            # ("table_index_2_UNIQUE_SYNC", pk_types, {},
            # index_second_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_1_UNIQUE_SYNC", pk_types, {},
            # index_first_sync, "", "UNIQUE", "SYNC"),
            # ("table_index_0_UNIQUE_SYNC", pk_types, {},
            # index_zero_sync, "", "UNIQUE", "SYNC"),
            ("table_index_4__SYNC", pk_types, {},
             index_four_sync, "", "", "SYNC"),
            ("table_index_3__SYNC", pk_types, {},
             index_three_sync, "", "", "SYNC"),
            ("table_index_2__SYNC", pk_types, {},
             index_second_sync, "", "", "SYNC"),
            ("table_index_1__SYNC", pk_types, {},
             index_first_sync, "", "", "SYNC"),
            ("table_index_0__SYNC", pk_types, {},
             index_zero_sync, "", "", "SYNC"),
            ("table_index_1__ASYNC", pk_types, {}, index_second, "", "", "ASYNC"),
            ("table_index_0__ASYNC", pk_types, {}, index_first, "", "", "ASYNC"),
            ("table_all_types", pk_types, {
             **pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_ttl_DyNumber", pk_types, {}, {}, "DyNumber", "", ""),
            ("table_ttl_Uint32", pk_types, {}, {}, "Uint32", "", ""),
            ("table_ttl_Uint64", pk_types, {}, {}, "Uint64", "", ""),
            ("table_ttl_Datetime", pk_types, {}, {}, "Datetime", "", ""),
            ("table_ttl_Timestamp", pk_types, {}, {}, "Timestamp", "", ""),
            ("table_ttl_Date", pk_types, {}, {}, "Date", "", ""),
        ]
    )
    def test_async_replication(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.do_test_async_replication(table_name, pk_types, all_types, index, ttl, unique, sync)


class TestAsyncReplicationSchemaChanges(TestAsyncReplicationBase):
    @classmethod
    def get_extra_feature_flags(cls):
        return super().get_extra_feature_flags() + ["enable_async_replication_schema_changes"]

    def test_add_and_drop_column(self):
        table_name = f"schema_changes_{self.hash_short}"
        table_path = f"{self.get_database()}/{table_name}"
        source = Query.create(self.get_database(), self.get_endpoint(self.clusters[0]))
        destination = Query.create(self.get_database(), self.get_endpoint(self.clusters[1]))
        destination_dml = DMLOperations(destination)

        source.query(f"CREATE TABLE `{table_name}` (key Uint32, value Utf8, PRIMARY KEY (key))")
        source.query(f'UPSERT INTO `{table_name}` (key, value) VALUES (1, "before")')
        destination.query(f"""
            CREATE ASYNC REPLICATION `replication_{table_name}`
            FOR `{table_path}` AS `{table_path}`
            WITH (CONNECTION_STRING =
                'grpc://{self.get_endpoint(self.clusters[0])}/?database={self.get_database()}')
        """)

        def destination_columns():
            description = destination.driver.table_client.session().create().describe_table(table_path)
            return {column.name for column in description.columns}

        assert wait_for(lambda: destination_dml.table_exists(table_name), timeout_seconds=100)
        assert wait_for(lambda: any(row.key == 1 and row.value == "before"
                                    for row in destination.query(f"SELECT key, value FROM `{table_name}`")),
                        timeout_seconds=100)

        source.query(f"ALTER TABLE `{table_name}` ADD COLUMN extra Uint64")
        source.query(f'UPSERT INTO `{table_name}` (key, value, extra) VALUES (2, "after", 42)')
        assert wait_for(lambda: "extra" in destination_columns(), timeout_seconds=100)
        assert wait_for(lambda: any(row.key == 2 and row.extra == 42
                                    for row in destination.query(f"SELECT key, extra FROM `{table_name}`")),
                        timeout_seconds=100)

        source.query(f"ALTER TABLE `{table_name}` DROP COLUMN value")
        assert wait_for(lambda: "value" not in destination_columns(), timeout_seconds=100)
        rows = destination.query(f"SELECT key, extra FROM `{table_name}` ORDER BY key")
        assert len(rows) == 2
        assert rows[1].key == 2 and rows[1].extra == 42


class TestAsyncReplicationSerial(TestAsyncReplicationBase):
    @classmethod
    def get_extra_grpc_services(cls):
        return ["replication"]

    def test_serial_column(self):
        table_name = f"serial_{self.hash_short}"
        table_path = f"{self.get_database()}/{table_name}"

        source = Query.create(self.get_database(), self.get_endpoint(self.clusters[0]))
        destination = Query.create(self.get_database(), self.get_endpoint(self.clusters[1]))

        # `Serial` is a column with a default from a table-local sequence.
        source.query(f"CREATE TABLE `{table_name}` (id Serial, value Utf8, PRIMARY KEY (id))")

        destination.query(f"""
            CREATE ASYNC REPLICATION `replication_{table_name}`
            FOR `{table_path}` AS `{table_path}`
            WITH (CONNECTION_STRING =
                'grpc://{self.get_endpoint(self.clusters[0])}/?database={self.get_database()}')
        """)

        destination_dml = DMLOperations(destination)
        assert wait_for(lambda: destination_dml.table_exists(table_name), timeout_seconds=100), "Table not created"

        # Rows are inserted after the destination is created, so its sequence starts below them and
        # only the replication itself can advance it past the applied values.
        source.query(f'INSERT INTO `{table_name}` (value) VALUES ("a"), ("b"), ("c")')

        source_ids = sorted(row.id for row in source.query(f"SELECT id FROM `{table_name}`"))
        assert len(source_ids) == 3, f"Unexpected number of source rows: {source_ids}"
        assert len(set(source_ids)) == 3, f"Ids are expected to be unique: {source_ids}"

        def replicated_ids():
            return sorted(row.id for row in destination.query(f"SELECT id FROM `{table_name}`"))

        assert wait_for(lambda: replicated_ids() == source_ids, timeout_seconds=100), \
            f"Replicated ids {replicated_ids()} differ from the source {source_ids}"

        # The destination owns its own sequence, and replication keeps it ahead of the applied data:
        # after the switchover generated ids continue above the replicated ones instead of colliding.
        assert wait_for(lambda: self.replication_done(destination, table_name), timeout_seconds=100), \
            "Cannot complete the replication"

        self.write_after_done(destination, table_name, f'INSERT INTO `{table_name}` (value) VALUES ("new")')
        new_ids = [row.id for row in destination.query(
            f'SELECT id FROM `{table_name}` WHERE value = "new"')]
        assert len(new_ids) == 1, f"Unexpected number of inserted rows: {new_ids}"
        assert new_ids[0] > max(source_ids), \
            f"The destination sequence was not advanced: {new_ids[0]} <= {max(source_ids)}"

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

    # The destination may also be seeded with a dump of the source (a backup-restore initial
    # synchronization) instead of an initial scan: the restored table brings its data and its
    # sequence position, and the replication continues on top of it.
    def test_serial_column_seeded_from_dump(self):
        table_name = f"serial_dump_{self.hash_short}"
        table_path = f"{self.get_database()}/{table_name}"

        source = Query.create(self.get_database(), self.get_endpoint(self.clusters[0]))
        destination = Query.create(self.get_database(), self.get_endpoint(self.clusters[1]))

        source.query(f"CREATE TABLE `{table_name}` (id Serial, value Utf8, PRIMARY KEY (id))")
        source.query(f'INSERT INTO `{table_name}` (value) VALUES ("a"), ("b"), ("c")')

        destination.query(f"""
            CREATE ASYNC REPLICATION `replication_{table_name}`
            FOR `{table_path}` AS `{table_path}`
            WITH (
                CONNECTION_STRING = 'grpc://{self.get_endpoint(self.clusters[0])}/?database={self.get_database()}',
                INITIAL_SCAN = FALSE
            )
        """)

        def source_ids():
            return sorted(row.id for row in source.query(f"SELECT id FROM `{table_name}`"))

        def destination_ids():
            return sorted(row.id for row in destination.query(f"SELECT id FROM `{table_name}`"))

        assert len(source_ids()) == 3

        # FIXME for the future: remove duplicate code (test_consistency.py)
        def changefeed_ready():
            description = self.describe(self.clusters[1], f"replication_{table_name}")
            assert "error" not in description, description
            items = description.get("items", [])
            if "paused" not in description or len(items) != 1:
                return False
            name = items[0].get("source_changefeed_name")
            table = self.describe(self.clusters[0], table_path)
            if not any(feed["name"] == name and feed["state"] == "STATE_ENABLED" for feed in table.get("changefeeds", [])):
                return False
            topic = self.describe(self.clusters[0], f"{table_path}/{name}")
            return any(consumer["name"] == "replicationConsumer" for consumer in topic.get("consumers", []))
        assert wait_for(changefeed_ready, timeout_seconds=100), "Source changefeed is not ready for backup"
        assert wait_for(lambda: DMLOperations(destination).table_exists(table_name), timeout_seconds=100), \
            "Destination placeholder was not created"

        bucket = f"replication-{self.hash_short}"
        boto3.client(
            "s3", endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1",
        ).create_bucket(Bucket=bucket)
        self.run_s3_operation(self.clusters[0], "export", bucket, table_path, table_name)
        self.run_s3_operation(self.clusters[1], "import", bucket, table_name, table_path)
        destination.query(f"ALTER ASYNC REPLICATION `replication_{table_name}` SET (STATE = 'STANDBY')")

        source.query(f'INSERT INTO `{table_name}` (value) VALUES ("d")')

        assert wait_for(lambda: destination_ids() == source_ids(), timeout_seconds=100), \
            "New rows were not replicated"

        assert wait_for(lambda: self.replication_done(destination, table_name), timeout_seconds=100), \
            "Cannot complete the replication"

        # The restored position of the destination sequence is behind the replicated values, so only
        # the advance keeps a generated id free of collisions.
        self.write_after_done(destination, table_name, f'INSERT INTO `{table_name}` (value) VALUES ("e")')
        new_ids = [row.id for row in destination.query(
            f'SELECT id FROM `{table_name}` WHERE value = "e"')]
        assert len(new_ids) == 1, f"Unexpected number of inserted rows: {new_ids}"
        assert new_ids[0] > max(source_ids()), \
            f"The destination sequence was not advanced: {new_ids[0]} <= {max(source_ids())}"


class TestPgAsyncReplication(TestPgBase, TestAsyncReplicationBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            # TODO: add UNIQUE_SYNC
            ("table_index_0__SYNC", pk_pg_types_mixed, {}, pk_pg_types_mixed, "", "", "SYNC"),
            ("table_index_0__ASYNC", pk_pg_types_mixed, {}, pk_pg_types_mixed, "", "", "ASYNC"),
            ("table_all_types", pk_pg_types_mixed, {**pk_pg_types_mixed, **non_pk_pg_types_mixed}, {}, "", "", ""),
            ("table_ttl_pgint4", pk_pg_types_mixed, {}, {}, "pgint4", "", ""),
            ("table_ttl_pgint8", pk_pg_types_mixed, {}, {}, "pgint8", "", ""),
            ("table_ttl_pgdate", pk_pg_types_mixed, {}, {}, "pgdate", "", ""),
            ("table_ttl_pgtimestamp", pk_pg_types_mixed, {}, {}, "pgtimestamp", "", ""),
        ]
    )
    def test_async_replication(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.do_test_async_replication(table_name, pk_types, all_types, index, ttl, unique, sync)

    def test_serial_column(self):
        table_name = f"pg_serial_{self.hash_short}"
        table_path = f"{self.get_database()}/{table_name}"

        source = Query.create(self.get_database(), self.get_endpoint(self.clusters[0]))
        destination = Query.create(self.get_database(), self.get_endpoint(self.clusters[1]))

        # A PostgreSQL serial column is a pg integer backed by a native sequence, so replication
        # has to advance it the same way it does for YQL serial columns.
        source.query(f"CREATE TABLE `{table_name}` (id Serial, value Utf8, PRIMARY KEY (id))")

        # An explicit value far above the sequence position: only the advance moves it past.
        source.query(f'INSERT INTO `{table_name}` (id, value) VALUES (1000, "a")')

        destination.query(f"""
            CREATE ASYNC REPLICATION `replication_{table_name}`
            FOR `{table_path}` AS `{table_path}`
            WITH (CONNECTION_STRING =
                'grpc://{self.get_endpoint(self.clusters[0])}/?database={self.get_database()}')
        """)

        destination_dml = DMLOperations(destination)
        assert wait_for(lambda: destination_dml.table_exists(table_name), timeout_seconds=100), "Table not created"

        count_query = f"SELECT COUNT(*) AS count FROM `{table_name}`"
        assert wait_for(lambda: destination.query(count_query)[0].count == 1, timeout_seconds=100), \
            "Data was not replicated"

        assert wait_for(lambda: self.replication_done(destination, table_name), timeout_seconds=100), \
            "Cannot complete the replication"

        # A generated value must not collide with the replicated one, so the destination sequence
        # has to be past it.
        self.write_after_done(destination, table_name, f'INSERT INTO `{table_name}` (value) VALUES ("b")')
        assert wait_for(lambda: destination.query(count_query)[0].count == 2, timeout_seconds=100), \
            "The row was not inserted"
        assert wait_for(lambda: destination.query(
            f"SELECT COUNT(*) AS count FROM `{table_name}` WHERE id > 1000")[0].count == 1,
            timeout_seconds=100), "The destination sequence was not advanced"
