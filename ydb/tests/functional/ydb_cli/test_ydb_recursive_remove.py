import json

from hamcrest import assert_that, contains_inanyorder, only_contains
import pytest

import yatest

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.functional.ydb_cli.ydb_cli_helpers import ydb_bin
from ydb.tests.library.common.wait_for import wait_for

CLUSTER_CONFIG = dict(
    extra_feature_flags=["enable_external_data_sources", "enable_schema_secrets", "enable_backup_service"],
    disabled_feature_flags=["enable_system_names_protection"],
    extra_grpc_services=["keyvalue"],
    enable_resource_pools=True,
    query_service_config=dict(
        available_external_data_sources=["ObjectStorage"]
    ))


def execute_ydb_cli_command(node, database, args, stdin=None):
    execution = yatest.common.execute(
        [ydb_bin(), "--endpoint", f"grpc://{node.host}:{node.grpc_port}", "--database", database] + args, stdin=stdin
    )
    return execution.std_out.decode("utf-8")


def create_table(session, table):
    session.execute_scheme(
        f"""
        CREATE TABLE `{table}` (key Int, value Utf8, PRIMARY KEY(key));
        """
    )


def create_view(session, view, query="SELECT 42"):
    session.execute_scheme(
        f"""
        CREATE VIEW `{view}` WITH security_invoker = TRUE AS {query};
        """
    )


def create_secret(session, secret):
    session.execute_scheme(
        f"""
        CREATE SECRET `{secret}` WITH (value = 'synthetic-value');
        """
    )


def create_external_data_source(session, external_data_source):
    session.execute_scheme(
        f"""
        CREATE EXTERNAL DATA SOURCE `{external_data_source}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="192.168.1.1:8123",
            AUTH_METHOD="NONE"
        );
        """
    )


def create_external_table(session, external_table, external_data_source):
    session.execute_scheme(
        f"""
        CREATE EXTERNAL TABLE `{external_table}` (
            key Utf8 NOT NULL,
            value Utf8 NOT NULL
        ) WITH (
            DATA_SOURCE="{external_data_source}",
            LOCATION="folder",
            FORMAT="csv_with_names",
            COMPRESSION="gzip"
        );
        """
    )


class TestRecursiveRemove:
    @pytest.fixture(autouse=True, scope="function")
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path

    @pytest.mark.parametrize(
        "path,volume", [(".", "volume"), ("volumes", "volumes/nested/volume")],
        ids=["database_root", "nested_directory"],
    )
    def test_solomon_volumes(self, ydb_cluster, ydb_database, ydb_client, path, volume):
        driver = ydb_client(ydb_database)
        driver.wait(timeout=10, fail_fast=True)
        if "/" in volume:
            driver.scheme_client.make_directory(f"{ydb_database}/{volume.rsplit('/', 1)[0]}")
        response = ydb_cluster.kv_client.create_tablets(1, f"{ydb_database}/{volume}")
        assert response.operation.status == StatusIds.SUCCESS, response
        entry = json.loads(execute_ydb_cli_command(
            ydb_cluster.nodes[1], ydb_database,
            ["scheme", "ls", "--format", "json", f"{ydb_database}/{volume}"],
        ))
        assert entry == {"type": "solomon-volume", "path": "volume"}
        execute_ydb_cli_command(
            ydb_cluster.nodes[1], ydb_database, ["scheme", "rmdir", "--recursive", "--force", path]
        )
        remaining = json.loads(execute_ydb_cli_command(
            ydb_cluster.nodes[1], ydb_database, ["scheme", "ls", "--format", "json", "."],
        ))
        assert_that(
            [child["path"] for child in remaining],
            only_contains(".metadata", ".sys"),
        )

    def test_resource_pools(self, ydb_cluster, ydb_database, ydb_client, ydb_client_session):
        driver = ydb_client(ydb_database)
        with ydb_client_session(ydb_database).checkout() as session:
            session.execute_scheme("CREATE RESOURCE POOL test_pool WITH (concurrent_query_limit = 1);")
        pools = f"{ydb_database}/.metadata/workload_manager/pools"
        assert "test_pool" in [child.name for child in driver.scheme_client.list_directory(pools).children]
        execute_ydb_cli_command(
            ydb_cluster.nodes[1], ydb_database,
            ["scheme", "rmdir", "--recursive", "--force", f"{pools}/test_pool"],
        )
        assert "test_pool" not in [child.name for child in driver.scheme_client.list_directory(pools).children]

    @pytest.mark.parametrize("with_backup", [False, True], ids=["empty", "populated"])
    def test_backup_collections(self, ydb_cluster, ydb_database, ydb_client, ydb_client_session, with_backup):
        driver = ydb_client(ydb_database)
        driver.wait(timeout=10, fail_fast=True)
        collections = f"{ydb_database}/.backups/collections"
        with ydb_client_session(ydb_database).checkout() as session:
            create_table(session, "source")
            session.transaction().execute(
                'UPSERT INTO source (key, value) VALUES (1, "retained");', commit_tx=True,
            )
            session.execute_scheme(
                f"CREATE BACKUP COLLECTION `{collections}/test_collection` "
                f"(TABLE `{ydb_database}/source`) WITH (STORAGE = 'cluster');"
            )
        entries = json.loads(execute_ydb_cli_command(
            ydb_cluster.nodes[1], ydb_database, ["scheme", "ls", "--format", "json", collections],
        ))
        assert entries == [{"type": "backup-collection", "path": "test_collection"}]
        if with_backup:
            backup = json.loads(execute_ydb_cli_command(
                ydb_cluster.nodes[1], ydb_database,
                ["sql", "--script", "BACKUP `test_collection`;", "--format", "json-unicode"],
            ))
            operation_id = f"ydb://fullbackup/14?id={backup['operation_id']}"

            def backup_is_complete():
                operation = json.loads(execute_ydb_cli_command(
                    ydb_cluster.nodes[1], ydb_database,
                    ["operation", "get", operation_id, "--format", "proto-json-base64"],
                ))
                if not operation.get("ready", False):
                    return False
                assert operation["status"] == "SUCCESS", operation
                return True

            assert wait_for(backup_is_complete, timeout_seconds=60), f"Backup {operation_id} did not complete"
            snapshots = driver.scheme_client.list_directory(f"{collections}/test_collection").children
            assert len(snapshots) == 1
            snapshot_path = f"{collections}/test_collection/{snapshots[0].name}"
            assert [child.name for child in driver.scheme_client.list_directory(snapshot_path).children] == ["source"]

        node = ydb_cluster.nodes[1]
        removal = yatest.common.execute(
            [ydb_bin(), "--endpoint", f"grpc://{node.host}:{node.grpc_port}", "--database", ydb_database,
             "scheme", "rmdir", "--recursive", "--force", collections],
        )
        assert b"Couldn't delete path" not in removal.std_err, removal.std_err.decode("utf-8")
        assert not driver.scheme_client.list_directory(f"{ydb_database}/.backups").children
        assert driver.scheme_client.describe_path(f"{ydb_database}/source").name == "source"
        with ydb_client_session(ydb_database).checkout() as session:
            rows = session.transaction().execute("SELECT key, value FROM source;", commit_tx=True)[0].rows
        assert [(row.key, row.value) for row in rows] == [(1, "retained")]

    @pytest.mark.parametrize(
        "path,secret", [(".", "secret"), ("secrets", "secrets/nested/secret")],
        ids=["database_root", "nested_directory"],
    )
    def test_secrets(self, ydb_cluster, ydb_database, ydb_client, ydb_client_session, path, secret):
        database_path = ydb_database
        driver = ydb_client(database_path)
        session_pool = ydb_client_session(database_path)
        with session_pool.checkout() as session:
            create_secret(session, secret)
            execute_ydb_cli_command(
                ydb_cluster.nodes[1], database_path, ["scheme", "rmdir", "--recursive", "--force", path]
            )
            assert_that(
                [child.name for child in driver.scheme_client.list_directory(database_path).children],
                only_contains(".metadata", ".sys"),
            )

    @pytest.mark.parametrize(
        "path", [".", "~backup_root"], ids=["database_root", "backup_directory"],
    )
    def test_tilde_names(self, ydb_cluster, ydb_database, ydb_client, ydb_client_session, path):
        database_path = ydb_database
        driver = ydb_client(database_path)
        session_pool = ydb_client_session(database_path)
        prefix = "" if path == "." else f"{path}/"
        with session_pool.checkout() as session:
            create_table(session, f"{prefix}~table")
            create_table(session, f"{prefix}~dir/~nested/~table")
            create_table(session, f"{prefix}~backup_12345/~nested/~table")
            create_table(session, f"{prefix}export-12345/nested/table")
            driver.scheme_client.make_directory(f"{database_path}/{prefix}~empty")
            driver.scheme_client.make_directory(f"{database_path}/{prefix}~backup_empty")
            driver.scheme_client.make_directory(f"{database_path}/{prefix}export-empty")
            execute_ydb_cli_command(
                ydb_cluster.nodes[1], database_path, ["scheme", "rmdir", "--recursive", "--force", path]
            )
            assert_that(
                [child.name for child in driver.scheme_client.list_directory(database_path).children],
                only_contains(".metadata", ".sys"),
            )

    def test_system_name_prefixes(self, ydb_cluster, ydb_database, ydb_client, ydb_client_session):
        database_path = ydb_database
        driver = ydb_client(database_path)
        session_pool = ydb_client_session(database_path)
        system_directories = [".sys_health", ".metadata_user", ".tmp_user", ".backups_user"]
        with session_pool.checkout() as session:
            for directory in system_directories:
                create_table(session, f"{directory}/table")
            expected_entries = [child.name for child in driver.scheme_client.list_directory(database_path).children]
            create_table(session, "ordinary/nested/table")
            execute_ydb_cli_command(
                ydb_cluster.nodes[1], database_path, ["scheme", "rmdir", "--recursive", "--force", "."]
            )
            assert_that(
                [child.name for child in driver.scheme_client.list_directory(database_path).children],
                contains_inanyorder(*expected_entries),
            )
            for directory in system_directories:
                session.describe_table(f"{database_path}/{directory}/table")

    def test_various_scheme_objects(self, ydb_cluster, ydb_database, ydb_client, ydb_client_session):
        database_path = ydb_database
        driver = ydb_client(database_path)
        session_pool = ydb_client_session(database_path)
        with session_pool.checkout() as session:
            create_table(session, "table")
            create_view(session, "views/view")
            create_secret(session, "secret")
            create_secret(session, "secrets/nested/secret")
            create_external_data_source(session, "externals/data_source")
            create_external_table(session, "externals/table", "externals/data_source")
            response = ydb_cluster.kv_client.create_tablets(1, f"{database_path}/volume")
            assert response.operation.status == StatusIds.SUCCESS, response
            execute_ydb_cli_command(
                ydb_cluster.nodes[1], database_path, ["scheme", "rmdir", "--recursive", "--force", "."]
            )
            assert_that(
                [child.name for child in driver.scheme_client.list_directory(database_path).children],
                only_contains(".metadata", ".sys"),
            )
