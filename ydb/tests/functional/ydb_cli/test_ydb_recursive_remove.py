from hamcrest import assert_that, contains_inanyorder, only_contains
import pytest

import yatest

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import ydb_bin

CLUSTER_CONFIG = dict(
    extra_feature_flags=["enable_external_data_sources", "enable_schema_secrets"],
    disabled_feature_flags=["enable_system_names_protection"],
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
            execute_ydb_cli_command(
                ydb_cluster.nodes[1], database_path, ["scheme", "rmdir", "--recursive", "--force", "."]
            )
            assert_that(
                [child.name for child in driver.scheme_client.list_directory(database_path).children],
                only_contains(".metadata", ".sys"),
            )
