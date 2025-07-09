from hamcrest import assert_that, only_contains
import os
import pytest

import yatest

CLUSTER_CONFIG = dict(
    extra_feature_flags=["enable_external_data_sources"],
    query_service_config=dict(
        available_external_data_sources=["ObjectStorage"]
    ))


def bin_from_env(var):
    if os.getenv(var):
        return yatest.common.binary_path(os.getenv(var))
    raise RuntimeError(f"{var} environment variable is not specified")


def ydb_bin():
    return bin_from_env("YDB_CLI_BINARY")


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

    def test_various_scheme_objects(self, ydb_cluster, ydb_database, ydb_client, ydb_client_session):
        database_path = ydb_database
        driver = ydb_client(database_path)
        session_pool = ydb_client_session(database_path)
        with session_pool.checkout() as session:
            create_table(session, "table")
            create_view(session, "views/view")
            create_external_data_source(session, "externals/data_source")
            create_external_table(session, "externals/table", "externals/data_source")
            execute_ydb_cli_command(
                ydb_cluster.nodes[1], database_path, ["scheme", "rmdir", "--recursive", "--force", "."]
            )
            assert_that(
                [child.name for child in driver.scheme_client.list_directory(database_path).children],
                only_contains(".metadata", ".sys"),
            )
