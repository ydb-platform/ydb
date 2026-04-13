import json
import os
import pytest

import yatest

from ydb.tests.oss.canonical import set_canondata_root

CLUSTER_CONFIG = dict(
    extra_feature_flags=["enable_views", "enable_external_data_sources"],
    extra_grpc_services=["view"],
    query_service_config=dict(available_external_data_sources=["ObjectStorage"]),
)


def bin_from_env(var):
    if os.getenv(var):
        return yatest.common.binary_path(os.getenv(var))
    raise RuntimeError(f"{var} environment variable is not specified")


def ydb_bin():
    return bin_from_env("YDB_CLI_BINARY")


def canonical_result(output_result, tmp_path):
    output_path = tmp_path / "result.output"
    with output_path.open("w") as f:
        f.write(output_result)
    return yatest.common.canonical_file(output_path, local=True, universal_lines=True)


def execute_ydb_cli_command(node, database, args, stdin=None):
    execution = yatest.common.execute(
        [ydb_bin(), "--endpoint", f"grpc://{node.host}:{node.grpc_port}", "--database", database] + args, stdin=stdin
    )
    return execution.std_out.decode("utf-8")


def create_view(session, view, query="SELECT 42"):
    session.execute_scheme(
        f"""
        CREATE VIEW {view} WITH security_invoker = TRUE AS {query};
        """
    )


def create_external_data_source(session, external_data_source):
    session.execute_scheme(
        f"""
        CREATE EXTERNAL DATA SOURCE {external_data_source} WITH (
            SOURCE_TYPE = "ObjectStorage",
            LOCATION = "localhost:1",
            AUTH_METHOD = "NONE"
        );
        """
    )


def create_external_table(session, external_table, external_data_source):
    session.execute_scheme(
        f"""
        CREATE EXTERNAL TABLE {external_table} (
            key Int,
            value Utf8
        ) WITH (
            DATA_SOURCE = "{external_data_source}",
            LOCATION = "whatever",
            FORMAT = "csv_with_names",
            COMPRESSION = "gzip"
        );
        """
    )


def create_secret(session, secret_name, value):
    session.execute_scheme(
        f"""
        CREATE SECRET `{secret_name}` WITH ( value = '{value}' );
        """
    )


def alter_secret(session, secret_name, value):
    session.execute_scheme(
        f"""
        ALTER SECRET `{secret_name}` WITH ( value = '{value}' );
        """
    )


class TestSchemeDescribe:
    @pytest.fixture(autouse=True, scope="function")
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        set_canondata_root("ydb/tests/functional/ydb_cli/canondata")

    def test_describe_external_table_references_json(self, ydb_cluster, ydb_database, ydb_client_session):
        database_path = ydb_database
        external_data_source = "external_data_source"
        external_table = "external_table"
        session_pool = ydb_client_session(database_path)
        with session_pool.checkout() as session:
            create_external_data_source(session, external_data_source)
            create_external_table(session, external_table, external_data_source)

            output = execute_ydb_cli_command(
                ydb_cluster.nodes[1],
                database_path,
                ["scheme", "describe", "--format", "proto-json-base64", external_table],
            )
            description = json.loads(output)
            source = description["data_source_path"]
            expected_source = database_path.rstrip("/") + "/" + external_data_source
            assert source == expected_source

            output = execute_ydb_cli_command(
                ydb_cluster.nodes[1],
                database_path,
                ["scheme", "describe", "--format", "proto-json-base64", external_data_source],
            )
            description = json.loads(output)
            print(description)
            references = json.loads(description["properties"]["REFERENCES"])
            expected_reference = database_path.rstrip("/") + "/" + external_table
            assert isinstance(references, list)
            assert len(references) == 1
            assert references[0] == expected_reference


class TestSecretSchemeDescribe:
    secret_value_created = "secret-random-value-9f3a2c1e"
    secret_value_altered = "secret-updated-value-7b4d8e2a"

    @staticmethod
    def _parse_secret_version_from_scheme_describe(describe_result):
        for line in describe_result.splitlines():
            line = line.strip()
            if line.startswith("Version:"):
                parts = line.split()
                return int(parts[1])
        raise AssertionError(f"Version is missing: {describe_result}")

    def _assert_scheme_describe_secret_default(
        self, ydb_cluster, ydb_database, secret_name, absent_substrings, expected_version
    ):
        describe_result = execute_ydb_cli_command(
            ydb_cluster.nodes[1],
            ydb_database,
            ["scheme", "describe", secret_name],
        )
        # type check
        assert "<secret>" in describe_result
        # secret value check: it should never be exposed
        for marker in absent_substrings:
            assert marker not in describe_result
        # version check
        version = self._parse_secret_version_from_scheme_describe(describe_result)
        assert version == expected_version, f"version: {version}"

    def _assert_scheme_describe_secret_proto_json_base64(
        self, ydb_cluster, ydb_database, secret_name, absent_substrings, expected_version
    ):
        describe_result = execute_ydb_cli_command(
            ydb_cluster.nodes[1],
            ydb_database,
            ["scheme", "describe", "--format", "proto-json-base64", secret_name],
        )
        description = json.loads(describe_result)
        # type check
        assert description["self"]["type"] == "SECRET"
        # secret value check: it should never be exposed
        for marker in absent_substrings:
            assert marker not in describe_result  # raw CLI output, not only parsed JSON
        # version check
        if expected_version is None:
            # There's no assert on `version` value here: proto3 JSON omits default fields
            assert "version" not in description
        else:
            assert int(description["version"]) == expected_version, (
                f"actual version: {description.get('version')!r}"
            )

    @pytest.fixture(scope="module")
    def ydb_cluster_configuration(self, request):
        return dict(
            extra_grpc_services=["secret"],
        )

    def test_describe_secret_format_default(self, ydb_cluster, ydb_database, ydb_client_session):
        secret_name = "cli_scheme_describe_secret"
        session_pool = ydb_client_session(ydb_database)

        # check the initial state
        with session_pool.checkout() as session:
            create_secret(session, secret_name, self.secret_value_created)
        self._assert_scheme_describe_secret_default(
            ydb_cluster,
            ydb_database,
            secret_name,
            absent_substrings=[self.secret_value_created],
            expected_version=0,
        )

        # check the altered state
        with session_pool.checkout() as session:
            alter_secret(session, secret_name, self.secret_value_altered)
        self._assert_scheme_describe_secret_default(
            ydb_cluster,
            ydb_database,
            secret_name,
            absent_substrings=[self.secret_value_created, self.secret_value_altered],
            expected_version=1,
        )

    def test_describe_secret_format_proto_json(self, ydb_cluster, ydb_database, ydb_client_session):
        secret_name = "cli_scheme_describe_secret_json"
        session_pool = ydb_client_session(ydb_database)

        # check the initial state
        with session_pool.checkout() as session:
            create_secret(session, secret_name, self.secret_value_created)
        self._assert_scheme_describe_secret_proto_json_base64(
            ydb_cluster,
            ydb_database,
            secret_name,
            absent_substrings=[self.secret_value_created],
            expected_version=None,
        )

        # check the altered state
        with session_pool.checkout() as session:
            alter_secret(session, secret_name, self.secret_value_altered)
        self._assert_scheme_describe_secret_proto_json_base64(
            ydb_cluster,
            ydb_database,
            secret_name,
            absent_substrings=[self.secret_value_created, self.secret_value_altered],
            expected_version=1,
        )


class TestViewSchemeDescribe:
    @pytest.fixture(
        scope="module",
        params=[
            (True, True),
            (True, False),
            (False, True),
            (False, False),
        ],
        ids=lambda param: f"show_create_{"enabled" if param[0] else "disabled"}_view_service_{"enabled" if param[1] else "disabled"}",
    )
    def ydb_cluster_configuration(self, request):
        show_create_enabled, view_service_enabled = request.param

        extra_feature_flags = []
        disabled_feature_flags = []
        if show_create_enabled:
            extra_feature_flags = ["enable_show_create"]
        else:
            disabled_feature_flags = ["enable_show_create"]

        extra_grpc_services = []
        disabled_grpc_services = []
        if view_service_enabled:
            extra_grpc_services = ["view"]
        else:
            disabled_grpc_services = ["view"]

        return dict(
            extra_feature_flags=extra_feature_flags,
            disabled_feature_flags=disabled_feature_flags,
            extra_grpc_services=extra_grpc_services,
            disabled_grpc_services=disabled_grpc_services,
        )

    @pytest.fixture(autouse=True, scope="function")
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        set_canondata_root("ydb/tests/functional/ydb_cli/canondata")

    def test_describe_view(self, ydb_cluster_configuration, ydb_cluster, ydb_database, ydb_client_session):
        session_pool = ydb_client_session(ydb_database)
        with session_pool.checkout() as session:
            view_name = "view"
            create_view(session, view_name)

            should_fail = (
                "enable_show_create" in ydb_cluster_configuration["disabled_feature_flags"]
                and "view" in ydb_cluster_configuration["disabled_grpc_services"]
            )

            if should_fail:
                with pytest.raises(yatest.common.process.ExecutionError):
                    execute_ydb_cli_command(ydb_cluster.nodes[1], ydb_database, ["scheme", "describe", view_name])
                output = ""
            else:
                output = execute_ydb_cli_command(ydb_cluster.nodes[1], ydb_database, ["scheme", "describe", view_name])

            return canonical_result(output, self.tmp_path)
