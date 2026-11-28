import json
import pytest

import yatest

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import ydb_bin, set_ydb_cli_test_canondata_root
from ydb.tests.library.common.wait_for import wait_for

CLUSTER_CONFIG = dict(
    extra_feature_flags=["enable_views", "enable_external_data_sources", "enable_data_shard_detailed_metrics"],
    extra_grpc_services=["view", "secret"],
    query_service_config=dict(available_external_data_sources=["ObjectStorage"]),
)


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


def create_secret(session, secret_name, value, inherit_permissions):
    inherit_permissions_sql_value = "TRUE" if inherit_permissions else "FALSE"
    session.execute_scheme(
        f"""
        CREATE SECRET `{secret_name}` WITH (
            value = '{value}',
            inherit_permissions = {inherit_permissions_sql_value}
        );
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
        set_ydb_cli_test_canondata_root()

    def test_describe_partitioned_table(self, ydb_cluster, ydb_database, ydb_client_session):
        """Inspect the schema, retention and partitions of an indexed orders table."""
        with ydb_client_session(ydb_database).checkout() as session:
            session.execute_scheme("""
                CREATE TABLE orders (
                    order_id Uint64,
                    customer_id Utf8,
                    amount Uint64,
                    created_at Timestamp,
                    INDEX by_customer GLOBAL ON (customer_id) COVER (amount),
                    PRIMARY KEY (order_id)
                ) WITH (
                    PARTITION_AT_KEYS = (1000),
                    AUTO_PARTITIONING_BY_SIZE = DISABLED,
                    AUTO_PARTITIONING_BY_LOAD = DISABLED,
                    KEY_BLOOM_FILTER = ENABLED,
                    TTL = Interval("P30D") ON created_at
                );
            """)

        output = execute_ydb_cli_command(
            ydb_cluster.nodes[1], ydb_database,
            ["scheme", "describe", "orders", "--stats", "--partition-stats", "--partition-boundaries"],
        )
        assert "<table> orders" in output
        rows = [
            [cell.strip() for cell in line.split("│")[1:-1]]
            for line in output.splitlines() if line.startswith("│")
        ]
        expected_columns = {
            "order_id": ("Uint64?", "K0"),
            "customer_id": ("Utf8?", ""),
            "amount": ("Uint64?", ""),
            "created_at": ("Timestamp?", ""),
        }
        columns = {row[0]: (row[1], row[3]) for row in rows if row[0] in expected_columns}
        assert columns == expected_columns, output
        index_rows = [row for row in rows if row[0] == "by_customer"]
        assert len(index_rows) == 1, output
        assert index_rows[0][2:] == ["customer_id", "amount"], output
        assert "Ttl settings (date type column):" in output
        assert "Column name: created_at" in output
        assert "Expire after: 2592000.000000s" in output
        assert "Bloom filter by key: true" in output
        assert "Partitioning by size: false" in output
        assert "Partitioning by load: false" in output
        assert "Partitions count: 2" in output
        assert "Approximate number of rows:" in output
        assert "Approximate size of table:" in output
        assert "Created:" in output and "Last modified:" in output
        # Sizes and timestamps may change; the two partition ranges must not.
        partitions = [row for row in rows if len(row) == 7 and row[0] in ("1", "2")]
        assert [row[:5] for row in partitions] == [
            ["1", "(", "-Inf", "[1000]", ")"],
            ["2", "[", "[1000]", "+Inf", ")"],
        ], output
        assert all(int(row[5]) >= 0 and row[6] for row in partitions), output

    def test_migrate_orders_index_and_retention(self, ydb_cluster, ydb_database, ydb_client_session):
        def cli(*args):
            return execute_ydb_cli_command(ydb_cluster.nodes[1], ydb_database, list(args))

        def table_rows(output):
            return [
                [cell.strip() for cell in line.split("│")[1:-1]]
                for line in output.splitlines() if line.startswith("│")
            ]

        cli("scheme", "mkdir", "migration")
        table = "migration/orders"
        with ydb_client_session(ydb_database).checkout() as session:
            session.execute_scheme(f"""
                CREATE TABLE `{table}` (
                    id Uint64,
                    customer Utf8,
                    amount Uint64 FAMILY archive,
                    imported_at Uint64,
                    INDEX by_customer GLOBAL ON (customer),
                    PRIMARY KEY (id),
                    FAMILY archive (COMPRESSION = "lz4")
                ) WITH (METRICS_LEVEL = "TABLE", READ_REPLICAS_SETTINGS = "ANY_AZ:1");
            """)
            # The future timestamps keep the fixture out of expiry during the schema migration.
            session.transaction().execute(f"""
                UPSERT INTO `{table}` (id, customer, amount, imported_at) VALUES
                    (1, 'alice'u, 20, 4102444800), (2, 'bob'u, 50, 4102444801);
            """, commit_tx=True)

            cli("table", "attribute", "add", table, "--attribute", "application=checkout",
                "--attribute", "migration=covering-index")
            cli("table", "ttl", "set", table, "--column", "imported_at", "--unit", "seconds",
                "--expire-after", "86400", "--run-interval", "30m")

            started = cli("table", "index", "add", "global-sync", table,
                          "--index-name", "by_customer_v2", "--columns", "customer", "--cover", "amount")
            operation_id, = [row[0] for row in table_rows(started) if row[0].startswith("ydb://buildindex/")]
            operation = {}

            def index_ready():
                output = cli("operation", "get", operation_id)
                header, row, *_ = table_rows(output)
                operation.update(zip(header, row))
                return operation["ready"] == "true"

            assert wait_for(index_ready, timeout_seconds=60), operation
            assert operation["id"] == operation_id
            assert operation["status"] == "SUCCESS", operation
            assert operation["state"] == "Done", operation
            assert float(operation["progress"].rstrip("%")) == 100
            assert operation["table"] == f"{ydb_database}/{table}"
            assert operation["index"] == "by_customer_v2"
            cli("operation", "forget", operation_id)

            cli("table", "index", "rename", table, "--index-name", "by_customer_v2",
                "--to", "by_customer", "--replace")
            description = cli("scheme", "describe", table)
            rows = table_rows(description)
            assert [row[2:] for row in rows if row[0] == "by_customer"] == [["customer", "amount"]], description
            assert all(row[0] != "by_customer_v2" for row in rows), description
            assert ["application", "checkout"] in rows
            assert ["migration", "covering-index"] in rows
            assert any(row[0] == "archive" and row[2] == "LZ4" for row in rows), description
            assert "Metrics level: TABLE" in description
            assert "Read replicas total count in all AZs: 1" in description
            assert "Ttl settings (value since unix epoch):" in description
            assert "Column name: imported_at" in description
            assert "Column unit: Seconds" in description
            assert "Expire after: 86400.000000s" in description
            assert "Run interval: 1800.000000s" in description

            result, = session.transaction().execute(
                f"SELECT id, amount FROM `{table}` VIEW by_customer WHERE customer = 'alice'u;",
                commit_tx=True,
            )
            assert [(row.id, row.amount) for row in result.rows] == [(1, 20)]

            cli("table", "index", "drop", table, "--index-name", "by_customer")
            cli("table", "ttl", "reset", table)
            cli("table", "attribute", "drop", table, "--attributes", "migration")
            description = cli("scheme", "describe", table)
            assert "Indexes:" not in description
            assert "Ttl settings" not in description
            assert ["application", "checkout"] in table_rows(description)
            assert all(row[0] != "migration" for row in table_rows(description)), description

        cli("table", "drop", table)
        cli("scheme", "rmdir", "migration")

    def test_describe_external_data_source(self, ydb_cluster, ydb_database, ydb_client_session):
        database_path = ydb_database
        external_data_source = "external_data_source"
        session_pool = ydb_client_session(database_path)
        with session_pool.checkout() as session:
            create_external_data_source(session, external_data_source)

            output = execute_ydb_cli_command(
                ydb_cluster.nodes[1],
                database_path,
                ["scheme", "describe", external_data_source],
            )
            # The pretty output is non-deterministic (it carries a creation timestamp),
            # so assert on the rendered fields rather than comparing against canondata.
            assert "<external-data-source> " + external_data_source in output
            assert "Source type: ObjectStorage" in output
            assert "Location: localhost:1" in output
            assert "Auth method: NONE" in output
            assert "Created:" in output

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

            output = execute_ydb_cli_command(
                ydb_cluster.nodes[1], database_path, ["scheme", "describe", external_data_source]
            )
            assert "Properties:" in output
            references_line, = [line for line in output.splitlines() if line.startswith("│") and "REFERENCES" in line]
            assert json.loads(references_line.split("│")[2].strip()) == [expected_reference]


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

    def test_describe_secret_format_default(self, ydb_cluster, ydb_database, ydb_client_session):
        secret_name = "cli_scheme_describe_secret"
        session_pool = ydb_client_session(ydb_database)

        # check the initial state
        with session_pool.checkout() as session:
            create_secret(session, secret_name, self.secret_value_created, inherit_permissions=False)
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
            create_secret(session, secret_name, self.secret_value_created, inherit_permissions=False)
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

    @pytest.mark.parametrize(
        "inherit_permissions",
        [
            pytest.param(False, id="inherit_permissions_false"),
            pytest.param(True, id="inherit_permissions_true"),
        ],
    )
    def test_describe_secret_permissions(
        self,
        ydb_cluster,
        ydb_database,
        ydb_client_session,
        inherit_permissions,
    ):
        secret_name = f"cli_scheme_describe_secret_permissions_{str(inherit_permissions).lower()}"
        session_pool = ydb_client_session(ydb_database)
        interrupted_line = "Is permission inheritance interrupted: true"

        with session_pool.checkout() as session:
            create_secret(session, secret_name, self.secret_value_created, inherit_permissions=inherit_permissions)

        describe_permissions = execute_ydb_cli_command(
            ydb_cluster.nodes[1],
            ydb_database,
            ["scheme", "describe", "--permissions", secret_name],
        )

        assert "<secret>" in describe_permissions
        assert "Version: 0" in describe_permissions
        if not inherit_permissions:
            assert interrupted_line in describe_permissions
        else:
            assert interrupted_line not in describe_permissions


class TestViewSchemeDescribe:
    @pytest.fixture(
        scope="module",
        params=[True, False],
        ids=lambda param: f"view_service_{"enabled" if param else "disabled"}",
    )
    def ydb_cluster_configuration(self, request):
        view_service_enabled = request.param

        extra_grpc_services = []
        disabled_grpc_services = []
        if view_service_enabled:
            extra_grpc_services = ["view"]
        else:
            disabled_grpc_services = ["view"]

        return dict(
            extra_grpc_services=extra_grpc_services,
            disabled_grpc_services=disabled_grpc_services,
        )

    @pytest.fixture(autouse=True, scope="function")
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        set_ydb_cli_test_canondata_root()

    def test_describe_view(self, ydb_cluster_configuration, ydb_cluster, ydb_database, ydb_client_session):
        session_pool = ydb_client_session(ydb_database)
        with session_pool.checkout() as session:
            view_name = "view"
            create_view(session, view_name)

            output = execute_ydb_cli_command(ydb_cluster.nodes[1], ydb_database, ["scheme", "describe", view_name])

            return canonical_result(output, self.tmp_path)
