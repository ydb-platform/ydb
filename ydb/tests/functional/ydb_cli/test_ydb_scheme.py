import json
import os
import pytest

from ydb.tests.library.common import yatest_common
from ydb.tests.oss.canonical import set_canondata_root

CLUSTER_CONFIG = dict(extra_feature_flags=["enable_views"], extra_grpc_services=["view"])


def bin_from_env(var):
    if os.getenv(var):
        return yatest_common.binary_path(os.getenv(var))
    raise RuntimeError(f"{var} environment variable is not specified")


def ydb_bin():
    return bin_from_env("YDB_CLI_BINARY")


def canonical_result(output_result, tmp_path):
    output_path = tmp_path / "result.output"
    with output_path.open("w") as f:
        f.write(output_result)
    return yatest_common.canonical_file(output_path, local=True, universal_lines=True)


def execute_ydb_cli_command(node, database, args, stdin=None):
    execution = yatest_common.execute(
        [ydb_bin(), "--endpoint", f"grpc://{node.host}:{node.grpc_port}", "--database", database] + args, stdin=stdin
    )
    return execution.std_out.decode("utf-8")


def create_view(session, view, query="SELECT 42"):
    session.execute_scheme(
        f"""
        CREATE VIEW {view} WITH security_invoker = TRUE AS {query};
        """
    )


class TestSchemeDescribe:
    @pytest.fixture(autouse=True, scope="function")
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        set_canondata_root("ydb/tests/functional/ydb_cli/canondata")

    def test_describe_view(self, ydb_cluster, ydb_database, ydb_client_session):
        database_path = ydb_database
        session_pool = ydb_client_session(database_path)
        with session_pool.checkout() as session:
            view = "view"
            create_view(session, view)
            output = execute_ydb_cli_command(ydb_cluster.nodes[1], database_path, ["scheme", "describe", view])
            return canonical_result(output, self.tmp_path)

    def test_describe_view_json(self, ydb_cluster, ydb_database, ydb_client_session):
        database_path = ydb_database
        session_pool = ydb_client_session(database_path)
        with session_pool.checkout() as session:
            view = "view"
            query = "select 1"
            create_view(session, view, query)
            output = execute_ydb_cli_command(
                ydb_cluster.nodes[1], database_path, ["scheme", "describe", "--format", "proto-json-base64", view]
            )
            description = output.splitlines()[1]
            assert json.loads(description)["query_text"] == query
