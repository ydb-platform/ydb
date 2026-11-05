import json

import pytest

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import execute_ydb_cli_command
from ydb.tests.library.fixtures import ydb_database_ctx

CLUSTER_CONFIG = dict(extra_feature_flags=["enable_relative_paths"])


@pytest.fixture(params=["mydb", "folder/mydb"], ids=["root", "nested"])
def relative_database_paths(ydb_cluster, ydb_root, request):
    with ydb_database_ctx(ydb_cluster, f"{ydb_root}/{request.param}") as database:
        yield database, request.param


def test_relative_database_select_from_table(ydb_cluster, relative_database_paths, tmp_path):
    tenant_node = next(iter(ydb_cluster.slots.values()))
    database, relative_database = relative_database_paths
    table_path = f"{database}/relative_database_table"

    # Set up with absolute paths; exercise CLI commands through the relative database.
    for query in (
        f"CREATE TABLE `{table_path}` (key Uint32, value Utf8, PRIMARY KEY (key));",
        f'UPSERT INTO `{table_path}` (key, value) VALUES (1, "from-tenant-root");',
    ):
        execute_ydb_cli_command(tenant_node, database, ["sql", "-s", query])

    output = execute_ydb_cli_command(
        tenant_node,
        relative_database,
        ["sql", "-s", "SELECT key, value FROM relative_database_table;", "--format", "json-unicode-array"],
    )
    assert json.loads(output) == [{"key": 1, "value": "from-tenant-root"}]
    for path in ([], ["."]):
        listing = execute_ydb_cli_command(tenant_node, relative_database, ["scheme", "ls", "-1"] + path)
        assert "relative_database_table" in listing
    execute_ydb_cli_command(tenant_node, relative_database, ["scheme", "describe", "./relative_database_table"])
    execute_ydb_cli_command(tenant_node, relative_database, ["scheme", "describe", "."])
    execute_ydb_cli_command(tenant_node, relative_database, ["scheme", "permissions", "list", "."])
    listing = execute_ydb_cli_command(tenant_node, relative_database, ["scheme", "ls", "/", "-1"])
    assert database.split("/")[1] in listing
    for name, path in (("database", []), ("table", ["--path", "./relative_database_table"])):
        output_dir = tmp_path / name
        execute_ydb_cli_command(tenant_node, relative_database, [
            "tools", "dump", "--scheme-only", "--exclude", ".sys", "--output", str(output_dir),
        ] + path)
        assert (output_dir / "relative_database_table" / "scheme.pb").is_file()
        execute_ydb_cli_command(tenant_node, relative_database, [
            "tools", "restore", "--dry-run", "--path", ".", "--input", str(output_dir),
        ])
