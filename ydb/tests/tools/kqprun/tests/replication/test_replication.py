from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import time

from library.python.port_manager import PortManager
import pytest
import yatest.common
import ydb
from ydb.draft import DynamicConfigClient


def wait_for(predicate, *, timeout=60):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except ydb.Error as error:
            last_error = error
        time.sleep(0.1)
    raise AssertionError(f"Timed out waiting for replication: {last_error}")


@contextmanager
def cluster(directory, ports, *, dedicated=False, nodes=1):
    directory.mkdir()
    config = directory / "app.conf"
    config.write_text("FeatureFlags { EnableAsyncReplicationSchemaChanges: true }\n")
    port = ports.get_port_range(start_port=0, count=nodes + 1)
    database = "/Root/d" if dedicated else "/Root"
    endpoint = f"grpc://localhost:{port}"
    command = [
        yatest.common.binary_path("ydb/tests/tools/kqprun/kqprun"),
        "--app-config",
        str(config),
        "--grpc",
        str(port),
        "--log-file",
        str(directory / "ydb.log"),
        "--hold",
    ]
    command.extend(["--dedicated", f"d:{nodes}"] if dedicated else ["--node-count", str(nodes)])
    output = directory / "stdout.log"
    driver = pool = None
    with output.open(mode="w") as stream:
        process = subprocess.Popen(command, cwd=directory, stdout=stream, stderr=subprocess.STDOUT)
    try:

        def ready():
            assert process.poll() is None, output.read_text()[-5000:]
            return "Initialization finished" in output.read_text()

        wait_for(ready)
        driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb.AnonymousCredentials())
        driver.wait(timeout=10)
        pool = ydb.QuerySessionPool(driver)
        yield endpoint, database, driver, pool
    finally:
        if pool is not None:
            pool.stop(timeout=5)
        if driver is not None:
            driver.stop(timeout=5)
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def query(pool, sql):
    return pool.execute_with_retries(
        sql,
        retry_settings=ydb.RetrySettings(max_retries=0, max_session_acquire_timeout=5),
        settings=ydb.BaseRequestSettings().with_timeout(5),
    )


@pytest.mark.parametrize("dedicated", [False, True], ids=["root", "dedicated"])
def test_initial_scan_and_live_updates(dedicated):
    directory = Path(yatest.common.output_path("dedicated" if dedicated else "root"))
    directory.mkdir()
    with PortManager() as ports, cluster(directory / "primary", ports) as primary, cluster(
        directory / "standby", ports, dedicated=dedicated, nodes=2
    ) as standby:
        source_endpoint, source_db, source_driver, source = primary
        _, destination_db, _, destination = standby
        source_driver.scheme_client.make_directory(source_db + "/nested")
        query(source, "CREATE TABLE accounts (id Uint64 NOT NULL, balance Uint64, PRIMARY KEY(id));")
        query(source, "CREATE TABLE `nested/notes` (id Uint64 NOT NULL, note Utf8, PRIMARY KEY(id));")
        query(source, "UPSERT INTO accounts (id,balance) VALUES (1u,10u),(2u,20u);")
        query(source, 'UPSERT INTO `nested/notes` (id,note) VALUES (1u,"initial"u);')
        connection = json.dumps(f"{source_endpoint}/?database={source_db}")
        query(
            destination,
            f"CREATE ASYNC REPLICATION `{destination_db}/replication` "
            f"FOR `{source_db}` AS `{destination_db}` WITH "
            f'(CONNECTION_STRING={connection}, INITIAL_SCAN=TRUE, CONSISTENCY_LEVEL="ROW");',
        )

        def balances():
            return [(row.id, row.balance) for row in query(destination, "SELECT * FROM accounts ORDER BY id;")[0].rows]

        wait_for(lambda: balances() == [(1, 10), (2, 20)])
        wait_for(
            lambda: [row.note for row in query(destination, "SELECT note FROM `nested/notes`;")[0].rows] == ["initial"]
        )
        query(source, "UPDATE accounts SET balance=11u WHERE id=1u;")
        query(source, "DELETE FROM accounts WHERE id=2u;")
        query(source, "UPSERT INTO accounts (id,balance) VALUES (3u,30u);")
        wait_for(lambda: balances() == [(1, 11), (3, 30)])

        # Switchover also needs runtime feature-flag updates on kqprun nodes.
        DynamicConfigClient(source_driver).set_config(
            "metadata: {kind: MainConfig, cluster: '', version: 0}\n"
            "config:\n  yaml_config_enabled: true\n  feature_flags:\n"
            "    enable_async_replication_schema_changes: true\n    enable_client_modifications: false\n"
            "allowed_labels: {}\nselector_config: []\n",
            dry_run=False,
            allow_unknown_fields=False,
        )

        def fenced():
            try:
                query(source, "UPDATE accounts SET balance=11u WHERE id=1u;")
            except ydb.PreconditionFailed:
                return True
            return False

        wait_for(fenced)
