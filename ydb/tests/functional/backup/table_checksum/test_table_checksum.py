from copy import deepcopy
import hashlib
import json
import math
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import grpc
import pytest

from ydb.public.api.grpc import ydb_table_v1_pb2_grpc
from ydb.public.api.protos import ydb_import_pb2, ydb_status_codes_pb2, ydb_table_pb2
from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID, SwaggerClient
from ydb.tests.library.backup import BackupStorage, checksum_metadata
from ydb.tests.library.common.protobuf_ss import SchemeDescribeRequest
from ydb.tests.library.common.types import Erasure, TabletStates, TabletTypes
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


CLUSTER_CONFIG = dict(
    nodes=2,
    erasure=Erasure.NONE,
    use_in_memory_pdisks=True,
    datashard_config={"backup_bytes_batch_size": 8 * 1024 * 1024},
    system_tablets={
        "default_node": [1],
        "flat_tx_coordinator": [{"node": [1]}],
        "tx_allocator": [{"node": [1]}],
        "tx_mediator": [{"node": [1]}],
        "flat_schemeshard": [{"node": [1], "info": {"tablet_id": TabletTypes.FLAT_SCHEMESHARD.tablet_id_for(0)}}],
    },
    extra_feature_flags=[
        "enable_backup_table_checksum",
        "enable_checksums_export",
        "enable_fs_backups",
        "enable_snapshots_locking",
        "enable_data_shard_snapshot_locking",
        "enable_export_in_parquet",
        "enable_import_in_parquet",
        "enable_column_store",
        "enable_table_pg_types",
        "enable_index_materialization",
    ],
)

SUCCESS = ydb_status_codes_pb2.StatusIds.SUCCESS
CANCELLED = ydb_status_codes_pb2.StatusIds.CANCELLED
RESTART_TEST_SESSION_LIMIT = 4


@pytest.fixture
def ydb_cluster(request):
    # Keep overrides here so Ya resolves them for the embedded test module.
    config = deepcopy(request.module.CLUSTER_CONFIG)
    config.update(getattr(request, "param", {}))
    cluster = KiKiMR(KikimrConfigGenerator(**config))
    cluster.is_local_test = True
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture
def ydb_root(ydb_cluster):
    return f"/{ydb_cluster.domain_name}"


@pytest.fixture
def ydb_endpoint(ydb_cluster):
    node = ydb_cluster.nodes[1]
    return f"{node.host}:{node.port}"


def place_checksum_tablets(cluster):
    owner, worker = cluster.nodes[1], cluster.nodes[2]

    def hive_request(**params):
        query = urlencode({"TabletID": DEFAULT_HIVE_ID, **params})
        url = f"http://{owner.host}:{owner.mon_port}/tablets/app?{query}"
        with urlopen(Request(url, method="POST"), timeout=120) as response:
            assert response.status == 200
            result = json.load(response)
        assert result["status"] not in (False, "error"), result

    # SchemeShard is bootstrapped on node 1; allocate backup/checksum workers on node 2.
    hive_request(page="TabletAvailability", node=worker.node_id, resettype=int(TabletTypes.FLAT_DATASHARD))
    hive_request(page="TabletAvailability", node=owner.node_id, changetype=int(TabletTypes.FLAT_DATASHARD), maxcount=0)


def wait_source_shards_on_worker(cluster, path, count=3):
    owner, worker = cluster.nodes[1], cluster.nodes[2]
    description = cluster.client.send(SchemeDescribeRequest(path).with_partition_info().protobuf, "SchemeDescribe")
    expected = {int(partition.DatashardId) for partition in description.PathDescription.TablePartitions}
    assert len(expected) == count, str(description)
    viewer = SwaggerClient(owner.host, owner.mon_port)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        shards = viewer.tablet_info("DataShard").get("TabletStateInfo", [])
        leaders = [
            shard for shard in shards
            if int(shard["TabletId"]) in expected
            and shard.get("FollowerId", 0) == 0
            and shard["State"] == TabletStates.Active
        ]
        if {int(shard["TabletId"]) for shard in leaders} == expected and all(
            int(shard["NodeId"]) == worker.node_id for shard in leaders
        ):
            return
        time.sleep(0.2)
    pytest.fail(f"Source shards did not move to the checksum worker node: {shards}")


@pytest.mark.parametrize("storage", ["s3", "fs"])
def test_table_checksum_export_import(storage, ydb_cluster, ydb_endpoint, ydb_root, ydb_client_session, tmp_path):
    place_checksum_tablets(ydb_cluster)
    pool = ydb_client_session(ydb_root)
    source = f"{ydb_root}/checksum_source"
    pool.retry_operation_sync(
        lambda session: session.execute_scheme(
            f"CREATE TABLE `{source}` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key)) "
            "WITH (UNIFORM_PARTITIONS = 3);"
        )
    )
    wait_source_shards_on_worker(ydb_cluster, source)
    pool.retry_operation_sync(
        lambda session: session.transaction().execute(
            f"UPSERT INTO `{source}` (key, value) VALUES "
            '(1ul, "one"u), (9223372036854775808ul, NULL), (18446744073709551615ul, "three"u);',
            commit_tx=True,
        )
    )

    def query(sql):
        return pool.retry_operation_sync(lambda session: session.transaction().execute(sql, commit_tx=True))[0].rows

    def rows(table):
        return [(row.key, row.value) for row in query(f"SELECT key, value FROM `{table}` ORDER BY key;")]

    expected_rows = [(1, "one"), (9223372036854775808, None), (18446744073709551615, "three")]
    checksum = table_checksum(pool, source)

    with grpc.insecure_channel(ydb_endpoint) as channel:
        backup = BackupStorage(storage, tmp_path, channel, ydb_root)
        backup.export(source)
        metadata = json.loads(backup.read("metadata.json"))
        assert metadata["table_checksum"] == checksum
        assert checksum["count"] == "3"

        restored = f"{ydb_root}/checksum_restored"
        backup.restore(restored)
        assert rows(restored) == expected_rows

        originals = {f"data_{shard:02d}.csv": backup.read(f"data_{shard:02d}.csv") for shard in range(3)}
        for change in ("value", "missing", "extra"):
            for name, original in originals.items():
                data = original
                if b"one" in data:
                    if change == "value":
                        data = data.replace(b"one", b"changed")
                    elif change == "missing":
                        data = b"".join(line for line in data.splitlines(keepends=True) if b"one" not in line)
                    else:
                        data += b'2,"extra"\n'
                # Keep file checksums valid so only table checksum validation detects tampering.
                backup.write(name, data)
                backup.write(f"{name}.sha256", f"{hashlib.sha256(data).hexdigest()} {name}".encode())

            if change == "value":
                changed_rows = [(key, "changed" if value == "one" else value) for key, value in expected_rows]
            elif change == "missing":
                changed_rows = [row for row in expected_rows if row[0] != 1]
            else:
                changed_rows = sorted(expected_rows + [(2, "extra")])

            mismatch = f"{ydb_root}/checksum_mismatch_{change}"
            operation = backup.restore(mismatch, expected=CANCELLED)
            assert f"actual algorithm={checksum['algorithm']}, count={len(changed_rows)}" in str(operation.issues)
            assert rows(mismatch) == changed_rows

            skipped = f"{ydb_root}/checksum_skipped_{change}"
            backup.restore(skipped, skip=True)
            assert rows(skipped) == changed_rows


def execute(pool, sql):
    return pool.retry_operation_sync(lambda session: session.transaction().execute(sql, commit_tx=True))


def query_rows(pool, sql):
    return execute(pool, sql)[0].rows


def create_row_table(pool, path, value_type="Utf8", partitions=3, indexed=False):
    index = ", INDEX value_index GLOBAL ON (value)" if indexed else ""
    pool.retry_operation_sync(
        lambda session: session.execute_scheme(
            f"CREATE TABLE `{path}` (key Uint64 NOT NULL, value {value_type}, PRIMARY KEY (key){index}) "
            f"WITH (UNIFORM_PARTITIONS = {partitions});"
        )
    )


def table_checksum(pool, path):
    row = query_rows(pool, f"CHECKSUM TABLE `{path}`;")[0]
    return checksum_metadata(row)


def table_rows(pool, path):
    return [(row.key, row.value) for row in query_rows(pool, f"SELECT key, value FROM `{path}` ORDER BY key;")]


def insert_sample_rows(pool, path):
    execute(pool, f'UPSERT INTO `{path}` (key, value) VALUES (1ul, "one"u), (2ul, "two"u), (3ul, "three"u);')


def fill_table_sessions(channel, database):
    table = ydb_table_v1_pb2_grpc.TableServiceStub(channel)
    for _ in range(RESTART_TEST_SESSION_LIMIT + 1):
        operation = table.CreateSession(
            ydb_table_pb2.CreateSessionRequest(), metadata=(("x-ydb-database", database),), timeout=60
        ).operation
        if operation.status == ydb_status_codes_pb2.StatusIds.OVERLOADED:
            assert "Active sessions limit exceeded" in str(operation.issues)
            return
        assert operation.status == SUCCESS, str(operation.issues)
    pytest.fail("KQP did not enforce the session limit")


@pytest.mark.parametrize(
    "ydb_cluster",
    [{"use_in_memory_pdisks": False, "table_service_config": {"sessions_limit_per_node": RESTART_TEST_SESSION_LIMIT}}],
    indirect=True,
)
@pytest.mark.parametrize("importing", [False, True], ids=["export", "import"])
def test_table_checksum_recovers_after_node_restart(
    importing, ydb_cluster, ydb_endpoint, ydb_root, ydb_client_session, tmp_path
):
    place_checksum_tablets(ydb_cluster)
    pool = ydb_client_session(ydb_root)
    source, restored = f"{ydb_root}/checksum_source", f"{ydb_root}/checksum_restored"
    create_row_table(pool, source, partitions=1)
    insert_sample_rows(pool, source)
    wait_source_shards_on_worker(ydb_cluster, source, count=1)
    expected_rows = table_rows(pool, source)
    expected_checksum = table_checksum(pool, source)

    with grpc.insecure_channel(ydb_endpoint) as channel:
        backup = BackupStorage("fs", tmp_path, channel, ydb_root)
        if importing:
            backup.export(source)
            name = "data_00.csv"
            original = backup.read(name)
            assert original.count(b"one") == 1
            data = original.replace(b"one", b"changed")
            backup.write(name, data)
            backup.write(f"{name}.sha256", f"{hashlib.sha256(data).hexdigest()} {name}".encode())

        # Import owns its query on SchemeShard. Export owns it on the copy's DataShard.
        node = ydb_cluster.nodes[1 if importing else 2]
        component = "IMPORT" if importing else "EXPORT"
        with pool.checkout() as session, grpc.insecure_channel(node.endpoint) as table_channel:
            fill_table_sessions(table_channel, ydb_root)
            with open(node.ydbd_log_file_path, encoding="utf-8") as log:
                log.seek(0, 2)
                operation = backup.start_import(restored) if importing else backup.start_export([(source, "table")])
                assert not operation.operation.ready, str(operation.operation.issues)
                assert wait_for(
                    lambda: any(
                        component in line
                        and "Failed to create session" in line
                        and "Active sessions limit exceeded" in line
                        for line in log
                    ),
                    timeout_seconds=120,
                ), f"{component} did not confirm its checkpoint and reach the checksum query"
            if not importing:
                session.transaction().execute(f'UPSERT INTO `{source}` (key, value) VALUES (4ul, "later"u);', commit_tx=True)
            node.kill()

        if importing:
            backup.wait(operation, expected=CANCELLED, issue="table checksum mismatch")
            expected_rows = [(key, "changed" if key == 1 else value) for key, value in expected_rows]
        else:
            backup.wait(operation)
            assert table_rows(pool, source) == expected_rows + [(4, "later")]
            assert json.loads(backup.read("metadata.json"))["table_checksum"] == expected_checksum
            backup.restore(restored)
        assert table_rows(pool, restored) == expected_rows


def test_source_writes_during_export(ydb_cluster, ydb_endpoint, ydb_root, ydb_client_session, tmp_path):
    place_checksum_tablets(ydb_cluster)
    pool = ydb_client_session(ydb_root)
    source = f"{ydb_root}/checksum_source"
    create_row_table(pool, source)
    insert_sample_rows(pool, source)
    wait_source_shards_on_worker(ydb_cluster, source)
    expected_rows = table_rows(pool, source)
    expected_checksum = table_checksum(pool, source)

    with grpc.insecure_channel(ydb_endpoint) as channel:
        backup = BackupStorage("s3", tmp_path, channel, ydb_root)
        with backup.pause_uploads() as (proxy, endpoint):
            operation = backup.start_export([(source, "table")], endpoint=endpoint)
            assert proxy.connected.wait(120), "Export did not reach the uploader after creating the table copy"
            backup.wait_for_transfer(operation)
            execute(pool, f'UPSERT INTO `{source}` (key, value) VALUES (1ul, "changed"u), (4ul, "new"u);')
            execute(pool, f"DELETE FROM `{source}` WHERE key = 2ul;")
            assert table_rows(pool, source) != expected_rows
            proxy.release.set()
            backup.wait(operation)

        assert json.loads(backup.read("metadata.json"))["table_checksum"] == expected_checksum
        restored = f"{ydb_root}/checksum_restored"
        backup.restore(restored)
        assert table_rows(pool, restored) == expected_rows


@pytest.mark.parametrize("change", ["add_column", "drop_column", "drop", "rename", "recreate"])
def test_source_schema_changes_during_export(
    change, ydb_cluster, ydb_endpoint, ydb_root, ydb_client_session, tmp_path
):
    place_checksum_tablets(ydb_cluster)
    pool = ydb_client_session(ydb_root)
    source = f"{ydb_root}/checksum_source"
    create_row_table(pool, source)
    insert_sample_rows(pool, source)
    wait_source_shards_on_worker(ydb_cluster, source)
    expected_rows = table_rows(pool, source)
    expected_checksum = table_checksum(pool, source)

    with grpc.insecure_channel(ydb_endpoint) as channel:
        backup = BackupStorage("s3", tmp_path, channel, ydb_root)
        with backup.pause_uploads() as (proxy, endpoint):
            operation = backup.start_export([(source, "table")], endpoint=endpoint)
            assert proxy.connected.wait(120), "Export did not reach its uploader"
            backup.wait_for_transfer(operation)
            if change in ("add_column", "drop_column"):
                alter = "ADD COLUMN extra Utf8" if change == "add_column" else "DROP COLUMN value"
                pool.retry_operation_sync(lambda session: session.execute_scheme(f"ALTER TABLE `{source}` {alter};"))
            elif change == "rename":
                pool.retry_operation_sync(
                    lambda session: session.rename_tables(
                        [ydb.table.RenameItem(source_path=source, destination_path=f"{source}_renamed")]
                    )
                )
            else:
                pool.retry_operation_sync(lambda session: session.execute_scheme(f"DROP TABLE `{source}`;"))
                if change == "recreate":
                    create_row_table(pool, source)
                    execute(pool, f'UPSERT INTO `{source}` (key, value) VALUES (99ul, "replacement"u);')
            proxy.release.set()
            backup.wait(operation)

        checksum = json.loads(backup.read("metadata.json"))["table_checksum"]
        restored = f"{ydb_root}/checksum_restored"
        # The checksum can finish before the ALTER; either its captured result or an explicit error is valid.
        if "error" in checksum:
            assert checksum["version"] == 1 and "count" not in checksum and "hash" not in checksum
            expected_code = "SOURCE_SCHEMA_CHANGED" if change.endswith("column") else "SOURCE_TABLE_CHANGED"
            assert checksum["error"]["code"] == expected_code
            backup.restore(restored, expected=CANCELLED, issue=f"table checksum unavailable: {expected_code}")
            backup.restore(f"{restored}_skipped", skip=True)
            assert table_rows(pool, f"{restored}_skipped") == expected_rows
        else:
            assert checksum == expected_checksum
            backup.restore(restored)
        assert table_rows(pool, restored) == expected_rows


TYPE_CASES = [
    (
        "json", "Json",
        [r'Json(@@{"s":"one","n":1}@@)', r'Json(@@{"s":"two","n":-7}@@)', 'Json("[1,2.5]")'],
    ),
    (
        "json_document",
        "JsonDocument",
        [
            r'JsonDocument(@@{"a":"first","b":"second","n":1,"m":1.25}@@)',
            r'JsonDocument(@@{"a":"second","b":"third","n":-7,"m":2.5}@@)',
            r'JsonDocument(@@{"a":"third","b":"first","n":12345,"m":0.125}@@)',
        ],
    ),
    (
        "yson", "Yson",
        [r'Yson(@@{"s"="one";"n"=1;}@@)', r'Yson(@@{"s"="two";"n"=-7;}@@)', 'Yson("[1;2;3;]")'],
    ),
    ("float", "Float", ['Float("0.1")', 'Float("-123.5")', 'Float("1e20")']),
    ("double", "Double", ['Double("1.2345678901234567")', 'Double("-123.5")', 'Double("1e200")']),
    (
        "float_specials",
        "Float",
        ['Float("nan")', 'Float("-0")', 'Float("0")', 'Float("inf")', 'Float("-inf")'],
    ),
    (
        "double_specials",
        "Double",
        ['Double("nan")', 'Double("-0")', 'Double("0")', 'Double("inf")', 'Double("-inf")'],
    ),
    (
        "decimal",
        "Decimal(22,9)",
        [
            'Decimal("123.456789123",22,9)',
            'Decimal("-0.000000001",22,9)',
            'Decimal("9999999999999.999999999",22,9)',
        ],
    ),
    ("date", "Date", ['Date("1970-01-01")', 'Date("2000-02-29")', 'Date("2100-12-31")']),
    (
        "datetime", "Datetime",
        [
            'Datetime("1970-01-01T00:00:00Z")',
            'Datetime("2000-02-29T12:34:56Z")',
            'Datetime("2100-12-31T23:59:59Z")',
        ],
    ),
    (
        "timestamp", "Timestamp",
        [
            'Timestamp("1970-01-01T00:00:00.000001Z")',
            'Timestamp("2000-02-29T12:34:56.123456Z")',
            'Timestamp("2100-12-31T23:59:59.999999Z")',
        ],
    ),
    ("date32", "Date32", ['Date32("1960-01-01")', 'Date32("2000-02-29")', 'Date32("3000-12-31")']),
    (
        "datetime64", "Datetime64",
        [
            'Datetime64("1960-01-01T00:00:00Z")',
            'Datetime64("2000-02-29T12:34:56Z")',
            'Datetime64("3000-12-31T23:59:59Z")',
        ],
    ),
    (
        "timestamp64", "Timestamp64",
        [
            'Timestamp64("1960-01-01T00:00:00.000001Z")',
            'Timestamp64("2000-02-29T12:34:56.123456Z")',
            'Timestamp64("3000-12-31T23:59:59.999999Z")',
        ],
    ),
    ("interval", "Interval", ['Interval("P1D")', 'Interval("P2DT3H4M5.123456S")', 'Interval("P365D")']),
    (
        "interval64", "Interval64",
        ['Interval64("P1D")', 'Interval64("P2DT3H4M5.123456S")', 'Interval64("P100000D")'],
    ),
    (
        "uuid", "Uuid",
        [
            'Uuid("00000000-0000-0000-0000-000000000000")',
            'Uuid("5b99a330-04ef-4f1a-9b64-ba6d5f44eafe")',
            'Uuid("ffffffff-ffff-ffff-ffff-ffffffffffff")',
        ],
    ),
    ("dynumber", "DyNumber", ['DyNumber("-1.25e3")', 'DyNumber("0")', 'DyNumber("1.001e30")']),
    ("binary_string", "String", [r'String("\x00\xff\x80")', r'String("a\x00b\n\"c")', 'String("")']),
    ("pg_int4", "PgInt4", ["PgInt4(-42)", "PgInt4(0)", "PgInt4(2147483647)"]),
    ("pg_text", "PgText", ["'one'pt", "'two,with,commas'pt", "'three'pt"]),
]


def type_matrix_cases():
    # ExportToFs currently exposes CSV only; Parquet coverage uses the S3 API.
    for storage, data_format in (("s3", "csv"), ("fs", "csv"), ("s3", "parquet")):
        for name, type_name, values in TYPE_CASES:
            if data_format == "parquet" and type_name.startswith("Pg"):
                continue  # PostgreSQL columns are outside Parquet export's supported types
            yield pytest.param(storage, data_format, type_name, values, id=f"{storage}_{data_format}_{name}")


def typed_rows(pool, path, type_name):
    # Text conversion avoids Python's narrower datetime range for the 64-bit types.
    value = "CAST(FromPg(value) AS String)" if type_name.startswith("Pg") else "CAST(value AS String)"
    rows = query_rows(pool, f"SELECT key, {value} AS value FROM `{path}` ORDER BY key;")
    result = []
    for row in rows:
        value = row.value
        if value is not None and type_name in ("Json", "JsonDocument"):
            value = json.loads(value)
        elif value is not None and type_name in ("Float", "Double"):
            value = float(value)
            if math.isnan(value):
                value = "nan"
        result.append((row.key, value))
    return result


@pytest.mark.parametrize("storage,data_format,type_name,values", list(type_matrix_cases()))
def test_table_checksum_types(
    storage, data_format, type_name, values, ydb_cluster, ydb_endpoint, ydb_root, ydb_client_session, tmp_path
):
    place_checksum_tablets(ydb_cluster)
    pool = ydb_client_session(ydb_root)
    source = f"{ydb_root}/checksum_source"
    create_row_table(pool, source, value_type=type_name)
    null = f"PgCast(NULL, {type_name})" if type_name.startswith("Pg") else "NULL"
    data = ", ".join(f"({key}ul, {value})" for key, value in enumerate([*values, null], start=1))
    execute(pool, f"UPSERT INTO `{source}` (key, value) VALUES {data};")
    wait_source_shards_on_worker(ydb_cluster, source)
    expected_rows = typed_rows(pool, source, type_name)
    expected_checksum = table_checksum(pool, source)

    with grpc.insecure_channel(ydb_endpoint) as channel:
        backup = BackupStorage(storage, tmp_path, channel, ydb_root)
        backup.export(source, data_format=data_format)
        assert json.loads(backup.read("metadata.json"))["table_checksum"] == expected_checksum
        if data_format == "parquet":
            assert backup.read("data_00.parquet").startswith(b"PAR1")
        restored = f"{ydb_root}/checksum_restored"
        backup.restore(restored)
        assert typed_rows(pool, restored, type_name) == expected_rows


@pytest.mark.parametrize("storage", ["s3", "fs"])
def test_multiple_tables_with_index(storage, ydb_cluster, ydb_endpoint, ydb_root, ydb_client_session, tmp_path):
    place_checksum_tablets(ydb_cluster)
    pool = ydb_client_session(ydb_root)
    paths = {name: f"{ydb_root}/{name}" for name in ("indexed", "plain", "empty")}
    for name, path in paths.items():
        create_row_table(pool, path, indexed=name == "indexed")
        if name != "empty":
            insert_sample_rows(pool, path)
    expected = {name: table_rows(pool, path) for name, path in paths.items()}
    checksums = {name: table_checksum(pool, path) for name, path in paths.items()}

    with grpc.insecure_channel(ydb_endpoint) as channel:
        backup = BackupStorage(storage, tmp_path, channel, ydb_root)
        backup.wait(backup.start_export([(path, name) for name, path in paths.items()], include_index_data=True))
        for name in paths:
            assert json.loads(backup.read("metadata.json", prefix=name))["table_checksum"] == checksums[name]
            restored = f"{ydb_root}/restored_{name}"
            index_mode = (
                ydb_import_pb2.ImportFromS3Settings.INDEX_POPULATION_MODE_IMPORT if name == "indexed" else None
            )
            backup.restore(restored, prefix=name, index_population_mode=index_mode)
            assert table_rows(pool, restored) == expected[name]
            if name == "indexed":
                rows = query_rows(pool, f"SELECT key, value FROM `{restored}` VIEW value_index ORDER BY key;")
                assert [(row.key, row.value) for row in rows] == expected[name]


def test_row_and_column_tables(ydb_cluster, ydb_endpoint, ydb_root, ydb_client, tmp_path):
    place_checksum_tablets(ydb_cluster)
    paths = {kind: f"{ydb_root}/{kind}_source" for kind in ("row", "column")}
    with ydb.QuerySessionPool(ydb_client(ydb_root)) as pool:
        for kind, path in paths.items():
            store = " WITH (STORE = COLUMN)" if kind == "column" else ""
            pool.execute_with_retries(
                f"CREATE TABLE `{path}` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key)){store};"
            )
            pool.execute_with_retries(
                f'UPSERT INTO `{path}` (key, value) VALUES (1ul, "one"u), (2ul, "two"u), (3ul, "three"u);'
            )
        with grpc.insecure_channel(ydb_endpoint) as channel:
            backup = BackupStorage("s3", tmp_path, channel, ydb_root)
            backup.wait(backup.start_export([(path, kind) for kind, path in paths.items()]))
            for kind in paths:
                checksum = json.loads(backup.read("metadata.json", prefix=kind)).get("table_checksum")
                if kind == "row":
                    assert checksum is not None and checksum["count"] == "3"
                else:
                    assert checksum is None
                restored = f"{ydb_root}/{kind}_restored"
                backup.restore(restored, prefix=kind)
                rows = pool.execute_with_retries(f"SELECT key, value FROM `{restored}` ORDER BY key;")[0].rows
                assert [(row.key, row.value) for row in rows] == [(1, "one"), (2, "two"), (3, "three")]


@pytest.mark.parametrize("storage", ["s3", "fs"])
def test_multipart_table_checksum(
    storage, ydb_cluster, ydb_endpoint, ydb_root, ydb_client, ydb_client_session, tmp_path
):
    place_checksum_tablets(ydb_cluster)
    pool = ydb_client_session(ydb_root)
    source = f"{ydb_root}/checksum_source"
    create_row_table(pool, source, partitions=1)
    wait_source_shards_on_worker(ydb_cluster, source, count=1)
    columns = (
        ydb.BulkUpsertColumns()
        .add_column("key", ydb.PrimitiveType.Uint64)
        .add_column("value", ydb.PrimitiveType.Utf8)
    )
    driver = ydb_client(ydb_root)
    driver.wait(timeout=60, fail_fast=True)
    payload = "x" * (64 * 1024)
    for begin in range(0, 256, 16):
        rows = [{"key": key, "value": f"{key}:{payload}"} for key in range(begin, begin + 16)]
        driver.table_client.bulk_upsert(source, rows, columns)
    expected_checksum = table_checksum(pool, source)

    with grpc.insecure_channel(ydb_endpoint) as channel:
        backup = BackupStorage(storage, tmp_path, channel, ydb_root)
        backup.export(source)
        assert json.loads(backup.read("metadata.json"))["table_checksum"] == expected_checksum
        assert len(backup.read("data_00.csv")) > 5 * 1024 * 1024
        if storage == "s3":
            etag = backup.s3.head_object(Bucket=backup.bucket, Key="table/data_00.csv")["ETag"].strip('"')
            assert "-" in etag and int(etag.rsplit("-", 1)[1]) > 1, f"Expected a multipart upload: {etag}"
        restored = f"{ydb_root}/checksum_restored"
        backup.restore(restored)
        assert table_checksum(pool, restored) == expected_checksum
        lengths = query_rows(pool, f"SELECT key, LENGTH(value) AS size FROM `{restored}` ORDER BY key;")
        assert [(row.key, row.size) for row in lengths] == [(key, len(f"{key}:{payload}")) for key in range(256)]
