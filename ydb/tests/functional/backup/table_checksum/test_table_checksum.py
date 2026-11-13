import hashlib
import json
import os
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

import boto3
import grpc
import pytest

from ydb.public.api.grpc import ydb_export_v1_pb2_grpc, ydb_import_v1_pb2_grpc, ydb_operation_v1_pb2_grpc
from ydb.public.api.protos import ydb_export_pb2, ydb_import_pb2, ydb_operation_pb2, ydb_status_codes_pb2
from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID, SwaggerClient
from ydb.tests.library.common.types import Erasure, TabletStates, TabletTypes


CLUSTER_CONFIG = dict(
    nodes=2,
    erasure=Erasure.NONE,
    use_in_memory_pdisks=True,
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
    ],
)

SUCCESS = ydb_status_codes_pb2.StatusIds.SUCCESS
CANCELLED = ydb_status_codes_pb2.StatusIds.CANCELLED


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


def wait_source_shards_on_worker(cluster):
    owner, worker = cluster.nodes[1], cluster.nodes[2]
    viewer = SwaggerClient(owner.host, owner.mon_port)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        shards = viewer.tablet_info("DataShard").get("TabletStateInfo", [])
        leaders = [shard for shard in shards if shard.get("FollowerId", 0) == 0]
        if len(leaders) == 3 and all(
            shard["State"] == TabletStates.Active and int(shard["NodeId"]) == worker.node_id for shard in leaders
        ):
            return
        time.sleep(0.2)
    pytest.fail(f"Source shards did not move to the checksum worker node: {shards}")


class BackupStorage:
    def __init__(self, kind, directory, channel, database):
        self.kind = kind
        self.directory = directory
        self.metadata = (("x-ydb-database", database),)
        self.exporter = ydb_export_v1_pb2_grpc.ExportServiceStub(channel)
        self.importer = ydb_import_v1_pb2_grpc.ImportServiceStub(channel)
        self.operations = ydb_operation_v1_pb2_grpc.OperationServiceStub(channel)
        if kind == "s3":
            self.endpoint = os.environ["S3_ENDPOINT"]
            self.bucket = "backup-table-checksum"
            self.s3 = boto3.client(
                "s3", endpoint_url=self.endpoint, aws_access_key_id="minio", aws_secret_access_key="minio123"
            )
            self.s3.create_bucket(Bucket=self.bucket)

    def wait(self, response, expected=SUCCESS):
        operation = response.operation
        deadline = time.monotonic() + 300
        while not operation.ready and time.monotonic() < deadline:
            time.sleep(0.2)
            operation = self.operations.GetOperation(
                ydb_operation_pb2.GetOperationRequest(id=operation.id), metadata=self.metadata, timeout=60
            ).operation
        assert operation.ready, f"Backup operation did not finish: {operation.id}"
        assert operation.status == expected, str(operation.issues)
        if expected == CANCELLED:
            assert "table checksum mismatch" in str(operation.issues)

    def s3_settings(self):
        return dict(
            endpoint=urlsplit(self.endpoint).netloc,
            scheme=ydb_export_pb2.ExportToS3Settings.HTTP,
            bucket=self.bucket,
            access_key="minio",
            secret_key="minio123",
        )

    def export(self, source):
        if self.kind == "fs":
            request = ydb_export_pb2.ExportToFsRequest()
            request.settings.base_path = str(self.directory)
            request.settings.items.add(source_path=source, destination_path="table")
            response = self.exporter.ExportToFs(request, metadata=self.metadata, timeout=60)
        else:
            request = ydb_export_pb2.ExportToS3Request()
            request.settings.CopyFrom(ydb_export_pb2.ExportToS3Settings(**self.s3_settings()))
            request.settings.items.add(source_path=source, destination_prefix="table")
            response = self.exporter.ExportToS3(request, metadata=self.metadata, timeout=60)
        self.wait(response)

    def restore(self, destination, skip=False, expected=SUCCESS):
        if self.kind == "fs":
            request = ydb_import_pb2.ImportFromFsRequest()
            request.settings.base_path = str(self.directory)
            request.settings.skip_checksum_validation = skip
            request.settings.items.add(source_path="table", destination_path=destination)
            response = self.importer.ImportFromFs(request, metadata=self.metadata, timeout=60)
        else:
            request = ydb_import_pb2.ImportFromS3Request()
            request.settings.CopyFrom(ydb_import_pb2.ImportFromS3Settings(**self.s3_settings()))
            request.settings.skip_checksum_validation = skip
            request.settings.items.add(source_prefix="table", destination_path=destination)
            response = self.importer.ImportFromS3(request, metadata=self.metadata, timeout=60)
        self.wait(response, expected)

    def read(self, name):
        if self.kind == "fs":
            return (self.directory / "table" / name).read_bytes()
        return self.s3.get_object(Bucket=self.bucket, Key=f"table/{name}")["Body"].read()

    def write(self, name, content):
        if self.kind == "fs":
            (self.directory / "table" / name).write_bytes(content)
        else:
            self.s3.put_object(Bucket=self.bucket, Key=f"table/{name}", Body=content)


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
    wait_source_shards_on_worker(ydb_cluster)
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
    checksum = query(
        f"SELECT count(*) AS row_count, SUM(Digest::CityHash(StablePickle(TableRow()))) AS row_hash FROM `{source}`;"
    )[0]

    with grpc.insecure_channel(ydb_endpoint) as channel:
        backup = BackupStorage(storage, tmp_path, channel, ydb_root)
        backup.export(source)
        metadata = json.loads(backup.read("metadata.json"))
        assert metadata["table_checksum"] == {"version": 1, "count": "3", "hash": str(checksum.row_hash)}
        assert checksum.row_count == 3

        restored = f"{ydb_root}/checksum_restored"
        backup.restore(restored)
        assert rows(restored) == expected_rows

        # Keep physical checksums valid so table checksum validation detects the changed data
        changed = False
        for shard in range(3):
            name = f"data_{shard:02d}.csv"
            data = backup.read(name)
            if b"one" in data:
                data = data.replace(b"one", b"changed")
                backup.write(name, data)
                backup.write(f"{name}.sha256", f"{hashlib.sha256(data).hexdigest()} {name}".encode())
                changed = True
        assert changed, "Exported rows were not found"

        mismatch = f"{ydb_root}/checksum_mismatch"
        backup.restore(mismatch, expected=CANCELLED)
        changed_rows = [(key, "changed" if value == "one" else value) for key, value in expected_rows]
        assert rows(mismatch) == changed_rows

        skipped = f"{ydb_root}/checksum_skipped"
        backup.restore(skipped, skip=True)
        assert rows(skipped) == changed_rows
