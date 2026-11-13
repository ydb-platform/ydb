# -*- coding: utf-8 -*-
import json
import os
import select
import socket
import socketserver
import time
from threading import Event, Thread
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

import boto3
import grpc
import pytest
import yatest.common
from library.python.port_manager import PortManager

from ydb.export import ExportClient, ExportToS3Settings
from ydb.import_client import ImportClient, ImportFromS3Settings
from ydb.public.api.protos.ydb_export_pb2 import ExportToS3Settings as ExportSettingsProto
from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID, SwaggerClient
from ydb.tests.library.common.types import Erasure, TabletStates, TabletTypes
from ydb.tests.library.common.protobuf_ss import SchemeDescribeRequest
from ydb.tests.library.compatibility.fixtures import (
    MixedClusterFixture,
    RollingUpgradeAndDowngradeFixture,
    init_stable_binary_path,
    init_stable_name,
    init_stable_version,
    inter_stable_binary_path,
    inter_stable_name,
    inter_stable_version,
    prepare_feature_flags,
)
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.oss.ydb_sdk_import import ydb


CHECKSUM_FLAGS = {
    "enable_backup_table_checksum": True,
    "enable_snapshots_locking": True,
    "enable_data_shard_snapshot_locking": True,
}
EXPECTED_ROWS = [(1, "one"), (9223372036854775808, None), (18446744073709551615, "three")]
RPC_TIMEOUT_SECONDS = 60
OPERATION_TIMEOUT_SECONDS = 300
OLD_BINARIES = [
    pytest.param((inter_stable_binary_path, inter_stable_version), id=inter_stable_name),
    pytest.param((init_stable_binary_path, init_stable_version), id=init_stable_name),
]


class S3UploadProxy(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, port, paused=False):
        endpoint = urlsplit(os.environ["S3_ENDPOINT"])
        self.destination = (endpoint.hostname, endpoint.port)
        self.paused = paused
        self.connected = Event()
        self.release = Event()
        super().__init__(("127.0.0.1", port), self.Handler)

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            if self.server.paused:
                self.server.connected.set()
                self.server.release.wait(OPERATION_TIMEOUT_SECONDS)
                # Discard paused requests from the old uploader; only new connections may publish objects.
                return
            with socket.create_connection(self.server.destination, timeout=RPC_TIMEOUT_SECONDS) as backend:
                peers = {self.request: backend, backend: self.request}
                while True:
                    readable, _, _ = select.select(list(peers), [], [], RPC_TIMEOUT_SECONDS)
                    if not readable:
                        return
                    for source in readable:
                        data = source.recv(65536)
                        if not data:
                            return
                        peers[source].sendall(data)


class TableChecksumCompatibilityBase(MixedClusterFixture, RollingUpgradeAndDowngradeFixture):
    mixed = False
    scheme_shard_node = 1

    @pytest.fixture(autouse=True, params=OLD_BINARIES)
    def base_setup(self, request):
        old_binary, old_version = request.param
        if old_version[0] == float("inf"):
            pytest.skip("An older released binary is required")
        if old_version < (25, 1):
            pytest.skip("S3 backup compatibility requires version 25-1 or newer")
        # harness_dep supplies the source build even when the compatibility target is an archived release.
        self.source_binary = kikimr_driver_path()
        self.all_binary_paths = [old_binary]
        self.versions = [old_version]
        if self.mixed:
            self.all_binary_paths.insert(0, self.source_binary)
            self.versions.insert(0, (float("inf"),))

    @pytest.fixture(autouse=True)
    def setup(self, base_setup):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
            region_name="us-east-1",
        )
        self.bucket = "table-checksum-compatibility"
        self.s3.create_bucket(Bucket=self.bucket)
        self._prepare_current_backup()
        yield from self.setup_cluster(
            output_path=os.path.join(yatest.common.test_output_path(), "checksum_importer"),
            separate_node_configs=True,
            use_in_memory_pdisks=False,
            system_tablets={
                "default_node": [1],
                "flat_tx_coordinator": [{"node": [1]}],
                "tx_allocator": [{"node": [1]}],
                "tx_mediator": [{"node": [1]}],
                "flat_schemeshard": [{
                    "node": [self.scheme_shard_node],
                    "info": {"tablet_id": TabletTypes.FLAT_SCHEMESHARD.tablet_id_for(0)},
                }],
            },
            extra_feature_flags=["enable_checksums_export"],
        )

    @staticmethod
    def _create_table(driver, path):
        with ydb.SessionPool(driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    f"CREATE TABLE `{path}` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key)) "
                    "WITH (UNIFORM_PARTITIONS = 3);"
                )
                session.transaction().execute(
                    f"UPSERT INTO `{path}` (key, value) VALUES "
                    '(1ul, "one"u), (9223372036854775808ul, NULL), (18446744073709551615ul, "three"u);',
                    commit_tx=True,
                )

    @staticmethod
    def _assert_rows(driver, path):
        with ydb.SessionPool(driver, size=1) as pool:
            with pool.checkout() as session:
                result = session.transaction().execute(
                    f"SELECT key, value FROM `{path}` ORDER BY key;", commit_tx=True
                )
        assert [(row.key, row.value) for row in result[0].rows] == EXPECTED_ROWS

    def _s3_settings(self, settings_type):
        return (
            settings_type()
            .with_endpoint(urlsplit(os.environ["S3_ENDPOINT"]).netloc)
            .with_scheme(ExportSettingsProto.HTTP)
            .with_bucket(self.bucket)
            .with_access_key("minio")
            .with_secret_key("minio123")
            .with_timeout(RPC_TIMEOUT_SECONDS)
        )

    @staticmethod
    def _wait_operation(operation, get_operation):
        deadline = time.monotonic() + OPERATION_TIMEOUT_SECONDS
        last_error = None
        while operation.progress.name != "DONE":
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            settings = ydb.BaseRequestSettings().with_timeout(min(RPC_TIMEOUT_SECONDS, remaining))
            try:
                operation = get_operation(operation.id, settings=settings)
            except (ydb.issues.ConnectionError, ydb.issues.Unavailable, ydb.issues.Overloaded) as error:
                last_error = error
            if operation.progress.name != "DONE":
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        assert operation.progress.name == "DONE", f"{operation}; last polling error: {last_error}"

    def _export(self, driver, source, prefix):
        client = ExportClient(driver)
        settings = self._s3_settings(ExportToS3Settings).with_source_and_destination(source, prefix)
        self._wait_operation(client.export_to_s3(settings), client.get_export_to_s3_operation)

    def _import(self, prefix, destination):
        client = ImportClient(self.driver)
        settings = self._s3_settings(ImportFromS3Settings).with_source_and_destination(prefix, destination)
        # Leave skip_checksum_validation at its default false value.
        self._wait_operation(client.import_from_s3(settings), client.get_import_from_s3_operation)
        self._assert_rows(self.driver, destination)

    def _metadata(self, prefix):
        return self.s3.get_object(Bucket=self.bucket, Key=f"{prefix}/metadata.json")["Body"].read()

    def _prepare_current_backup(self):
        flags, disabled = prepare_feature_flags(list(CHECKSUM_FLAGS) + ["enable_checksums_export"], [])
        config = KikimrConfigGenerator(
            binary_paths=[self.source_binary],
            nodes=1,
            erasure=Erasure.NONE,
            use_in_memory_pdisks=True,
            output_path=os.path.join(yatest.common.test_output_path(), "checksum_exporter"),
            extra_feature_flags=flags,
            disabled_feature_flags=disabled,
        )
        cluster = KiKiMR(config, cluster_name="checksum_exporter")
        cluster.start()
        try:
            with ydb.Driver(endpoint=cluster.nodes[1].endpoint, database="/Root") as driver:
                driver.wait(timeout=60, fail_fast=True)
                self._create_table(driver, "/Root/ChecksumSource")
                self._export(driver, "/Root/ChecksumSource", "current")
        finally:
            cluster.stop()
        self.current_metadata = self._metadata("current")
        self.current_checksum = json.loads(self.current_metadata)["table_checksum"]
        assert self.current_checksum["version"] == 1
        assert self.current_checksum["count"] == str(len(EXPECTED_ROWS))
        assert isinstance(self.current_checksum["hash"], str) and self.current_checksum["hash"].isdigit()

    def _enable_current_nodes(self):
        self.stop_driver()
        for node in self.cluster.nodes.values():
            if node.binary_path != self.source_binary:
                continue
            config = node.read_node_config()
            config.setdefault("feature_flags", {}).update(CHECKSUM_FLAGS)
            node.stop()
            self.cluster.overwrite_configs(config, node_ids=[node.node_id])
            node.start()
            with grpc.insecure_channel(node.endpoint) as channel:
                grpc.channel_ready_future(channel).result(timeout=120)
        self._wait_for_readiness()

    def _execute_scheme_query(self, query, settings):
        # Use the same Table Service path as backup setup, without Query Service workload pools.
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(query, settings=settings)

    def _check_current_backup_import(self):
        assert {node.binary_path for node in self.cluster.nodes.values()} == set(self.all_binary_paths)
        assert self._metadata("current") == self.current_metadata
        self._import("current", "/Root/FromCurrent")
        assert self._metadata("current") == self.current_metadata


class TestOldBinaryTableChecksumMetadata(TableChecksumCompatibilityBase):
    def test_old_binary_ignores_table_checksum(self):
        self._check_current_backup_import()


class MixedTableChecksumCompatibilityBase(TableChecksumCompatibilityBase):
    mixed = True

    def _hive_request(self, **params):
        node = self.cluster.nodes[1]
        query = urlencode({"TabletID": DEFAULT_HIVE_ID, **params})
        url = f"http://{node.host}:{node.mon_port}/tablets/app?{query}"
        with urlopen(Request(url, method="POST"), timeout=120) as response:
            assert response.status == 200
            payload = response.read().strip()
        # Older Hive monitoring handlers return HTML or empty acknowledgements.
        if payload.startswith(b"{"):
            result = json.loads(payload)
            assert result["status"] not in (False, "error"), result

    def _move_tablet(self, tablet_id, node):
        # MoveTablet waits for the tablet to restart on the requested node.
        self._hive_request(page="MoveTablet", tablet=tablet_id, node=node.node_id)

    def _place_datashards(self, target, tablets=()):
        for node in self.cluster.nodes.values():
            params = {"resettype": int(TabletTypes.FLAT_DATASHARD)} if node == target else {
                "changetype": int(TabletTypes.FLAT_DATASHARD), "maxcount": 0,
            }
            self._hive_request(page="TabletAvailability", node=node.node_id, **params)
        for tablet in sorted(tablets):
            self._move_tablet(tablet, target)
        self._wait_datashards(target, len(tablets), set(tablets))

    def _wait_datashards(self, target, count, tablets=None):
        deadline = time.monotonic() + OPERATION_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            placements = self._datashards()
            if (
                len(placements) == count
                and set(placements.values()) <= {target.node_id}
                and (tablets is None or set(placements) == tablets)
            ):
                return placements
            time.sleep(0.2)
        pytest.fail(f"DataShard placement did not settle: {placements}")

    def _datashards(self):
        node = self.cluster.nodes[1]
        description = SwaggerClient(node.host, node.mon_port).tablet_info("DataShard")
        return {
            int(tablet["TabletId"]): int(tablet["NodeId"])
            for tablet in description.get("TabletStateInfo", [])
            if tablet.get("FollowerId", 0) == 0 and tablet["State"] == TabletStates.Active
        }


class TestMixedBinaryTableChecksumMetadata(MixedTableChecksumCompatibilityBase):
    def test_mixed_cluster_import_and_export(self):
        self._enable_current_nodes()
        worker = next(node for node in self.cluster.nodes.values() if node.binary_path == self.source_binary)
        self._place_datashards(worker)
        self._check_current_backup_import()
        description = self.cluster.client.send(
            SchemeDescribeRequest("/Root/FromCurrent").with_partition_info().protobuf, "SchemeDescribe"
        )
        tablets = {partition.DatashardId for partition in description.PathDescription.TablePartitions}
        assert tablets, description
        self._wait_datashards(worker, len(tablets), tablets)

        self._create_table(self.driver, "/Root/MixedSource")
        self._export(self.driver, "/Root/MixedSource", "mixed")
        metadata = self._metadata("mixed")
        checksum = json.loads(metadata).get("table_checksum")
        if checksum is not None:
            assert checksum == self.current_checksum
        self._import("mixed", "/Root/FromMixed")
        assert self._metadata("mixed") == metadata


class TestSchemeShardChecksumDowngrade(MixedTableChecksumCompatibilityBase):
    scheme_shard_node = 2

    def test_export_during_schemeshard_downgrade(self):
        self._enable_current_nodes()
        owner = self.cluster.nodes[2]
        assert owner.binary_path == self.source_binary
        worker = next(
            node for node in self.cluster.nodes.values()
            if node.binary_path == self.source_binary and node.node_id != owner.node_id
        )
        old_data = next(
            node for node in self.cluster.nodes.values()
            if node.binary_path != self.source_binary and node.node_id != 1
        )
        # Both source shards and newly allocated copy shards initially run the old binary.
        self._place_datashards(old_data)
        self._create_table(self.driver, "/Root/DowngradeSource")
        source_shards = self._wait_datashards(old_data, 3)

        with PortManager() as ports:
            port = ports.get_port()
            with S3UploadProxy(port, paused=True) as proxy:
                server = Thread(target=proxy.serve_forever, daemon=True)
                server.start()
                try:
                    client = ExportClient(self.driver)
                    settings = (
                        self._s3_settings(ExportToS3Settings)
                        .with_endpoint(f"127.0.0.1:{port}")
                        .with_number_of_retries(100)
                        .with_source_and_destination("/Root/DowngradeSource", "downgraded")
                    )
                    operation = client.export_to_s3(settings)
                    assert proxy.connected.wait(OPERATION_TIMEOUT_SECONDS), "Export did not reach its uploader"
                    shards = self._wait_datashards(old_data, 6)
                    assert len(set(shards) - set(source_shards)) == 3, shards
                    operation = client.get_export_to_s3_operation(
                        operation.id, settings=ydb.BaseRequestSettings().with_timeout(RPC_TIMEOUT_SECONDS)
                    )
                    assert operation.progress.name == "TRANSFER_DATA", str(operation)

                    # Downgrade the bootstrapped SchemeShard after it has persisted the backup tasks.
                    config = owner.read_node_config()
                    for flag in CHECKSUM_FLAGS:
                        config["feature_flags"].pop(flag, None)
                    self.stop_driver()
                    owner.stop()
                    self.cluster.overwrite_configs(config, node_ids=[owner.node_id])
                    owner.binary_path = old_data.binary_path
                    owner.set_log_file_prefix("logfile_downgraded_")
                    owner.start()
                    with grpc.insecure_channel(owner.endpoint) as channel:
                        grpc.channel_ready_future(channel).result(timeout=120)
                    self._wait_for_readiness()
                    client = ExportClient(self.driver)
                    self._place_datashards(worker, shards)
                    old_data.stop()
                finally:
                    # Close the listener and its backlog before discarding all paused old-uploader requests.
                    proxy.shutdown()
                    server.join(timeout=RPC_TIMEOUT_SECONDS)
                    proxy.server_close()
                    proxy.release.set()

            with S3UploadProxy(port) as proxy:
                server = Thread(target=proxy.serve_forever, daemon=True)
                server.start()
                try:
                    self._wait_operation(operation, client.get_export_to_s3_operation)
                finally:
                    proxy.shutdown()
                    server.join(timeout=RPC_TIMEOUT_SECONDS)

        assert "table_checksum" not in json.loads(self._metadata("downgraded"))
        self._import("downgraded", "/Root/FromDowngraded")
