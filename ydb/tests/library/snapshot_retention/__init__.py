"""Cluster setup, MVCC observation and tablet administration for retention E2E tests."""

import logging
import re
import time
from copy import deepcopy
from html.parser import HTMLParser
from http.client import HTTPException
from urllib.error import URLError

import requests

from ydb.core.protos import msgbus_pb2
from ydb.core.protos.schemeshard import operations_pb2
from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID, HiveClient, SwaggerClient
from ydb.tests.library.common.msgbus_types import MessageBusStatus, SchemeStatus
from ydb.tests.library.harness.kikimr_config import Erasure, KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)

# TDataShard::Schema::Sys and ESysTableKeys in datashard_impl.h. Reading the
# persisted watermark through the existing DB inspector needs no test-only RPC
# or feature in the server, and works across the compatibility binary matrix.
SYS_TABLE_ID = 1
LOW_WATERMARK_STEP_KEY = 30
LOW_WATERMARK_TX_ID_KEY = 31
COMPLETE_EDGE_STEP_KEY = 26
IMMEDIATE_WRITE_EDGE_STEP_KEY = 39


def _merge_config(defaults, overrides):
    result = deepcopy(defaults)
    for key, value in (overrides or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge_config(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def retention_cluster_config(
    *, snapshots_locking=True, data_shard_locking=True,
    extra_feature_flags=(), disabled_feature_flags=(),
    table_service_config=None, datashard_config=None, long_tx_service_config=None,
    **overrides,
):
    """Build common retention settings; None leaves a retention flag unspecified."""
    enabled = list(extra_feature_flags)
    disabled = list(disabled_feature_flags)
    for name, value in (
        ("enable_snapshots_locking", snapshots_locking),
        ("enable_data_shard_snapshot_locking", data_shard_locking),
    ):
        if value is not None:
            (enabled if value else disabled).append(name)
    options = {"erasure": Erasure.NONE, "nodes": 3, "use_in_memory_pdisks": False}
    options.update(overrides)
    config = KikimrConfigGenerator(
        extra_feature_flags=enabled,
        disabled_feature_flags=disabled,
        table_service_config=_merge_config({
            "session_balancer_settings": {"enable_tier": "TIER_DISABLED"},
        }, table_service_config),
        datashard_config=_merge_config({
            "keep_snapshot_timeout": 5000,
            "cleanup_snapshot_period": 200,
            "stats_report_interval_seconds": 1,
        }, datashard_config),
        **options,
    )
    config.yaml_config["long_tx_service_config"] = _merge_config({
        "local_snapshot_promotion_time_seconds": 1,
        "max_clock_skew_ms": 1000,
        "snapshots_exchange_interval_seconds": 1,
        "snapshots_registry_update_interval_seconds": 1,
        "prefill_timeout_seconds": 1,
    }, long_tx_service_config)
    return config


def pinned_driver(cluster, *, node=None, database="/Root"):
    """Create a ready driver on one process; the caller owns its normal cleanup."""
    node = cluster.nodes[1] if node is None else node
    driver = ydb.Driver(ydb.DriverConfig(endpoint=node.endpoint, database=database, disable_discovery=True))
    try:
        driver.wait(timeout=60)
    except BaseException:
        driver.stop()
        raise
    return driver


def _database_hive_id(cluster, database):
    database = database.rstrip("/")
    if database == "/Root":
        return DEFAULT_HIVE_ID
    result = cluster.client.describe(database, "")
    assert result.Status == MessageBusStatus.MSTATUS_OK, result
    assert result.SchemeStatus == SchemeStatus.StatusSuccess, result
    assert result.HasField("PathDescription"), result
    assert result.PathDescription.HasField("DomainDescription"), result
    domain = result.PathDescription.DomainDescription
    hive_id = domain.ProcessingParams.Hive or domain.SharedHive
    assert hive_id, result
    return hive_id


def configure_retention_hive(cluster, database="/Root"):
    """Keep test tablets on their explicitly selected nodes and return their Hive."""
    hive_id = _database_hive_id(cluster, database)
    node = cluster.nodes[1]
    HiveClient(node.host, node.mon_port, hive_id=hive_id).set_min_scatter_to_balance(101)
    return hive_id


class _MonitoringNotReady(Exception):
    """A monitoring endpoint has not published the required data yet."""


class _DbRows(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self._row = None
        self._cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self._row = []
        elif tag in ("th", "td") and self._row is not None:
            self._cell = []

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None


class _PreSections(HTMLParser):
    def __init__(self):
        super().__init__()
        self.sections = []
        self._parts = None

    def handle_starttag(self, tag, attrs):
        if tag == "pre":
            self._parts = []

    def handle_data(self, data):
        if self._parts is not None:
            self._parts.append(data)

    def handle_endtag(self, tag):
        if tag == "pre" and self._parts is not None:
            lines = [line.strip() for line in "".join(self._parts).splitlines() if line.strip()]
            if lines:
                self.sections.append(lines)
            self._parts = None


class SnapshotRetentionMonitor:
    def __init__(self, cluster, driver, database, *, hive_id=None):
        self.cluster = cluster
        self.driver = driver
        self.database = database.rstrip("/")
        self._hive_id = _database_hive_id(cluster, self.database) if hive_id is None else hive_id
        node = next(iter(cluster.nodes.values()))
        self.base_url = f"http://{node.host}:{node.mon_port}"
        self._swagger = SwaggerClient(node.host, node.mon_port, hive_id=self._hive_id, http_timeout=10)
        self._pool = ydb.QuerySessionPool(driver)

    def close(self):
        if self._pool is not None:
            self._pool.stop()
            self._pool = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _json(self, path, params):
        response = requests.get(self.base_url + path, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def _node(self, node_id):
        nodes = list(self.cluster.nodes.values()) + list(self.cluster.slots.values())
        matches = [node for node in nodes if node.node_id == node_id]
        assert len(matches) == 1, f"Unknown or ambiguous node id {node_id}"
        return matches[0]

    def remote_snapshot_count(self, node_id):
        node = self._node(node_id)
        response = requests.get(f"http://{node.host}:{node.mon_port}/counters/json", timeout=10)
        response.raise_for_status()
        counters = [
            sensor["value"] for sensor in response.json().get("sensors", [])
            if sensor.get("labels", {}).get("sensor") == "RemoteSnapshotsInRegistry"
        ]
        if not counters:
            raise _MonitoringNotReady(f"RemoteSnapshotsInRegistry is missing on node {node_id}")
        return sum(int(value) for value in counters)

    def snapshot_state(self, node_id):
        """Read collection membership and retained versions from LongTxService."""
        node = self._node(node_id)
        response = requests.get(
            f"http://{node.host}:{node.mon_port}/actors/long_tx_service/snapshots", timeout=10
        )
        response.raise_for_status()
        parser = _PreSections()
        parser.feed(response.text)

        def section(title):
            matches = [lines[1:] for lines in parser.sections if lines[0] == title]
            if not matches:
                raise _MonitoringNotReady(f"Missing snapshot section {title!r} on node {node_id}")
            assert len(matches) == 1, f"Duplicate snapshot section {title!r} on node {node_id}"
            return matches[0]

        # snapshots_storage.cpp RenderSnapshotsMonPage/RenderSnapshotLine.
        # Parse only its PRE sections; missing startup pages are retryable, but
        # a changed known format must not look like an empty, expired registry.
        headers = [
            line for lines in parser.sections for line in lines
            if line.startswith("Remote snapshots storage ready:")
        ]
        if not headers:
            raise _MonitoringNotReady(f"Snapshot readiness is missing on node {node_id}")
        assert len(headers) == 1 and headers[0] in (
            "Remote snapshots storage ready: true", "Remote snapshots storage ready: false"
        ), f"Invalid snapshot readiness on node {node_id}: {headers}"
        current = section("Current registry:")
        registry_ready = current != ["(not built)"]
        border = None
        if registry_ready:
            assert len(current) == 1, f"Invalid snapshot registry metadata on node {node_id}: {current}"
            metadata = re.fullmatch(
                r"Border \[Step (\d+), TxId (\d+)\] OldestCollectionTime \S+", current[0]
            )
            assert metadata, f"Invalid snapshot registry metadata on node {node_id}: {current}"
            border = tuple(map(int, metadata.groups()))

        snapshot_pattern = (
            r"Snapshot \[Step (\d+), TxId (\d+)\] Age \d+s SessionActorId .+ "
            r"\[(?:registry input|covered by border)\] Tables:(?: .*)?"
        )
        local_snapshots = set()
        local_rows = 0
        local_total = None
        for line in section("Local snapshots (promoted and alive):"):
            snapshot = re.fullmatch(snapshot_pattern, line)
            total = re.fullmatch(r"Total: (\d+)", line)
            if snapshot:
                local_snapshots.add(tuple(map(int, snapshot.groups())))
                local_rows += 1
            elif total:
                assert local_total is None, f"Duplicate local snapshot total on node {node_id}"
                local_total = int(total.group(1))
            else:
                raise AssertionError(f"Invalid local snapshot on node {node_id}: {line!r}")
        assert local_total == local_rows, f"Invalid local snapshot total on node {node_id}: {local_total}"

        remote_nodes = {}
        remote_snapshots = set()
        for line in section("Remote snapshots:"):
            collection = re.fullmatch(r"Node (\d+) CollectionTime (\S+) \(.+ ago\)", line)
            snapshot = re.fullmatch(r"Node (\d+) " + snapshot_pattern, line)
            if collection:
                peer = int(collection.group(1))
                assert peer not in remote_nodes, f"Duplicate snapshot peer {peer} on node {node_id}"
                remote_nodes[peer] = collection.group(2)
            elif snapshot:
                remote_snapshots.add(tuple(map(int, snapshot.groups())))
            else:
                raise AssertionError(f"Invalid remote snapshot on node {node_id}: {line!r}")
        return {
            "registry_ready": registry_ready,
            "border": border,
            "storage_ready": headers[0].endswith(" true"),
            "remote_nodes": remote_nodes,
            "local_snapshots": local_snapshots,
            "remote_snapshots": remote_snapshots,
        }

    def wait_remote_node(self, observer_node_id, remote_node_id, present=True, timeout=90):
        def matched():
            state = self.snapshot_state(observer_node_id)
            if not state["registry_ready"]:
                return None
            if (remote_node_id in state["remote_nodes"]) == present:
                return state
            return None

        return self._wait(
            f"snapshot peer {remote_node_id} {'present' if present else 'expired'} on node {observer_node_id}",
            matched,
            timeout,
        )

    def wait_registry_empty(self, node_ids=None, timeout=60):
        self.wait_registry_count(0, node_ids=node_ids, timeout=timeout)

    def wait_registry_count(self, expected_count, node_ids=None, timeout=60):
        """Wait for the exact remote snapshot count on every selected node."""
        if expected_count < 0:
            raise ValueError("Expected snapshot count must be nonnegative")
        if node_ids is None:
            nodes = list(self.cluster.nodes.values()) + list(self.cluster.slots.values())
            node_ids = [node.node_id for node in nodes]
        counts = {}

        def matched():
            nonlocal counts
            counts = {node_id: self.remote_snapshot_count(node_id) for node_id in node_ids}
            return all(count == expected_count for count in counts.values())

        try:
            self._wait(f"remote snapshot count {expected_count} on nodes {node_ids}", matched, timeout)
        except AssertionError as error:
            raise AssertionError(f"{error}; last snapshot counts: {counts}") from error

    def tablets(self, path):
        """Return current leader tablets; re-query after every split or merge."""
        if not path.startswith("/"):
            path = f"{self.database}/{path}"
        result = self._pool.execute_with_retries(
            "DECLARE $path AS Utf8; "
            "SELECT TabletId FROM `.sys/partition_stats` "
            "WHERE Path = $path AND FollowerId = 0;",
            {"$path": path},
        )
        # SchemeShard can publish the partition before its first tablet stats
        # arrive. Such a row has TabletId=0 and is not a usable tablet yet.
        return sorted({
            int(row["TabletId"]) for row in result[0].rows
            if row["TabletId"] is not None and int(row["TabletId"]) > 0
        })

    def wait_tablets(self, path, count=None, timeout=90):
        if count is not None and count < 1:
            raise ValueError("Expected tablet count must be positive")

        def available():
            tablets = self.tablets(path)
            return tablets if tablets and (count is None or len(tablets) == count) else None

        return self._wait(f"{count or 'nonempty'} tablets for {path}", available, timeout)

    def _sys_value(self, tablet_id, key, default=None):
        response = requests.get(
            self.base_url + "/tablets/db",
            params={
                "TabletID": tablet_id,
                "TableID": SYS_TABLE_ID,
                "Key": key,
                "Lookup": "Exact",
                "MaxRows": 1,
            },
            timeout=10,
        )
        response.raise_for_status()
        parser = _DbRows()
        parser.feed(response.text)
        # Check the schema before using numeric column positions. A changed
        # inspector or Sys schema must fail loudly, not turn into a false pass.
        header = ["1:Uint64 Id", "2:String Bytes", "3:Uint64 Uint64"]
        assert header in parser.rows, (
            f"Unexpected Sys schema for tablet {tablet_id}: {parser.rows}"
        )
        rows = [row for row in parser.rows if row and row[0] == str(key)]
        if not rows:
            if default is not None:
                return default
            raise _MonitoringNotReady(f"Missing Sys key {key} on tablet {tablet_id}: {parser.rows}")
        assert len(rows) == 1 and len(rows[0]) == 3 and rows[0][2].isdigit(), (
            f"Invalid Sys key {key} on tablet {tablet_id}: {parser.rows}"
        )
        return int(rows[0][2])

    def low_watermark(self, tablet_id):
        return (
            self._sys_value(tablet_id, LOW_WATERMARK_STEP_KEY),
            self._sys_value(tablet_id, LOW_WATERMARK_TX_ID_KEY),
        )

    def write_edge(self, path, timeout=90):
        """Capture a write bound after the caller has acknowledged its write."""
        tablets = self.wait_tablets(path, timeout=timeout)
        bound = max(
            max(
                self._sys_value(tablet, COMPLETE_EDGE_STEP_KEY, default=0),
                self._sys_value(tablet, IMMEDIATE_WRITE_EDGE_STEP_KEY, default=0),
            )
            for tablet in tablets
        )
        assert bound > 0, f"No persisted MVCC write edge on {path} tablets {tablets}"
        return bound

    @staticmethod
    def _wait(description, predicate, timeout=90, advance=None):
        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                if advance is not None:
                    advance()
                result = predicate()
                if result:
                    return result
            except (
                requests.RequestException, URLError, TimeoutError, ConnectionError, HTTPException,
                ydb.Error, _MonitoringNotReady,
            ) as error:
                # Tablet movement can temporarily make independent monitoring
                # reads unavailable or incomplete. Never retry the transaction
                # being checked, or hide malformed monitoring data.
                last_error = error
            time.sleep(0.25)
        raise AssertionError(f"Timed out waiting for {description}; last error: {last_error}")

    def snapshot_bound(self, held, advance=None, timeout=90, anchor_key=None):
        """Capture a fixed write-step bound above the held snapshot.

        First observe an acknowledged newer generation at anchor, then capture
        its persisted MVCC edge. That write follows the held repeatable read,
        so its version bounds the held snapshot from above. Coordinator steps
        can be ahead of wall time; the client's timestamp is not a safe bound.

        Reuse the returned bound for every checked path. Capture another bound
        after a release or fault when requiring further cleanup progress.
        An explicit anchor_key observes a separate marker, leaving the held
        transaction's anchor read intact for read-write conflict checks.
        """
        anchor = held.workload.anchor_path

        def newer_generation():
            if anchor_key is not None:
                result = held.workload.execute(
                    f"SELECT generation FROM `{anchor}` WHERE key = {anchor_key}u;"
                )
                rows = result[0].rows
                return len(rows) == 1 and int(rows[0]["generation"]) > held.generation
            rows = held.workload.read_current(table_path=anchor)
            return bool(rows) and all(row[1] > held.generation for row in rows)

        self._wait(
            f"acknowledged anchor write after generation {held.generation}",
            newer_generation,
            timeout,
            advance=advance,
        )
        # Freeze once: chasing the writer's moving edge would prevent the
        # cleanup horizon, which deliberately lags writes, from catching up.
        return self.write_edge(anchor, timeout=timeout)

    def wait_low_watermark(self, path, after_step, advance=None, timeout=90):
        watermarks = {}

        def advanced():
            nonlocal watermarks
            tablets = self.tablets(path)
            if not tablets:
                return False
            watermarks = {tablet: self.low_watermark(tablet) for tablet in tablets}
            logger.debug("Snapshot bound %s, %s watermarks: %s", after_step, path, watermarks)
            return watermarks if all(v[0] > after_step for v in watermarks.values()) else None

        try:
            return self._wait(
                f"LowWatermark > {after_step} on {path}",
                advanced,
                timeout=timeout,
                advance=advance,
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; last watermarks: {watermarks}") from error

    def restart_tablet(self, tablet_id, timeout=90):
        """Wait up to timeout for an active tablet before and after one kill."""

        def active_info():
            result = self._json("/cms/api/datashard/json/getinfo", {"tabletid": tablet_id})
            if result.get("Status", {}).get("Code") != "SUCCESS":
                return None
            info = result.get("TabletInfo") or {}
            if not info.get("IsActive"):
                return None
            try:
                current_generation = int(info.get("Generation"))
            except (TypeError, ValueError):
                # Incomplete metadata is not proof of an active generation.
                return None
            return current_generation, info

        generation, _ = self._wait(f"tablet {tablet_id} active before restart", active_info, timeout)
        self.cluster.client.tablet_kill(tablet_id, assert_success=True)

        def restarted():
            observed = active_info()
            if observed is None:
                return None
            current_generation, info = observed
            return info if current_generation > generation else None

        return self._wait(f"tablet {tablet_id} restart after generation {generation}", restarted, timeout)

    def tablet_node(self, tablet_id):
        info = self._swagger.hive_info_by_tablet_id(tablet_id)
        if info is None:
            return 0
        leaders = [
            tablet for tablet in info.get("Tablets", [])
            if int(tablet["TabletID"]) == tablet_id and not int(tablet.get("FollowerID", 0))
        ]
        if not leaders:
            return 0
        assert len(leaders) == 1, info
        return int(leaders[0].get("NodeID", 0))

    def move_tablet(self, tablet_id, node_id, timeout=90):
        if self.tablet_node(tablet_id) != node_id:
            # Acknowledge the command without holding the HTTP request open
            # until restart completes. Submit it once, then poll read-only state.
            response = requests.post(
                self.base_url + "/tablets/app",
                params={
                    "TabletID": self._hive_id,
                    "page": "MoveTablet",
                    "tablet": tablet_id,
                    "node": node_id,
                    "wait": "0",
                },
                timeout=60,
            )
            response.raise_for_status()
            result = response.json()
            assert result == {}, result

        def moved():
            if self.tablet_node(tablet_id) != node_id:
                return False
            result = self._json("/cms/api/datashard/json/getinfo", {"tabletid": tablet_id})
            return (
                result.get("Status", {}).get("Code") == "SUCCESS"
                and (result.get("TabletInfo") or {}).get("IsActive")
            )

        self._wait(
            f"tablet {tablet_id} move to node {node_id}",
            moved,
            timeout,
        )

    def _split_merge(self, path, tablet_ids, boundary=None, timeout=90):
        previous = set(self.tablets(path))
        request = msgbus_pb2.TSchemeOperation()
        modify = request.Transaction.ModifyScheme
        modify.OperationType = operations_pb2.ESchemeOpSplitMergeTablePartitions
        operation = modify.SplitMergeTablePartitions
        operation.TablePath = path
        operation.SourceTabletId.extend(tablet_ids)
        if boundary is not None:
            # The workload's primary key is Uint64. Match TClient::SplitTable
            # in core/testlib/test_client.cpp, including the Optional wrapper.
            operation.SplitBoundary.add().KeyPrefix.Tuple.add().Optional.Uint64 = boundary
        # Submit the mutation once. The default send_and_poll_request asks the
        # server to wait 120s, longer than the client's 10s RPC deadline.
        result = self.cluster.client.send_request(request, "SchemeOperation")
        assert result.Status in (MessageBusStatus.MSTATUS_OK, MessageBusStatus.MSTATUS_INPROGRESS), result
        assert result.SchemeStatus in (SchemeStatus.StatusSuccess, SchemeStatus.StatusAccepted), result
        if result.Status != MessageBusStatus.MSTATUS_OK or result.SchemeStatus != SchemeStatus.StatusSuccess:
            tx_id = result.FlatTxId.TxId
            scheme_shard = result.FlatTxId.SchemeShardTabletId
            assert tx_id and scheme_shard, result

            def completed():
                # Poll only this transaction. A poll timeout must never submit a
                # second split/merge proposal with a different transaction id.
                status = self.cluster.client.flat_transaction_status(tx_id, scheme_shard, timeout=5)
                assert status.Status in (
                    MessageBusStatus.MSTATUS_OK,
                    MessageBusStatus.MSTATUS_INPROGRESS,
                    MessageBusStatus.MSTATUS_TIMEOUT,
                ), status
                if status.Status != MessageBusStatus.MSTATUS_OK:
                    return False
                # NotifyTxCompletion responses omit SchemeStatus; its protobuf
                # default is StatusSuccess. Reject any explicit nonzero value.
                assert status.SchemeStatus == SchemeStatus.StatusSuccess, status
                return True

            self._wait(f"completion of split/merge {tx_id} on {scheme_shard}", completed, timeout)

        def transferred():
            current = set(self.tablets(path))
            expected_count = len(previous) - len(tablet_ids) + (2 if boundary is not None else 1)
            if len(current) != expected_count or current.intersection(tablet_ids):
                return None
            return sorted(current)

        return self._wait(f"split/merge of {path}, sources {tablet_ids}", transferred, timeout)

    def split(self, path, tablet_id, boundary=10, timeout=90):
        return self._split_merge(path, [tablet_id], boundary, timeout)

    def merge(self, path, tablet_ids, timeout=90):
        return self._split_merge(path, tablet_ids, timeout=timeout)
