"""Read-only MVCC observation and tablet administration for retention E2E tests."""

import logging
import time
from html.parser import HTMLParser

import requests

from ydb.core.protos import msgbus_pb2
from ydb.core.protos.schemeshard import operations_pb2
from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID
from ydb.tests.library.common.msgbus_types import MessageBusStatus, SchemeStatus
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


class SnapshotRetentionMonitor:
    def __init__(self, cluster, driver, database):
        self.cluster = cluster
        self.driver = driver
        self.database = database.rstrip("/")
        node = next(iter(cluster.nodes.values()))
        self.base_url = f"http://{node.host}:{node.mon_port}"
        self._snapshot_bounds = {}

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
        assert counters, f"RemoteSnapshotsInRegistry is missing on node {node_id}"
        return sum(int(value) for value in counters)

    def wait_registry_empty(self, node_ids=None, timeout=60):
        if node_ids is None:
            nodes = list(self.cluster.nodes.values()) + list(self.cluster.slots.values())
            node_ids = [node.node_id for node in nodes]
        counts = {}

        def empty():
            nonlocal counts
            counts = {node_id: self.remote_snapshot_count(node_id) for node_id in node_ids}
            return all(count == 0 for count in counts.values())

        try:
            self._wait("released snapshots to leave the registries", empty, timeout)
        except AssertionError as error:
            raise AssertionError(f"{error}; last snapshot counts: {counts}") from error

    def tablets(self, path):
        """Return current leader tablets; re-query after every split or merge."""
        if not path.startswith("/"):
            path = f"{self.database}/{path}"
        with ydb.QuerySessionPool(self.driver) as pool:
            result = pool.execute_with_retries(
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

    def info(self, tablet_id):
        result = self._json("/cms/api/datashard/json/getinfo", {"tabletid": tablet_id})
        assert result.get("Status", {}).get("Code") == "SUCCESS", result
        return result

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
        rows = [row for row in parser.rows if len(row) == 3 and row[0] == str(key)]
        if not rows and default is not None:
            return default
        assert len(rows) == 1 and rows[0][2].isdigit(), (
            f"Missing Sys key {key} on tablet {tablet_id}: {parser.rows}"
        )
        return int(rows[0][2])

    def low_watermark(self, tablet_id):
        return (
            self._sys_value(tablet_id, LOW_WATERMARK_STEP_KEY),
            self._sys_value(tablet_id, LOW_WATERMARK_TX_ID_KEY),
        )

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
            except (requests.RequestException, ydb.Error) as error:
                # Tablet movement can temporarily make independent monitoring
                # reads unavailable. Never retry the transaction being checked.
                last_error = error
            time.sleep(0.25)
        raise AssertionError(f"Timed out waiting for {description}; last error: {last_error}")

    def wait_past_snapshot(self, held, path, advance=None, timeout=90):
        """Prove that every target shard advanced past the held snapshot.

        First observe an acknowledged newer generation at anchor, then capture
        its persisted MVCC edge. That write follows the held repeatable read,
        so its version bounds the held snapshot from above. Coordinator steps
        can be ahead of wall time; the client's timestamp is not a safe bound.
        """
        if held.tx_id not in self._snapshot_bounds:
            anchor = held.workload.anchor_path

            def newer_generation():
                rows = held.workload.read_current(table_path=anchor)
                return bool(rows) and all(row[1] > held.generation for row in rows)

            self._wait(
                f"acknowledged anchor write after generation {held.generation}",
                newer_generation,
                timeout,
                advance=advance,
            )
            anchors = self.wait_tablets(anchor, timeout=timeout)
            bound = max(
                max(
                    self._sys_value(tablet, COMPLETE_EDGE_STEP_KEY, default=0),
                    self._sys_value(tablet, IMMEDIATE_WRITE_EDGE_STEP_KEY, default=0),
                )
                for tablet in anchors
            )
            assert bound > 0, f"No persisted MVCC write edge on anchor tablets {anchors}"
            # Freeze once: chasing the writer's moving edge would prevent the
            # cleanup horizon, which deliberately lags writes, from catching up.
            self._snapshot_bounds[held.tx_id] = bound
        watermark_bound = self._snapshot_bounds[held.tx_id]
        watermarks = {}

        def advanced():
            nonlocal watermarks
            tablets = self.tablets(path)
            if not tablets:
                return False
            watermarks = {tablet: self.low_watermark(tablet) for tablet in tablets}
            logger.debug("Snapshot bound %s, %s watermarks: %s", watermark_bound, path, watermarks)
            return watermarks if all(v[0] > watermark_bound for v in watermarks.values()) else None

        try:
            return self._wait(
                f"LowWatermark > {watermark_bound} on {path}",
                advanced,
                timeout=timeout,
                advance=advance,
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; last watermarks: {watermarks}") from error

    def restart_tablet(self, tablet_id, timeout=90):
        generation = int(self.info(tablet_id)["TabletInfo"]["Generation"])
        self.cluster.client.tablet_kill(tablet_id, assert_success=True)

        def restarted():
            result = self._json("/cms/api/datashard/json/getinfo", {"tabletid": tablet_id})
            if result.get("Status", {}).get("Code") != "SUCCESS":
                return None
            info = result.get("TabletInfo", {})
            return info if info.get("IsActive") and int(info["Generation"]) > generation else None

        return self._wait(f"tablet {tablet_id} restart after generation {generation}", restarted, timeout)

    def tablet_node(self, tablet_id):
        info = self._json(
            "/viewer/json/hiveinfo",
            {"hive_id": DEFAULT_HIVE_ID, "tablet_id": tablet_id},
        )
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
        response = requests.post(
            self.base_url + "/tablets/app",
            params={
                "TabletID": DEFAULT_HIVE_ID,
                "page": "MoveTablet",
                "tablet": tablet_id,
                "node": node_id,
            },
            timeout=60,
        )
        response.raise_for_status()
        result = response.json()
        assert result.get("status"), result
        self._wait(
            f"tablet {tablet_id} move to node {node_id}",
            lambda: self.tablet_node(tablet_id) == node_id,
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
