# Adapted from ydb/tests/tools/nemesis/library/disk.py

from __future__ import annotations

import random
import socket
import subprocess
import time
from typing import Any, Tuple

from ydb.tests.library.common.msgbus_types import EDriveStatus
from ydb.tests.library.predicates import blobstorage
from ydb.core.protos.blobstorage_config_pb2 import TConfigResponse

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


def _resolve_node_id_and_node(cluster, host: str | None) -> Tuple[int | None, Any]:
    """
    Map orchestrator ``payload['host']`` (same as dispatch target) to harness ``(node_id, node)``.
    Hostnames must match cluster.yaml ``host`` / ``name`` (short hostname fallback).
    """
    if not host or not str(host).strip():
        return None, None
    h = str(host).strip()
    for node_id, node in cluster.nodes.items():
        if node.host == h:
            return node_id, node
    h_short = h.split(".")[0]
    for node_id, node in cluster.nodes.items():
        nh = node.host.split(".")[0] if "." in node.host else node.host
        if nh == h_short:
            return node_id, node
    return None, None


def _host_from_payload(payload: dict) -> str | None:
    h = payload.get("host")
    if h and str(h).strip():
        return str(h).strip()
    try:
        fq = socket.getfqdn()
        if fq and fq != "localhost":
            return fq
    except Exception:
        pass
    try:
        return socket.gethostname()
    except Exception:
        return None


class ClusterSafelyCleanupDisksNemesis(MonitoredAgentActor):
    """Stop kikimr, cleanup disks, start on the node selected by the orchestrator (``payload.host``)."""

    def __init__(self) -> None:
        super().__init__(scope="disk")

    def _state_ready(self, cluster) -> bool:
        if len(cluster.nodes.values()) < 8:
            self._logger.info("Data erase is prohibited (< 8 nodes).")
            return False
        return blobstorage.cluster_has_no_unreplicated_vdisks(cluster, 1, self._logger)

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        if not self._state_ready(cluster):
            return
        node_id, node = _resolve_node_id_and_node(cluster, _host_from_payload(payload))
        if node is None:
            self._logger.error(
                "SafelyCleanupDisks: unknown host %r (not in cluster.yaml nodes)",
                _host_from_payload(payload),
            )
            return
        self._logger.info("SafelyCleanupDisks on node_id=%s host=%s", node_id, node.host)
        try:
            node.ssh_command("sudo systemctl stop kikimr.service")
            node.kill_process_and_daemon()
            node.cleanup_disks()
            node.start()
            self.on_success_inject_fault()
        except subprocess.CalledProcessError as e:
            self._logger.error("SafelyCleanupDisks failed: %s", e)

    def extract_fault(self, payload=None) -> None:
        del payload


class ClusterSafelyBreakDiskNemesis(MonitoredAgentActor):
    """Mark one drive BROKEN on the dispatch node; extract_fault restores ACTIVE for tracked drives."""

    def __init__(self) -> None:
        super().__init__(scope="disk")
        self._states: dict = {}
        self._broken_drives: set = set()

    def _state_ready(self, cluster) -> bool:
        if len(cluster.nodes.values()) < 8:
            self._logger.info("Disk break is prohibited (< 8 nodes).")
            return False
        return blobstorage.cluster_has_no_unreplicated_vdisks(cluster, 1, self._logger)

    def _refresh_drive_states_for_node(self, cluster, node_id: int) -> None:
        node = cluster.nodes.get(node_id)
        if node is None:
            return
        self._broken_drives = {(nid, p) for nid, p in self._broken_drives if nid != node_id}
        self._states[node_id] = {}
        drives = cluster.client.read_drive_status(node.host, node.ic_port).BlobStorageConfigResponse
        for status in drives.Status:
            for drive in status.DriveStatus:
                self._states[node_id][drive.Path] = drive.Status
                if drive.Status != EDriveStatus.ACTIVE:
                    self._broken_drives.add((node_id, drive.Path))

    def _change_drive_status(self, cluster, node_id, path, status) -> bool:
        node = cluster.nodes[node_id]
        host, ic_port = node.host, node.ic_port
        self._logger.info("Change drive status %s:%s path %s -> %s", host, ic_port, path, status.name)
        response = None
        for _ in range(6):
            response = cluster.client.update_drive_status(host, ic_port, path, status).BlobStorageConfigResponse
            if (
                not response.Success
                and len(response.Status) == 1
                and response.Status[0].FailReason == TConfigResponse.TStatus.EFailReason.kMayLoseData
            ):
                time.sleep(10)
            else:
                break
        self._logger.info("update_drive_status response: %s", response)
        return response.Success

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        if not self._state_ready(cluster):
            return
        node_id, _node = _resolve_node_id_and_node(cluster, _host_from_payload(payload))
        if node_id is None:
            self._logger.error(
                "SafelyBreakDisk: unknown host %r (not in cluster.yaml nodes)",
                _host_from_payload(payload),
            )
            return
        self.extract_fault(payload)
        self._refresh_drive_states_for_node(cluster, node_id)
        paths = self._states.get(node_id) or {}
        if paths:
            path = random.choice(list(paths.keys()))
            if self._change_drive_status(cluster, node_id, path, EDriveStatus.BROKEN):
                self.on_success_inject_fault()
        self._refresh_drive_states_for_node(cluster, node_id)

    def extract_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        for node_id, path in list(self._broken_drives):
            self._change_drive_status(cluster, node_id, path, EDriveStatus.ACTIVE)
        self._broken_drives.clear()
