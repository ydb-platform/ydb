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

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import (
    local_harness_mode,
    require_cluster,
    require_external_cluster,
)
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor
from ydb.tests.stability.nemesis.internal.nemesis.runners.target_payload import target_from_payload


def _resolve_node_id_and_node(cluster, host: str | None, node_id: int | None = None) -> Tuple[int | None, Any]:
    """
    Map orchestrator target to harness ``(node_id, node)``.
    Prefers explicit ``node_id``; else match ``host`` against cluster.yaml hosts.
    """
    if node_id is not None:
        node = cluster.nodes.get(node_id)
        if node is not None:
            return node_id, node
    if not host or not str(host).strip():
        return None, None
    h = str(host).strip()
    for nid, node in cluster.nodes.items():
        if node.host == h:
            return nid, node
    h_short = h.split(".")[0]
    for nid, node in cluster.nodes.items():
        nh = node.host.split(".")[0] if "." in node.host else node.host
        if nh == h_short:
            return nid, node
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


def _kikimr_slot_id(node: Any) -> str | None:
    """``None`` = single ``kikimr`` service; else ``kikimr-multi@`` instance id (matches harness)."""
    return getattr(node, "_KikimrExternalNode__slot_id", None)


def _local_stop_kikimr(node: Any) -> None:
    """Same semantics as :meth:`KikimrExternalNode.stop`, without SSH."""
    slot_id = _kikimr_slot_id(node)
    if slot_id is None:
        subprocess.run("sudo service kikimr stop", shell=True, check=True)
    else:
        subprocess.run(
            ["sudo", "systemctl", "stop", "kikimr-multi@{}".format(slot_id)],
            check=True,
        )


def _local_kill_process_and_daemon(ic_port: int) -> None:
    """Mirror :meth:`ExternalNodeDaemon.kill_process_and_daemon` locally."""
    sigkill = 9
    subprocess.run(
        "ps aux | grep daemon | grep %d | grep -v grep | awk '{ print $2 }' | xargs -r sudo kill -%d"
        % (ic_port, sigkill),
        shell=True,
        check=False,
    )
    subprocess.run(
        "ps aux | grep %d | grep -v grep | awk '{ print $2 }' | xargs -r sudo kill -%d" % (ic_port, sigkill),
        shell=True,
        check=False,
    )


def _local_cleanup_disks() -> None:
    subprocess.run(
        "for X in /dev/disk/by-partlabel/kikimr_*; "
        "do sudo /Berkanavt/kikimr/bin/kikimr admin bs disk obliterate $X; done",
        shell=True,
        check=True,
    )


def _local_start_kikimr(node: Any) -> None:
    """Start one local kikimr unit without rewriting ``slot_cfg``."""
    slot_id = _kikimr_slot_id(node)
    if slot_id is None:
        subprocess.run("sudo service kikimr start", shell=True, check=True)
        return
    subprocess.run(
        ["sudo", "systemctl", "start", "kikimr-multi@{}".format(slot_id)],
        check=True,
    )


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
        target = target_from_payload(payload)
        node_id_hint = target.node_id if target is not None else None
        host = (target.host if target is not None else None) or _host_from_payload(payload)
        node_id, node = _resolve_node_id_and_node(cluster, host, node_id_hint)
        if node is None:
            self._logger.error(
                "SafelyCleanupDisks: unknown host %r / node_id %r (not in cluster.yaml nodes)",
                host,
                node_id_hint,
            )
            return
        self._logger.info("SafelyCleanupDisks on node_id=%s host=%s", node_id, node.host)
        try:
            _local_stop_kikimr(node)
            _local_kill_process_and_daemon(int(node.ic_port))
            _local_cleanup_disks()
            _local_start_kikimr(node)
            self.on_success_inject_fault()
        except subprocess.CalledProcessError as e:
            self._logger.error("SafelyCleanupDisks failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterSafelyBreakDiskNemesis(MonitoredAgentActor):
    """Mark one drive BROKEN on the dispatch node; extract_fault restores ACTIVE for tracked drives."""

    def __init__(self) -> None:
        super().__init__(scope="disk")
        # External clusters keep the historical "restore every observed non-ACTIVE
        # drive" state. Local harness runs track only drives changed by this actor.
        self._states: dict = {}
        self._broken_drives: set = set()
        self._changed_drives: dict[str, set[tuple[int, str]]] = {}

    def _state_ready(self, cluster) -> bool:
        if len(cluster.nodes.values()) < 8:
            self._logger.info("Disk break is prohibited (< 8 nodes).")
            return False
        return blobstorage.cluster_has_no_unreplicated_vdisks(cluster, 1, self._logger)

    def _refresh_drive_states_for_node(self, cluster, node_id: int) -> None:
        node = cluster.nodes.get(node_id)
        if node is None:
            return
        self._broken_drives = {
            (nid, path) for nid, path in self._broken_drives if nid != node_id
        }
        self._states[node_id] = {}
        drives = cluster.client.read_drive_status(
            node.host,
            node.ic_port,
        ).BlobStorageConfigResponse
        for status in drives.Status:
            for drive in status.DriveStatus:
                self._states[node_id][drive.Path] = drive.Status
                if drive.Status != EDriveStatus.ACTIVE:
                    self._broken_drives.add((node_id, drive.Path))

    def _drive_states_for_node(self, cluster, node_id: int) -> dict[str, EDriveStatus]:
        node = cluster.nodes.get(node_id)
        if node is None:
            return {}
        states = {}
        drives = cluster.client.read_drive_status(node.host, node.ic_port).BlobStorageConfigResponse
        for status in drives.Status:
            for drive in status.DriveStatus:
                states[drive.Path] = drive.Status
        return states

    def _local_spare_paths(
        self,
        cluster,
        node_id: int,
        active_paths: list[str],
    ) -> list[str]:
        base = cluster.client.query_base_config().BaseConfig
        paths_by_pdisk = {
            (pdisk.NodeId, pdisk.PDiskId): pdisk.Path
            for pdisk in base.PDisk
            if pdisk.NodeId == node_id
        }
        occupied = {
            (vslot.VSlotId.NodeId, vslot.VSlotId.PDiskId)
            for vslot in base.VSlot
            if vslot.VSlotId.NodeId == node_id
        }
        occupied_paths = {
            paths_by_pdisk[pdisk]
            for pdisk in occupied
            if pdisk in paths_by_pdisk
        }
        dedicated_spare_paths = {
            pdisk.get("pdisk_path")
            for pdisk in getattr(cluster.config, "pdisks_info", ()) or ()
            if (
                pdisk.get("node_id") == node_id
                and pdisk.get("pdisk_user_kind", 0) != 0
            )
        }
        return sorted(
            set(active_paths) & dedicated_spare_paths - occupied_paths
        )

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
        if local_harness_mode():
            self._inject_local_fault(payload)
        else:
            self._inject_external_fault(payload)

    def _inject_external_fault(self, payload: dict) -> None:
        cluster = require_external_cluster()
        if not self._state_ready(cluster):
            return
        target = target_from_payload(payload)
        node_id_hint = target.node_id if target is not None else None
        host = (target.host if target is not None else None) or _host_from_payload(payload)
        node_id, _node = _resolve_node_id_and_node(cluster, host, node_id_hint)
        if node_id is None:
            self._logger.error(
                "SafelyBreakDisk: unknown host %r / node_id %r (not in cluster.yaml nodes)",
                host,
                node_id_hint,
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

    def _inject_local_fault(self, payload: dict) -> None:
        cluster = require_cluster()
        if not self._state_ready(cluster):
            return
        target = target_from_payload(payload)
        node_id_hint = target.node_id if target is not None else None
        host = (target.host if target is not None else None) or _host_from_payload(payload)
        node_id, _node = _resolve_node_id_and_node(cluster, host, node_id_hint)
        if node_id is None:
            self._logger.error(
                "SafelyBreakDisk: unknown host %r / node_id %r (not in cluster.yaml nodes)",
                host,
                node_id_hint,
            )
            return
        states = self._drive_states_for_node(cluster, node_id)
        active_paths = self._local_spare_paths(
            cluster,
            node_id,
            [
                path
                for path, status in states.items()
                if status == EDriveStatus.ACTIVE
            ],
        )
        if not active_paths:
            raise RuntimeError(
                f"SafelyBreakDisk: node_id={node_id} has no eligible ACTIVE drive"
            )
        path = random.choice(active_paths)
        identity = (
            target.identity_key()
            if target is not None
            else f"disk:{node_id}:{host or ''}"
        )
        if self._changed_drives.get(identity):
            raise RuntimeError(
                f"SafelyBreakDisk: target {identity} already owns a changed drive"
            )
        changed = self._changed_drives.setdefault(identity, set())
        changed.add((node_id, path))
        if not self._change_drive_status(
            cluster,
            node_id,
            path,
            EDriveStatus.BROKEN,
        ):
            states_after_failure = self._drive_states_for_node(cluster, node_id)
            restored = states_after_failure.get(path) == EDriveStatus.ACTIVE
            if not restored:
                restored = self._change_drive_status(
                    cluster,
                    node_id,
                    path,
                    EDriveStatus.ACTIVE,
                )
            if restored:
                changed.discard((node_id, path))
                if not changed:
                    self._changed_drives.pop(identity, None)
            raise RuntimeError(
                "SafelyBreakDisk: failed to mark "
                f"node_id={node_id} path={path} BROKEN; "
                f"compensating_restore={'succeeded' if restored else 'failed'}"
            )
        self.on_success_inject_fault()

    def extract_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        if not local_harness_mode():
            self._extract_external_fault()
            return
        cluster = require_cluster()
        target = target_from_payload(payload)
        identities = (
            [target.identity_key()]
            if target is not None
            else list(self._changed_drives)
        )
        failures = []
        for identity in identities:
            changed = self._changed_drives.get(identity, set())
            restored = set()
            for node_id, path in list(changed):
                if self._change_drive_status(
                    cluster,
                    node_id,
                    path,
                    EDriveStatus.ACTIVE,
                ):
                    restored.add((node_id, path))
                else:
                    failures.append((node_id, path))
            changed.difference_update(restored)
            if not changed:
                self._changed_drives.pop(identity, None)
        if failures:
            raise RuntimeError(
                f"SafelyBreakDisk: failed to restore drives: {failures}"
            )
        self.on_success_extract_fault()

    def _extract_external_fault(self) -> None:
        cluster = require_external_cluster()
        for node_id, path in list(self._broken_drives):
            self._change_drive_status(cluster, node_id, path, EDriveStatus.ACTIVE)
        self._broken_drives.clear()
        self.on_success_extract_fault()

    def changed_drives(self) -> dict[str, tuple[tuple[int, str], ...]]:
        return {
            identity: tuple(sorted(drives))
            for identity, drives in self._changed_drives.items()
        }
