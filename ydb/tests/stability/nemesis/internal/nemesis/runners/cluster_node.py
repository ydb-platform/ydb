# Adapted from ydb/tests/tools/nemesis/library/node.py
#
# All runners execute **locally** on the agent host.  The orchestrator picks
# the target host and dispatches the command; the agent runs subprocess calls
# against the local kikimr / kikimr-multi@ services.

from __future__ import annotations

import signal
import subprocess
import threading
import time
from typing import Any

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import (
    local_harness_mode,
    require_cluster,
    require_external_cluster,
)
from ydb.tests.stability.nemesis.internal.nemesis.local_harness_controller import (
    HarnessNodeController,
)
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor
from ydb.tests.stability.nemesis.internal.nemesis.runners.target_payload import target_from_payload


# ---------------------------------------------------------------------------
# Local subprocess helpers (shared by runners in this module)
# ---------------------------------------------------------------------------


def _kikimr_slot_id(node: Any) -> str | None:
    """``None`` = single ``kikimr`` service; else ``kikimr-multi@`` instance id."""
    return getattr(node, "_KikimrExternalNode__slot_id", None)


def _local_kill_by_ic_port(ic_port: int, sig: int = 9) -> None:
    """Kill local process(es) whose command line contains the given ic-port."""
    subprocess.run(
        "ps aux | grep %d | grep -v grep | awk '{ print $2 }' | xargs -r sudo kill -%d"
        % (ic_port, sig),
        shell=True,
        check=False,
    )


def _local_kill_daemon_and_process(ic_port: int) -> None:
    """Mirror :meth:`ExternalNodeDaemon.kill_process_and_daemon` locally (SIGKILL)."""
    sigkill = 9
    subprocess.run(
        "ps aux | grep daemon | grep %d | grep -v grep | awk '{ print $2 }' | xargs -r sudo kill -%d"
        % (ic_port, sigkill),
        shell=True,
        check=False,
    )
    subprocess.run(
        "ps aux | grep %d | grep -v grep | awk '{ print $2 }' | xargs -r sudo kill -%d"
        % (ic_port, sigkill),
        shell=True,
        check=False,
    )


def _local_stop_kikimr(node: Any) -> None:
    """``systemctl stop`` for the local kikimr / kikimr-multi@ service."""
    slot_id = _kikimr_slot_id(node)
    if slot_id is None:
        subprocess.run("sudo service kikimr stop", shell=True, check=True)
    else:
        subprocess.run(
            ["sudo", "systemctl", "stop", "kikimr-multi@{}".format(slot_id)],
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


def _local_send_signal(ic_port: int, sig: int) -> None:
    """Send *sig* to local process(es) matching *ic_port*."""
    subprocess.run(
        "ps aux | grep '\\-\\-ic-port %d' | grep -v grep | awk '{ print $2 }' | xargs -r sudo kill -%d"
        % (ic_port, sig),
        shell=True,
        check=False,
    )


def _local_switch_version() -> None:
    """Swap ``/Berkanavt/kikimr/bin/kikimr`` symlink between ``kikimr-next`` and ``kikimr-last``."""
    subprocess.run(
        "sudo bash -c '"
        'cur=$(readlink /Berkanavt/kikimr/bin/kikimr); '
        'if [ "$cur" = "/Berkanavt/kikimr/bin/kikimr-next" ]; then '
        '  ln -sf /Berkanavt/kikimr/bin/kikimr-last /Berkanavt/kikimr/bin/kikimr; '
        'else '
        '  ln -sf /Berkanavt/kikimr/bin/kikimr-next /Berkanavt/kikimr/bin/kikimr; '
        "fi'",
        shell=True,
        check=True,
    )


def _node_from_target(cluster, payload: dict):
    """Resolve a cluster node from ChaosTarget / payload; no hostname guessing."""
    target = target_from_payload(payload)
    node_id = payload.get("node_id")
    if node_id is None and target is not None:
        node_id = target.node_id
    if node_id is None:
        return None, None
    return node_id, cluster.nodes.get(node_id)


def _slot_from_target(cluster, payload: dict):
    """Resolve a cluster slot from ChaosTarget / payload; no hostname guessing."""
    target = target_from_payload(payload)
    slot_idx = payload.get("slot_idx")
    if slot_idx is None and target is not None:
        slot_idx = target.slot_idx
    if slot_idx is None:
        return None, None
    return slot_idx, cluster.slots.get(slot_idx)


def _ic_port_from_target(payload: dict) -> int | None:
    target = target_from_payload(payload)
    if target is not None and target.ic_port is not None:
        return int(target.ic_port)
    if payload.get("node_ic_port") is not None:
        return int(payload["node_ic_port"])
    return None


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------


class ClusterKillSlotDaemonNemesis(MonitoredAgentActor):
    """Kill one slot daemon; requires ChaosTarget.slot_idx (or payload.slot_idx)."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        slot_idx, slot = _slot_from_target(cluster, payload)
        if slot is None:
            self._logger.error("KillSlotDaemon: missing/unknown slot_idx in ChaosTarget (got %r)", slot_idx)
            return
        try:
            self._logger.info("Killing slot daemon slot_idx=%s ic_port=%d", slot_idx, slot.ic_port)
            _local_kill_daemon_and_process(int(slot.ic_port))
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Kill slot failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterKillNodeDaemonNemesis(MonitoredAgentActor):
    """Kill a kikimr node daemon; requires ChaosTarget.node_id (or payload.node_id)."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        node_id, node = _node_from_target(cluster, payload)
        if node is None:
            self._logger.error("KillNodeDaemon: missing/unknown node_id in ChaosTarget (got %r)", node_id)
            return
        try:
            self._logger.info("Killing node daemon node_id=%s ic_port=%d", node_id, node.ic_port)
            _local_kill_daemon_and_process(int(node.ic_port))
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Kill node daemon failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterSerialKillNodeNemesis(MonitoredAgentActor):
    """Kill the node daemon; ``node_id`` + ``sleep_before`` come from the orchestrator payload."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        delay = float(payload.get("sleep_before", 0) or 0)
        if delay > 0:
            self._logger.info("Serial kill node: sleep %.1fs before kill", delay)
            time.sleep(delay)
        cluster = require_external_cluster()
        node_id, node = _node_from_target(cluster, payload)
        if node is None:
            self._logger.error("Serial kill node: missing/unknown node_id (got %r)", node_id)
            return
        try:
            self._logger.info("Serial kill node daemon node_id=%s ic_port=%d", node_id, node.ic_port)
            _local_kill_daemon_and_process(int(node.ic_port))
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Serial kill node failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterSerialKillSlotsNemesis(MonitoredAgentActor):
    """Kill the slot daemon; ``slot_idx`` + ``sleep_before`` from orchestrator."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        delay = float(payload.get("sleep_before", 0) or 0)
        if delay > 0:
            self._logger.info("Serial kill slot: sleep %.1fs before kill", delay)
            time.sleep(delay)
        cluster = require_external_cluster()
        slot_idx, slot = _slot_from_target(cluster, payload)
        if slot is None:
            self._logger.error("Serial kill slot: missing/unknown slot_idx (got %r)", slot_idx)
            return
        try:
            self._logger.info("Serial kill slot daemon slot_idx=%s ic_port=%d", slot_idx, slot.ic_port)
            _local_kill_daemon_and_process(int(slot.ic_port))
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Serial kill slot failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterStopStartNodeNemesis(MonitoredAgentActor):
    """Stop the node process; next inject starts it again (>=8 nodes). Requires node_id."""

    def __init__(self) -> None:
        super().__init__(scope="node")
        self._stopped_node = None
        self._local_cluster = None
        self._local_controller: HarnessNodeController | None = None
        self._local_controller_lock = threading.Lock()

    def _controller(self) -> HarnessNodeController:
        cluster = require_cluster()
        with self._local_controller_lock:
            if self._local_cluster is cluster and self._local_controller is not None:
                return self._local_controller
            if (
                self._local_controller is not None
                and self._local_controller.stopped_node_ids()
            ):
                raise RuntimeError(
                    "Cannot replace local harness controller while nodes are stopped"
                )
            self._local_cluster = cluster
            self._local_controller = HarnessNodeController(cluster)
            return self._local_controller

    @staticmethod
    def _local_node_id(payload: dict) -> int:
        target = target_from_payload(payload)
        node_id = payload.get("node_id")
        if node_id is None and target is not None:
            node_id = target.node_id
        if node_id is None:
            raise ValueError(
                "StopStartNodeNemesis: local harness target must include node_id"
            )
        return int(node_id)

    def restore_local_harness_nodes(self) -> None:
        controller = self._local_controller
        if controller is not None:
            controller.restore_all()

    def local_stopped_node_ids(self) -> tuple[int, ...]:
        controller = self._local_controller
        return controller.stopped_node_ids() if controller is not None else ()

    def local_held_ports(self, node_id: int) -> tuple[int, ...]:
        controller = self._local_controller
        return controller.held_ports(node_id) if controller is not None else ()

    def _try_start_stopped(self) -> bool:
        if self._stopped_node is None:
            return False
        self._logger.info("Starting local node ic_port=%d", self._stopped_node.ic_port)
        _local_start_kikimr(self._stopped_node)
        self._stopped_node = None
        self.on_success_extract_fault()
        return True

    def extract_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        if local_harness_mode():
            node_id = self._local_node_id(payload)
            started = self._controller().start(node_id)
            if started is not None:
                self._logger.info(
                    "Started local harness node_id=%d pid=%d->%d log=%s",
                    started.node_id,
                    started.previous_pid,
                    started.current_pid,
                    started.log_file,
                )
            self.on_success_extract_fault()
            return
        if not self._try_start_stopped():
            self.on_success_extract_fault()

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        if local_harness_mode():
            node_id = self._local_node_id(payload)
            stopped = self._controller().stop(node_id)
            self._logger.info(
                "Stopped local harness node_id=%d pid=%d held_ports=%s",
                stopped.node_id,
                stopped.previous_pid,
                stopped.held_ports,
            )
            self.on_success_inject_fault()
            return
        cluster = require_external_cluster()
        if len(cluster.nodes) < 8:
            self._logger.info("Stop/start prohibited (< 8 nodes)")
            return
        if self._try_start_stopped():
            return
        node_id, node = _node_from_target(cluster, payload)
        if node is None:
            self._logger.error("StopStartNode: missing/unknown node_id in ChaosTarget (got %r)", node_id)
            return
        self._stopped_node = node
        self._logger.info("Stopping node_id=%s ic_port=%d", node_id, node.ic_port)
        _local_stop_kikimr(node)
        self.on_success_inject_fault()


class ClusterSuspendNodeNemesis(MonitoredAgentActor):
    """SIGSTOP / SIGCONT on a node/slot process (>=8 nodes). Requires ic_port or node/slot id."""

    def __init__(self) -> None:
        super().__init__(scope="node")
        self._suspended_ic_port: int | None = None
        self._local_cluster = None
        self._local_controller: HarnessNodeController | None = None
        self._local_controller_lock = threading.Lock()

    def _controller(self) -> HarnessNodeController:
        cluster = require_cluster()
        with self._local_controller_lock:
            if self._local_cluster is cluster and self._local_controller is not None:
                return self._local_controller
            if (
                self._local_controller is not None
                and self._local_controller.frozen_node_ids()
            ):
                raise RuntimeError(
                    "Cannot replace local harness controller while nodes are frozen"
                )
            self._local_cluster = cluster
            self._local_controller = HarnessNodeController(cluster)
            return self._local_controller

    @staticmethod
    def _local_node_id(payload: dict) -> int:
        target = target_from_payload(payload)
        node_id = payload.get("node_id")
        if node_id is None and target is not None:
            node_id = target.node_id
        if node_id is None:
            raise ValueError(
                "SuspendNodeNemesis: local harness target must include node_id"
            )
        return int(node_id)

    def restore_local_harness_nodes(self) -> None:
        controller = self._local_controller
        if controller is not None:
            controller.restore_all()

    def local_frozen_node_ids(self) -> tuple[int, ...]:
        controller = self._local_controller
        return controller.frozen_node_ids() if controller is not None else ()

    def _try_cont(self) -> bool:
        if self._suspended_ic_port is None:
            return False
        self._logger.info("SIGCONT local process ic_port=%d", self._suspended_ic_port)
        _local_send_signal(self._suspended_ic_port, signal.SIGCONT)
        self._suspended_ic_port = None
        self.on_success_extract_fault()
        return True

    def extract_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        if local_harness_mode():
            node_id = self._local_node_id(payload)
            unfrozen = self._controller().unfreeze(node_id)
            if unfrozen is not None:
                self._logger.info(
                    "Resumed local harness node_id=%d pid=%d",
                    unfrozen.node_id,
                    unfrozen.pid,
                )
            self.on_success_extract_fault()
            return
        if not self._try_cont():
            self.on_success_extract_fault()

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        if local_harness_mode():
            node_id = self._local_node_id(payload)
            frozen = self._controller().freeze(node_id)
            self._logger.info(
                "Suspended local harness node_id=%d pid=%d",
                frozen.node_id,
                frozen.pid,
            )
            self.on_success_inject_fault()
            return
        cluster = require_external_cluster()
        if len(cluster.nodes) < 8:
            self._logger.info("Suspend prohibited (< 8 nodes)")
            return
        if self._try_cont():
            return

        ic_port = _ic_port_from_target(payload)
        if ic_port is None:
            _, node = _node_from_target(cluster, payload)
            if node is not None:
                ic_port = int(node.ic_port)
        if ic_port is None:
            _, slot = _slot_from_target(cluster, payload)
            if slot is not None:
                ic_port = int(slot.ic_port)
        if ic_port is None:
            self._logger.error(
                "SuspendNode: ChaosTarget must include ic_port, node_id, or slot_idx"
            )
            return
        self._suspended_ic_port = ic_port
        self._logger.info("SIGSTOP process ic_port=%d", self._suspended_ic_port)
        _local_send_signal(self._suspended_ic_port, signal.SIGSTOP)
        self.on_success_inject_fault()


class ClusterRollingRestartNemesis(MonitoredAgentActor):
    """Agent-side rolling restart of a single ydb node via systemd.

    Each ``inject_fault`` call is one step of a cluster-wide rolling restart:
    the orchestrator-side ``RollingRestartNemesisPlanner`` picks which nodes
    must be restarted and dispatches an ``inject`` command per node; this
    runner consumes that command on the target host and performs the actual
    stop-wait-start cycle.

    Payload (from the planner):
        ``node_ic_port`` (int, required):
            Interconnect port of the node to restart. Used to derive the
            systemd unit name: ``kikimr-multi@<port>.service`` for slots,
            with one special case — port ``19001`` maps to ``kikimr.service``
            (storage node).
        ``duration`` (int, optional, default 60):
            How many seconds the unit stays stopped before being started
            back.

    Flow:
        1. ``sudo systemctl stop <unit>``.
        2. ``time.sleep(duration)`` — the node is down for ``duration`` sec.
        3. ``sudo systemctl start <unit>`` (always, via ``finally``) so the
           node is brought back even if the wait is interrupted.

    ``extract_fault`` is intentionally a no-op: the recovery is performed by
    the ``finally`` block in ``inject_fault`` itself.
    """

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        if not payload or "node_ic_port" not in payload:
            self._logger.error("inject_fault: missing node_ic_port in payload=%s", payload)
            return

        node_ic_port = int(payload["node_ic_port"])
        duration = int(payload.get("duration", 60))
        if node_ic_port == 19001:
            service = "kikimr.service"
        else:
            service = "kikimr-multi@{}.service".format(node_ic_port)
        self._logger.info(
            "Stopping %s for %ds", service, duration,
        )
        try:
            subprocess.run(
                ["sudo", "systemctl", "stop", service],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            self._logger.error("systemctl stop %s failed: %s", service, e)
            raise

        try:
            time.sleep(duration)
        finally:
            try:
                subprocess.run(
                    ["sudo", "systemctl", "start", service],
                    check=True,
                )
                self._logger.info("Restarted %s after %ds", service, duration)
            except subprocess.CalledProcessError as e:
                self._logger.error("systemctl start %s failed: %s", service, e)
                raise

        self.on_success_inject_fault()

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()
