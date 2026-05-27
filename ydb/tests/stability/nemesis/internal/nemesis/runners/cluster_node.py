# Adapted from ydb/tests/tools/nemesis/library/node.py
#
# All runners execute **locally** on the agent host.  The orchestrator picks
# the target host and dispatches the command; the agent runs subprocess calls
# against the local kikimr / kikimr-multi@ services.

from __future__ import annotations

import signal
import subprocess
import time
from typing import Any

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


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
    """``systemctl start`` for the local kikimr / kikimr-multi@ service."""
    slot_id = _kikimr_slot_id(node)
    if slot_id is None:
        subprocess.run("sudo service kikimr start", shell=True, check=True)
        return
    slot_dir = "/Berkanavt/kikimr_{slot}".format(slot=slot_id)
    slot_cfg = slot_dir + "/slot_cfg"
    env_txt = slot_dir + "/env.txt"
    cfg = """\
tenant=/Root/db1
grpc={grpc}
mbus={mbus}
ic={ic}
mon={mon}""".format(
        mbus=node.mbus_port,
        grpc=node.grpc_port,
        mon=node.mon_port,
        ic=node.ic_port,
    )
    subprocess.run(["sudo", "mkdir", "-p", slot_dir], check=True)
    subprocess.run(["sudo", "touch", env_txt], check=True)
    p = subprocess.Popen(
        ["sudo", "tee", slot_cfg],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    _out, _err = p.communicate(cfg.encode("utf-8"))
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, "sudo tee slot_cfg", None)
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


def _resolve_local_node(cluster):
    """Find the cluster node object whose host matches the local FQDN / hostname."""
    import socket

    fqdn = socket.getfqdn()
    hostname = socket.gethostname()
    for node in cluster.nodes.values():
        if node.host in (fqdn, hostname) or node.host.split(".")[0] == hostname.split(".")[0]:
            return node
    return None


def _resolve_local_slots(cluster):
    """Return list of slot objects whose host matches the local FQDN / hostname."""
    import socket

    fqdn = socket.getfqdn()
    hostname = socket.gethostname()
    result = []
    for slot in cluster.slots.values():
        if slot.host in (fqdn, hostname) or slot.host.split(".")[0] == hostname.split(".")[0]:
            result.append(slot)
    return result


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------


class ClusterKillSlotDaemonNemesis(MonitoredAgentActor):
    """Kill one random local slot daemon via subprocess."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        local_slots = _resolve_local_slots(cluster)
        if not local_slots:
            self._logger.warning("No local slots to kill")
            return
        import random
        slot = random.choice(local_slots)
        try:
            self._logger.info("Killing local slot daemon ic_port=%d", slot.ic_port)
            _local_kill_daemon_and_process(int(slot.ic_port))
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Kill slot failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterKillNodeDaemonNemesis(MonitoredAgentActor):
    """Kill the local kikimr node daemon via subprocess."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        node = _resolve_local_node(cluster)
        if node is None:
            self._logger.warning("No local node found in cluster — cannot kill")
            return
        try:
            self._logger.info("Killing local node daemon ic_port=%d", node.ic_port)
            _local_kill_daemon_and_process(int(node.ic_port))
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Kill node daemon failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterSerialKillNodeNemesis(MonitoredAgentActor):
    """Kill the local node daemon; ``node_id`` + ``sleep_before`` come from the orchestrator payload."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        delay = float(payload.get("sleep_before", 0) or 0)
        if delay > 0:
            self._logger.info("Serial kill node: sleep %.1fs before kill", delay)
            time.sleep(delay)
        cluster = require_external_cluster()
        # Resolve the target node from payload or fall back to local node
        node_id = payload.get("node_id")
        if node_id is not None:
            node = cluster.nodes.get(node_id)
        else:
            node = _resolve_local_node(cluster)
        if node is None:
            self._logger.error("Serial kill node: no daemon (node_id=%s)", node_id)
            return
        try:
            self._logger.info("Serial kill node daemon ic_port=%d", node.ic_port)
            _local_kill_daemon_and_process(int(node.ic_port))
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Serial kill node failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterSerialKillSlotsNemesis(MonitoredAgentActor):
    """Kill the local slot daemon; ``slot_idx`` + ``sleep_before`` from orchestrator."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        delay = float(payload.get("sleep_before", 0) or 0)
        if delay > 0:
            self._logger.info("Serial kill slot: sleep %.1fs before kill", delay)
            time.sleep(delay)
        cluster = require_external_cluster()
        # Resolve the target slot from payload or fall back to a random local slot
        slot_idx = payload.get("slot_idx")
        if slot_idx is not None:
            slot = cluster.slots.get(slot_idx)
        else:
            local_slots = _resolve_local_slots(cluster)
            import random
            slot = random.choice(local_slots) if local_slots else None
        if slot is None:
            self._logger.error("Serial kill slot: no daemon (slot_idx=%s)", slot_idx)
            return
        try:
            self._logger.info("Serial kill slot daemon ic_port=%d", slot.ic_port)
            _local_kill_daemon_and_process(int(slot.ic_port))
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Serial kill slot failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterStopStartNodeNemesis(MonitoredAgentActor):
    """Stop the local node process; next inject starts it again (>=8 nodes)."""

    def __init__(self) -> None:
        super().__init__(scope="node")
        self._stopped_node = None

    def _try_start_stopped(self) -> bool:
        if self._stopped_node is None:
            return False
        self._logger.info("Starting local node ic_port=%d", self._stopped_node.ic_port)
        _local_start_kikimr(self._stopped_node)
        self._stopped_node = None
        self.on_success_extract_fault()
        return True

    def extract_fault(self, payload=None) -> None:
        del payload
        if not self._try_start_stopped():
            self.on_success_extract_fault()

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        if len(cluster.nodes) < 8:
            self._logger.info("Stop/start prohibited (< 8 nodes)")
            return
        if self._try_start_stopped():
            return
        node = _resolve_local_node(cluster)
        if node is None:
            self._logger.warning("No local node found in cluster — cannot stop")
            return
        self._stopped_node = node
        self._logger.info("Stopping local node ic_port=%d", node.ic_port)
        _local_stop_kikimr(node)
        self.on_success_inject_fault()


class ClusterSuspendNodeNemesis(MonitoredAgentActor):
    """SIGSTOP / SIGCONT on local node or slot (>=8 nodes)."""

    def __init__(self) -> None:
        super().__init__(scope="node")
        self._suspended_ic_port: int | None = None

    def _try_cont(self) -> bool:
        if self._suspended_ic_port is None:
            return False
        self._logger.info("SIGCONT local process ic_port=%d", self._suspended_ic_port)
        _local_send_signal(self._suspended_ic_port, signal.SIGCONT)
        self._suspended_ic_port = None
        self.on_success_extract_fault()
        return True

    def extract_fault(self, payload=None) -> None:
        del payload
        if not self._try_cont():
            self.on_success_extract_fault()

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        if len(cluster.nodes) < 8:
            self._logger.info("Suspend prohibited (< 8 nodes)")
            return
        if self._try_cont():
            return
        import random
        local_node = _resolve_local_node(cluster)
        local_slots = _resolve_local_slots(cluster)
        pool = []
        if local_node is not None:
            pool.append(local_node)
        pool.extend(local_slots)
        if not pool:
            self._logger.warning("No local processes to suspend")
            return
        target = random.choice(pool)
        self._suspended_ic_port = int(target.ic_port)
        self._logger.info("SIGSTOP local process ic_port=%d", self._suspended_ic_port)
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
