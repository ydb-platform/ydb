# Adapted from ydb/tests/tools/nemesis/library/node.py

from __future__ import annotations

import collections
import itertools
import random
import signal
import time

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


class ClusterKillSlotDaemonNemesis(MonitoredAgentActor):
    """Kill one random slot daemon."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        daemons = list(cluster.slots.values())
        if not daemons:
            self._logger.warning("No slots to kill")
            return
        d = random.choice(daemons)
        try:
            d.kill()
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Kill slot failed: %s", e)

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterKillNodeDaemonNemesis(MonitoredAgentActor):
    """Kill one random kikimr node daemon (cluster harness object)."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        daemons = list(cluster.nodes.values())
        if not daemons:
            return
        d = random.choice(daemons)
        try:
            d.kill()
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Kill node daemon failed: %s", e)

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterSerialKillNodeNemesis(MonitoredAgentActor):
    """Kill the same node daemon per tick; ``node_id`` + ``sleep_before`` come from the orchestrator payload."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        delay = float(payload.get("sleep_before", 0) or 0)
        if delay > 0:
            self._logger.info("Serial kill node: sleep %.1fs before kill", delay)
            time.sleep(delay)
        cluster = require_external_cluster()
        node_id = payload.get("node_id")
        if node_id is not None:
            d = cluster.nodes.get(node_id)
        else:
            daemons = list(cluster.nodes.values())
            d = random.choice(daemons) if daemons else None
        if d is None:
            self._logger.error("Serial kill node: no daemon (node_id=%s)", node_id)
            return
        try:
            self._logger.info("Serial kill node daemon %s", d)
            d.kill()
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Serial kill node failed: %s", e)

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterSerialKillSlotsNemesis(MonitoredAgentActor):
    """Kill the same slot daemon; ``slot_idx`` + ``sleep_before`` from orchestrator."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None) -> None:
        payload = payload if isinstance(payload, dict) else {}
        delay = float(payload.get("sleep_before", 0) or 0)
        if delay > 0:
            self._logger.info("Serial kill slot: sleep %.1fs before kill", delay)
            time.sleep(delay)
        cluster = require_external_cluster()
        slot_idx = payload.get("slot_idx")
        if slot_idx is not None:
            d = cluster.slots.get(slot_idx)
        else:
            daemons = list(cluster.slots.values())
            d = random.choice(daemons) if daemons else None
        if d is None:
            self._logger.error("Serial kill slot: no daemon (slot_idx=%s)", slot_idx)
            return
        try:
            self._logger.info("Serial kill slot daemon %s", d)
            d.kill()
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Serial kill slot failed: %s", e)

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterStopStartNodeNemesis(MonitoredAgentActor):
    """Stop one node process; next inject starts it again (>=8 nodes)."""

    def __init__(self) -> None:
        super().__init__(scope="node")
        self._current = None

    def _try_start_stopped(self) -> bool:
        if self._current is None:
            return False
        self._logger.info("Starting node %s", self._current)
        self._current.start()
        self._current = None
        self.on_success_extract_fault()
        return True

    def extract_fault(self, payload=None) -> None:
        del payload
        if not self._try_start_stopped():
            self.on_success_extract_fault()

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        processes = list(cluster.nodes.values())
        if len(processes) < 8:
            self._logger.info("Stop/start prohibited (< 8 nodes)")
            return
        if self._try_start_stopped():
            return
        self._current = random.choice(processes)
        self._logger.info("Stopping node %s", self._current)
        self._current.stop()
        self.on_success_inject_fault()


class ClusterSuspendNodeNemesis(MonitoredAgentActor):
    """SIGSTOP / SIGCONT on random node or slot (>=8 nodes)."""

    def __init__(self) -> None:
        super().__init__(scope="node")
        self._wake = None

    def _try_cont(self) -> bool:
        if self._wake is None:
            return False
        self._logger.info("SIGCONT %s", self._wake)
        self._wake.send_signal(signal.SIGCONT)
        self._wake = None
        self.on_success_extract_fault()
        return True

    def extract_fault(self, payload=None) -> None:
        del payload
        if not self._try_cont():
            self.on_success_extract_fault()

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        nodes = list(cluster.nodes.values())
        if len(nodes) < 8:
            self._logger.info("Suspend prohibited (< 8 nodes)")
            return
        if self._try_cont():
            return
        pool = list(cluster.nodes.values()) + list(cluster.slots.values())
        self._wake = random.choice(pool)
        self._logger.info("SIGSTOP %s", self._wake)
        self._wake.send_signal(signal.SIGSTOP)
        self.on_success_inject_fault()


class ClusterRollingUpdateNemesis(MonitoredAgentActor):
    """Rolling switch_version + kill node + kill slots on that host."""

    def __init__(self) -> None:
        super().__init__(scope="node")
        self._prepared = False
        self._buckets: dict[int, collections.deque] | None = None
        self._step_id = None
        self._slots_by_host: dict | None = None

    def _ensure(self, cluster) -> None:
        if self._prepared:
            return
        self._buckets = {0: collections.deque(cluster.nodes.values()), 1: collections.deque()}
        self._step_id = itertools.count(1)
        self._slots_by_host = collections.defaultdict(list)
        for slot in cluster.slots.values():
            self._slots_by_host[slot.host].append(slot)
        self._prepared = True

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        self._ensure(cluster)
        assert self._buckets is not None and self._slots_by_host is not None
        self._logger.info("Rolling update step %d", next(self._step_id))
        bucket_id = 0 if len(self._buckets[0]) >= len(self._buckets[1]) else 1
        node = self._buckets[bucket_id].popleft()
        self._buckets[bucket_id ^ 1].append(node)
        try:
            node.switch_version()
            node.kill()
            for slot in self._slots_by_host.get(node.host, []):
                try:
                    slot.kill()
                except Exception as e:
                    self._logger.error("slot kill failed: %s", e)
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("rolling update failed: %s", e)

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()
