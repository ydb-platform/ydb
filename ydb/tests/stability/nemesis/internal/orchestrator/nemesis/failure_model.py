"""Failure-model guard: constrain nemesis chaos to the cluster's declared fault tolerance.

Parses ``cluster.yaml`` into a :class:`ClusterTopologyModel` and exposes a
:class:`FailureModelGuard` that vetoes/filters chaos exceeding the tolerated
simultaneous-failure budget (fail domain = rack, fail realm = datacenter):

    mirror-3-dc : 1 realm fully + 1 domain in another realm
    block-4-2   : any 2 domains
    none        : 0

Unrelated to the runner-side ``scope=`` metrics argument in ``monitored_actor.py``.

Each recorded impairment carries an optional recovery deadline, so faults that recover
without an explicit extract (systemd auto-restart after SIGKILL, self-healing rolling
restart) release their budget on a timer instead of piling up forever. ``recovery_sec=None``
holds an impairment until an explicit extract (toggle faults that stay down until next inject).
"""

from __future__ import annotations

import enum
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml

logger = logging.getLogger(__name__)


class ImpactScope(enum.Enum):
    """Topology level a nemesis affects (orchestrator-side annotation)."""

    NODE = "node"
    DISK = "disk"
    NETWORK = "network"
    RACK = "rack"
    DATACENTER = "datacenter"
    PILE = "pile"
    UNKNOWN = "unknown"


class GuardMode(enum.Enum):
    """How the failure-model guard treats a nemesis type.

    FULL           : pre-filter host list (B) AND post-veto + record impact (A).
    PREFILTER_ONLY : pre-filter host list only; never vetoed / recorded at dispatch.
    BYPASS         : guard does not touch this type at all.
    """

    FULL = "full"
    PREFILTER_ONLY = "prefilter_only"
    BYPASS = "bypass"


# Scopes that collapse to "one fail domain = the host's rack".
_HOST_LEVEL_SCOPES = frozenset(
    {ImpactScope.NODE, ImpactScope.DISK, ImpactScope.NETWORK, ImpactScope.RACK}
)

# Fallback recovery window (seconds) when a nemesis type has no ``auto_recovery_sec`` annotation.
# Conservatively longer than a systemd restart so a node stays "impaired" until it likely rejoined.
DEFAULT_RECOVERY_SEC: float = 120.0


@dataclass(frozen=True)
class FailureTolerance:
    """Max simultaneous failures allowed by an erasure mode.

    ``max_realm_plus_domain`` (mirror-3-dc): one whole realm may be lost, plus this many
    extra domains in *other* realms combined.
    ``max_domains`` (block-4-2): total domains allowed regardless of realm.
    """

    erasure: str
    kind: str  # "realm_plus_domain" | "domains" | "none" | "unknown"
    max_extra_domains: int = 0
    max_domains: int = 0

    @classmethod
    def from_erasure(cls, erasure: str) -> "FailureTolerance":
        e = (erasure or "").strip().lower()
        if e == "mirror-3-dc":
            return cls(erasure=e, kind="realm_plus_domain", max_extra_domains=1)
        if e == "block-4-2":
            return cls(erasure=e, kind="domains", max_domains=2)
        if e == "none":
            return cls(erasure=e, kind="none")
        return cls(erasure=e or "unknown", kind="unknown")

    @property
    def guards(self) -> bool:
        """True if this tolerance can actually make veto decisions."""
        return self.kind in ("realm_plus_domain", "domains", "none")


@dataclass
class HostTopology:
    host: str
    rack: str | None
    datacenter: str | None


class ClusterTopologyModel:
    """Parses ``cluster.yaml``: erasure mode and host -> rack -> datacenter mapping.

    Fails open (``guards`` is False) when the file is missing, unparsable, the erasure mode
    is unknown, or hosts have no rack/datacenter — the guard then never vetoes anything.
    """

    def __init__(self, yaml_path: str | None) -> None:
        self.yaml_path = yaml_path or ""
        self.hosts: dict[str, HostTopology] = {}
        self._rack_to_dc: dict[str, str | None] = {}
        self.tolerance = FailureTolerance.from_erasure("")
        self._parse()

    # -- parsing ------------------------------------------------------------

    def _parse(self) -> None:
        doc = self._load_doc(self.yaml_path)
        if not doc:
            logger.warning("FailureModel: cluster.yaml not loaded (%r); guard disabled", self.yaml_path)
            return
        self.tolerance = FailureTolerance.from_erasure(doc.get("static_erasure", ""))

        hosts = doc.get("hosts")
        if not isinstance(hosts, list) or not hosts:
            hosts = (doc.get("config") or {}).get("hosts", [])
        if not isinstance(hosts, list):
            hosts = []

        for h in hosts:
            if not isinstance(h, dict):
                continue
            name = h.get("name") or h.get("host")
            if not name:
                continue
            loc = h.get("location") or {}
            if not isinstance(loc, dict):
                loc = {}
            rack = loc.get("rack")
            dc = loc.get("data_center")
            rack = str(rack) if rack is not None else None
            dc = str(dc) if dc is not None else None
            self.hosts[name] = HostTopology(host=name, rack=rack, datacenter=dc)
            if rack is not None:
                self._rack_to_dc[rack] = dc

    @staticmethod
    def _load_doc(path: str) -> dict | None:
        if not path or not Path(path).is_file():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = yaml.safe_load(f.read())
        except Exception as e:  # noqa: BLE001 - fail open on any parse error
            logger.warning("FailureModel: failed to parse %r: %s", path, e)
            return None
        return doc if isinstance(doc, dict) else None

    # -- lookups ------------------------------------------------------------

    @property
    def guards(self) -> bool:
        """True only if we have a usable erasure model and topology to reason about."""
        return self.tolerance.guards and bool(self.hosts)

    def rack_of(self, host: str) -> str | None:
        t = self.hosts.get(host)
        return t.rack if t else None

    def dc_of(self, host: str) -> str | None:
        t = self.hosts.get(host)
        return t.datacenter if t else None

    def racks_in_dc(self, dc: str | None) -> set[str]:
        return {r for r, d in self._rack_to_dc.items() if d == dc}

    def dc_of_rack(self, rack: str) -> str | None:
        return self._rack_to_dc.get(rack)


@dataclass
class _Impairment:
    """One recorded fault footprint. ``deadline`` is a ``time.monotonic()`` value; ``None`` = held
    until an explicit extract."""

    execution_id: str
    racks: set[str]
    deadline: float | None


class FailureModelGuard:
    """Tracks impaired fail domains (racks) and validates new chaos against the failure model.

    Only ``FULL``-mode injects are recorded; ``PREFILTER_ONLY`` / ``BYPASS`` are not counted.
    Each impairment carries a recovery deadline so auto-recovering faults release their budget
    without an explicit extract; expired impairments are purged lazily on every read/write.
    """

    def __init__(self, topology: ClusterTopologyModel) -> None:
        self._topology = topology
        self._lock = threading.Lock()
        self._impairments: list[_Impairment] = []

    @property
    def enabled(self) -> bool:
        return self._topology.guards

    # -- rack resolution ----------------------------------------------------

    def _racks_for(self, host: str, scope: ImpactScope) -> set[str]:
        """Fail domains (rack keys) touched by injecting ``scope`` on ``host``."""
        if scope == ImpactScope.DATACENTER:
            dc = self._topology.dc_of(host)
            racks = self._topology.racks_in_dc(dc)
            return set(racks) if racks else {self._synthetic_key(host)}
        if scope in _HOST_LEVEL_SCOPES:
            rack = self._topology.rack_of(host)
            return {rack if rack is not None else self._synthetic_key(host)}
        # PILE / UNKNOWN: fall back to the host's rack (best effort).
        rack = self._topology.rack_of(host)
        return {rack if rack is not None else self._synthetic_key(host)}

    @staticmethod
    def _synthetic_key(host: str) -> str:
        return f"__host__:{host}"

    # -- impairment bookkeeping (call under _lock) --------------------------

    def _purge_expired(self, now: float) -> None:
        self._impairments = [
            imp for imp in self._impairments if imp.deadline is None or imp.deadline > now
        ]

    def _active_racks(self, now: float) -> set[str]:
        self._purge_expired(now)
        active: set[str] = set()
        for imp in self._impairments:
            active |= imp.racks
        return active

    # -- tolerance check ----------------------------------------------------

    def _is_tolerable(self, impaired_racks: set[str]) -> bool:
        tol = self._topology.tolerance
        n = len(impaired_racks)
        if tol.kind == "none":
            return n == 0
        if tol.kind == "domains":
            return n <= tol.max_domains
        if tol.kind == "realm_plus_domain":
            # One realm may be lost entirely, plus max_extra_domains in other realms combined.
            by_dc: dict[str | None, int] = {}
            for rack in impaired_racks:
                dc = self._topology.dc_of_rack(rack) if not rack.startswith("__host__:") else None
                by_dc[dc] = by_dc.get(dc, 0) + 1
            if not by_dc:
                return True
            sacrificial = max(by_dc.values())  # the realm we allow to fail fully
            remaining = n - sacrificial
            return remaining <= tol.max_extra_domains
        # unknown -> fail open
        return True

    # -- public API ---------------------------------------------------------

    def can_inject(self, host: str, scope: ImpactScope) -> bool:
        if not self.enabled:
            return True
        now = time.monotonic()
        with self._lock:
            hypothetical = self._active_racks(now) | self._racks_for(host, scope)
            return self._is_tolerable(hypothetical)

    def filter_safe_hosts(self, hosts: Iterable[str], scope: ImpactScope) -> list[str]:
        if not self.enabled:
            return list(hosts)
        now = time.monotonic()
        safe: list[str] = []
        with self._lock:
            active = self._active_racks(now)
            for host in hosts:
                hypothetical = active | self._racks_for(host, scope)
                if self._is_tolerable(hypothetical):
                    safe.append(host)
        return safe

    def record_inject(
        self,
        execution_id: str,
        host: str,
        scope: ImpactScope,
        recovery_sec: float | None = DEFAULT_RECOVERY_SEC,
    ) -> None:
        """Mark ``host``'s fail domain(s) impaired.

        ``recovery_sec``: auto-release window; after it elapses the impairment is dropped even
        without an extract. ``None`` holds it until an explicit extract (toggle faults).
        """
        if not self.enabled:
            return
        racks = self._racks_for(host, scope)
        deadline = None if recovery_sec is None else time.monotonic() + float(recovery_sec)
        with self._lock:
            # Idempotent re-inject: replace any prior record for this execution.
            self._impairments = [
                imp for imp in self._impairments if imp.execution_id != execution_id
            ]
            self._impairments.append(
                _Impairment(execution_id=execution_id, racks=racks, deadline=deadline)
            )

    def record_extract(self, execution_id: str, host: str, scope: ImpactScope) -> None:
        """Release the impairment recorded for ``execution_id`` (early recovery)."""
        if not self.enabled:
            return
        with self._lock:
            before = len(self._impairments)
            self._impairments = [
                imp for imp in self._impairments if imp.execution_id != execution_id
            ]
            if len(self._impairments) == before:
                # Untracked execution (e.g. after restart): best-effort drop by racks.
                racks = self._racks_for(host, scope)
                self._impairments = [
                    imp for imp in self._impairments if not (imp.racks <= racks)
                ]

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            active = sorted(self._active_racks(now))
            return {
                "enabled": self.enabled,
                "erasure": self._topology.tolerance.erasure,
                "impaired_racks": active,
                "tracked_executions": len(self._impairments),
            }


__all__ = [
    "ImpactScope",
    "GuardMode",
    "FailureTolerance",
    "HostTopology",
    "ClusterTopologyModel",
    "FailureModelGuard",
    "DEFAULT_RECOVERY_SEC",
]
