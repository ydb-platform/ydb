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

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
    TargetKind,
)

logger = logging.getLogger(__name__)


class ImpactScope(enum.Enum):
    """Topology level a nemesis affects (orchestrator-side annotation)."""

    NODE = "node"
    DISK = "disk"
    RACK = "rack"
    DATACENTER = "datacenter"
    PILE = "pile"
    UNKNOWN = "unknown"


class GuardMode(enum.Enum):
    """How the failure-model guard treats a nemesis type.

    FULL           : pre-filter candidates AND record inject/extract impact after dispatch.
    PREFILTER_ONLY : pre-filter candidates only; impact is not recorded at dispatch.
    BYPASS         : guard does not touch this type at all.
    """

    FULL = "full"
    PREFILTER_ONLY = "prefilter_only"
    BYPASS = "bypass"


# Scopes that collapse to "one fail domain = the host's rack".
_HOST_LEVEL_SCOPES = frozenset(
    {ImpactScope.NODE, ImpactScope.DISK, ImpactScope.RACK}
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
    identity_key: str
    deadline: float | None


class FailureModelGuard:
    """Tracks impaired fail domains (racks) and filters candidates against the failure model.

    Safety is applied at plan time via :meth:`filter_safe` (no dispatch-time veto).
    ``record_inject`` / ``record_extract`` update the touched set after dispatch.
    Tablet targets contribute no fail-domain racks.

    Assumes a single active nemesis in MVP (no allocate/reserve locks across types).
    """

    def __init__(self, topology: ClusterTopologyModel) -> None:
        self._topology = topology
        self._lock = threading.Lock()
        self._impairments: list[_Impairment] = []

    @property
    def enabled(self) -> bool:
        return self._topology.guards

    # -- rack resolution ----------------------------------------------------

    def _racks_for_host(self, host: str, scope: ImpactScope) -> set[str]:
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

    def _racks_for_target(self, target: ChaosTarget, scope: ImpactScope) -> set[str]:
        if target.kind is TargetKind.TABLET:
            return set()
        if target.kind is TargetKind.DATACENTER or scope == ImpactScope.DATACENTER:
            dc = target.group_id or self._topology.dc_of(target.host)
            racks = self._topology.racks_in_dc(dc)
            return set(racks) if racks else {self._synthetic_key(target.host)}
        return self._racks_for_host(target.host, scope)

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

    def _touched_keys(self, now: float) -> set[str]:
        self._purge_expired(now)
        return {imp.identity_key for imp in self._impairments}

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

    def filter_safe(
        self,
        candidates: Iterable[ChaosTarget],
        scope: ImpactScope,
    ) -> list[ChaosTarget]:
        """Return a jointly safe subset of ``candidates`` under the failure budget.

        Candidates are checked in order. Each admitted target's fail domains are added to
        the running ``active`` set, so later candidates see earlier admissions (order-dependent).
        Already-touched identities (``ChaosTarget.identity_key``) are skipped.
        When the guard is disabled, returns all candidates unchanged.
        """
        candidates = list(candidates)
        if not self.enabled:
            return candidates
        now = time.monotonic()
        safe: list[ChaosTarget] = []
        with self._lock:
            active = self._active_racks(now)
            touched = self._touched_keys(now)
            for target in candidates:
                if target.identity_key() in touched:
                    continue
                racks = self._racks_for_target(target, scope)
                if not racks:
                    # e.g. TABLET — always safe for erasure budget
                    safe.append(target)
                    continue
                hypothetical = active | racks
                if self._is_tolerable(hypothetical):
                    safe.append(target)
                    active = hypothetical
        return safe

    def filter_safe_hosts(self, hosts: Iterable[str], scope: ImpactScope) -> list[str]:
        """Legacy wrapper: host strings → HOST targets → filter → host strings."""
        targets = self.filter_safe(
            [ChaosTarget.for_host(h) for h in hosts],
            scope,
        )
        return [t.host for t in targets]

    def record_inject(
        self,
        execution_id: str,
        target: ChaosTarget | str,
        scope: ImpactScope,
        recovery_sec: float | None = DEFAULT_RECOVERY_SEC,
    ) -> None:
        """Mark ``target``'s fail domain(s) impaired after a successful plan/dispatch.

        ``recovery_sec``: auto-release window; ``None`` holds until an explicit extract.
        ``target`` may be a hostname string (legacy) or :class:`ChaosTarget`.
        """
        if not self.enabled:
            return
        chaos_target = (
            target if isinstance(target, ChaosTarget) else ChaosTarget.for_host(str(target))
        )
        racks = self._racks_for_target(chaos_target, scope)
        if not racks and chaos_target.kind is TargetKind.TABLET:
            return
        deadline = None if recovery_sec is None else time.monotonic() + float(recovery_sec)
        with self._lock:
            self._impairments = [
                imp for imp in self._impairments if imp.execution_id != execution_id
            ]
            self._impairments.append(
                _Impairment(
                    execution_id=execution_id,
                    racks=racks,
                    identity_key=chaos_target.identity_key(),
                    deadline=deadline,
                )
            )

    def record_extract(
        self,
        execution_id: str,
        target: ChaosTarget | str,
        scope: ImpactScope,
    ) -> None:
        """Release the impairment recorded for ``execution_id`` (early recovery)."""
        if not self.enabled:
            return
        chaos_target = (
            target if isinstance(target, ChaosTarget) else ChaosTarget.for_host(str(target))
        )
        with self._lock:
            before = len(self._impairments)
            self._impairments = [
                imp for imp in self._impairments if imp.execution_id != execution_id
            ]
            if len(self._impairments) == before:
                # Untracked execution (e.g. after restart): drop by identity, not by rack subset
                # (same rack can hold unrelated impairments).
                key = chaos_target.identity_key()
                self._impairments = [
                    imp for imp in self._impairments if imp.identity_key != key
                ]

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            active = sorted(self._active_racks(now))
            touched = sorted(self._touched_keys(now))
            return {
                "enabled": self.enabled,
                "erasure": self._topology.tolerance.erasure,
                "impaired_racks": active,
                "touched_targets": touched,
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
