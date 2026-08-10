"""Failure-model guard: keep chaos within the cluster's declared fault tolerance.

Budget per erasure mode from ``cluster.yaml`` (``static_erasure`` or ``erasure``, top level or under
``config:`` — see :func:`_find_erasure`)::

    mirror-3-dc : 1 realm fully + 1 domain in another realm
    block-4-2   : any 2 domains
    none        : 0

A fail domain is ``"<dc>/<rack>"`` (:func:`fail_domain_key`): rack labels repeat across DCs, so the
realm must be part of the key. Slot (dynamic node) kills cost no redundancy and draw from a separate
budget instead: ≤30% of the cluster's slots at once.

An unusable config raises :class:`FailureModelConfigError` and the orchestrator refuses to start
(``app.require_failure_model_or_die``) — there is no unguarded mode.

Unrelated to the runner-side ``scope=`` metrics argument in ``monitored_actor.py``.
"""

from __future__ import annotations

import enum
import logging
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
    TargetKind,
)

logger = logging.getLogger(__name__)


class FailureModelConfigError(RuntimeError):
    """``cluster.yaml`` cannot back a failure model, so the orchestrator must not start."""


class ImpactScope(enum.Enum):
    """Topology level a nemesis affects (orchestrator-side annotation)."""

    NODE = "node"
    SLOT = "slot"
    DISK = "disk"
    DATACENTER = "datacenter"
    PILE = "pile"
    UNKNOWN = "unknown"


class GuardMode(enum.Enum):
    """FULL: filtered and accounted for. BYPASS: costs no budget (tablet chaos)."""

    FULL = "full"
    BYPASS = "bypass"


# Extract-trigger / timeout base when auto_recovery_sec is unset — not a budget-release timer.
DEFAULT_RECOVERY_SEC: float = 120.0

# Share of the cluster's slots allowed down at once.
DEFAULT_SLOT_FRACTION: float = 0.3

_UNKNOWN_DC = "?"                      # host with no ``location.data_center``
_SYNTHETIC_DOMAIN_PREFIX = "__host__:"  # host with no rack in ``cluster.yaml``

# ``ydb/tools/cfg`` accepts either spelling of the erasure mode -- see the ``anyOf`` on
# CLUSTER_DETAILS_SCHEMA in ``ydb/tools/cfg/validation.py`` -- and a V2 config renames
# ``static_erasure`` to ``erasure`` (``kikimr_config.py``). Both spellings may sit at the top level
# or under ``config:``, the same two places ``hosts`` is looked up.
_ERASURE_KEYS = ("static_erasure", "erasure")


def _find_erasure(doc: dict) -> tuple[str, str | None]:
    """``(erasure mode, the key it came from)``, or ``("", None)`` when no spelling is present."""
    for root_name, root in (("", doc), ("config", doc.get("config"))):
        if not isinstance(root, dict):
            continue
        for key in _ERASURE_KEYS:
            value = root.get(key)
            if value is not None:
                return value, f"{root_name}.{key}" if root_name else key
    return "", None


def _slots_within_budget(active_slots: int, add_slots: int, max_slots: int) -> bool:
    """Slot-budget rule, shared by the guard and :class:`BudgetView`. ``max_slots<=0`` never blocks."""
    if add_slots <= 0 or max_slots <= 0:
        return True
    return active_slots + add_slots <= max_slots


def fail_domain_key(datacenter: str | None, rack: str) -> str:
    """``"<dc>/<rack>"`` — rack labels are only unique inside a datacenter."""
    return f"{datacenter or _UNKNOWN_DC}/{rack}"


@dataclass(frozen=True)
class Footprint:
    """What one fault consumes: fail-domain keys and/or slots. A tablet fault consumes neither."""

    racks: frozenset[str] = frozenset()
    slots: int = 0

    def __bool__(self) -> bool:
        """True if this represents a real impairment (used to decide fact-based recovery)."""
        return bool(self.racks) or self.slots > 0


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
    """``cluster.yaml`` -> erasure mode + host/rack/datacenter map, or :class:`FailureModelConfigError`.

    ``rack_of`` / ``dc_of`` give the raw ``location`` labels (inventory, UI); the guard reasons in
    fail-domain keys via ``domain_of`` / ``domains_in_dc`` / ``dc_of_domain``.
    """

    def __init__(self, yaml_path: str | None) -> None:
        self.yaml_path = yaml_path or ""
        self.hosts: dict[str, HostTopology] = {}
        # fail-domain key ("<dc>/<rack>") -> datacenter
        self._domain_to_dc: dict[str, str | None] = {}
        self.tolerance = FailureTolerance.from_erasure("")
        self._parse()

    # -- parsing ------------------------------------------------------------

    def _parse(self) -> None:
        doc = self._load_doc(self.yaml_path)
        erasure, erasure_key = _find_erasure(doc)
        self.tolerance = FailureTolerance.from_erasure(erasure)
        if not self.tolerance.guards:
            if erasure_key is None:
                raise FailureModelConfigError(
                    f"{self.yaml_path}: no erasure mode found — looked for "
                    f"{' / '.join(_ERASURE_KEYS)} at the top level and under 'config:' "
                    f"(expected mirror-3-dc, block-4-2 or none)"
                )
            raise FailureModelConfigError(
                f"{self.yaml_path}: {erasure_key}={erasure!r} is not a mode the "
                f"failure model understands (expected mirror-3-dc, block-4-2 or none)"
            )

        hosts = doc.get("hosts")
        if not isinstance(hosts, list) or not hosts:
            hosts = (doc.get("config") or {}).get("hosts", [])
        if not isinstance(hosts, list) or not hosts:
            raise FailureModelConfigError(f"{self.yaml_path}: no 'hosts' list to build a topology from")

        needs_realm = self.tolerance.kind == "realm_plus_domain"
        for h in hosts:
            if not isinstance(h, dict):
                raise FailureModelConfigError(f"{self.yaml_path}: host entry is not a mapping: {h!r}")
            name = h.get("name") or h.get("host")
            if not name:
                raise FailureModelConfigError(f"{self.yaml_path}: host entry without name/host: {h!r}")
            loc = h.get("location")
            if not isinstance(loc, dict):
                raise FailureModelConfigError(f"{self.yaml_path}: host {name} has no 'location' mapping")
            rack = loc.get("rack")
            dc = loc.get("data_center")
            if rack is None:
                raise FailureModelConfigError(
                    f"{self.yaml_path}: host {name} has no location.rack — the fail domain of every "
                    f"host must be known to bound chaos"
                )
            if needs_realm and dc is None:
                raise FailureModelConfigError(
                    f"{self.yaml_path}: host {name} has no location.data_center, required by "
                    f"{self.tolerance.erasure} (one realm may be sacrificed, so realms must be known)"
                )
            rack = str(rack)
            dc = str(dc) if dc is not None else None
            self.hosts[name] = HostTopology(host=name, rack=rack, datacenter=dc)
            self._domain_to_dc[fail_domain_key(dc, rack)] = dc

        logger.info(
            "FailureModel: %s, %d host(s), %d fail domain(s) from %s",
            self.tolerance.erasure, len(self.hosts), len(self._domain_to_dc), self.yaml_path,
        )

    @staticmethod
    def _load_doc(path: str) -> dict:
        if not path:
            raise FailureModelConfigError(
                "no cluster.yaml path (set YAML_CONFIG_LOCATION); the failure-model guard needs the "
                "cluster topology to bound chaos"
            )
        if not Path(path).is_file():
            raise FailureModelConfigError(f"cluster.yaml not found at {path!r}")
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = yaml.safe_load(f.read())
        except Exception as e:
            raise FailureModelConfigError(f"cannot parse {path!r}: {e}") from e
        if not isinstance(doc, dict):
            raise FailureModelConfigError(f"{path!r}: expected a YAML mapping, got {type(doc).__name__}")
        return doc

    # -- lookups ------------------------------------------------------------

    @property
    def guards(self) -> bool:
        """Always True (an unusable config raises); reported through ``snapshot()`` / the API."""
        return self.tolerance.guards and bool(self.hosts)

    def rack_of(self, host: str) -> str | None:
        """Raw ``location.rack`` label, not a fail-domain key."""
        t = self.hosts.get(host)
        return t.rack if t else None

    def dc_of(self, host: str) -> str | None:
        t = self.hosts.get(host)
        return t.datacenter if t else None

    def domain_of(self, host: str) -> str | None:
        """Fail-domain key of ``host``, or None if it is not in ``cluster.yaml``."""
        t = self.hosts.get(host)
        if t is None or t.rack is None:
            return None
        return fail_domain_key(t.datacenter, t.rack)

    def domains_in_dc(self, dc: str | None) -> set[str]:
        """Fail-domain keys of realm ``dc``."""
        return {d for d, ddc in self._domain_to_dc.items() if ddc == dc}

    def dc_of_domain(self, domain: str) -> str | None:
        return self._domain_to_dc.get(domain)


class BudgetView:
    """Read-only budget snapshot: one lock for a whole menu of candidates.

    Advisory — :meth:`FailureModelGuard.reserve` re-checks atomically before anything is injected.
    """

    __slots__ = ("impaired_racks", "impaired_slots", "touched", "_is_tolerable", "_max_slots")

    def __init__(
        self,
        *,
        impaired_racks: frozenset[str],
        impaired_slots: int,
        touched: frozenset[str],
        is_tolerable,
        max_slots: int,
    ) -> None:
        self.impaired_racks = impaired_racks
        self.impaired_slots = impaired_slots
        self.touched = touched
        self._is_tolerable = is_tolerable
        self._max_slots = max_slots

    def fits(self, footprint: Footprint) -> bool:
        return self._is_tolerable(
            set(self.impaired_racks) | set(footprint.racks)
        ) and _slots_within_budget(self.impaired_slots, footprint.slots, self._max_slots)


@dataclass
class _Impairment:
    """One recorded fault; held until release / record_extract (no timer expiry)."""

    execution_id: str
    racks: set[str]
    identity_key: str
    slots: int = 0


class FailureModelGuard:
    """Impaired fail domains + slots. Lease API (reserve/release) or plan-then-record; no timer expiry."""

    def __init__(
        self,
        topology: ClusterTopologyModel,
        *,
        total_slots: int = 0,
        slot_fraction: float = DEFAULT_SLOT_FRACTION,
        metrics=None,
    ) -> None:
        self._topology = topology
        self._lock = threading.Lock()
        self._impairments: list[_Impairment] = []
        self._metrics = metrics
        slots = max(0, int(total_slots))
        self._total_slots = slots
        # ≥1 on small clusters; 0 (unknown) never blocks.
        self._max_slots = max(1, int(slots * float(slot_fraction))) if slots else 0

    @property
    def enabled(self) -> bool:
        """Always True — an unusable topology raises at construction. Reported in :meth:`snapshot`."""
        return self._topology.guards

    def set_metrics(self, metrics) -> None:
        """Attach orchestrator metrics emitter (late bind from app init)."""
        self._metrics = metrics

    # -- rack resolution ----------------------------------------------------

    def _racks_for_host(self, host: str, scope: ImpactScope) -> set[str]:
        """Fail domains touched by injecting ``scope`` on ``host``."""
        if scope == ImpactScope.DATACENTER:
            dc = self._topology.dc_of(host)
            domains = self._topology.domains_in_dc(dc)
            return set(domains) if domains else {self._synthetic_key(host)}
        # Everything else collapses to the host's own domain (SLOT is routed to the slot budget).
        domain = self._topology.domain_of(host)
        return {domain if domain is not None else self._synthetic_key(host)}

    @staticmethod
    def _is_slot(target: ChaosTarget, scope: ImpactScope) -> bool:
        """A slot fault: draws from the slot budget, not from a fail domain."""
        return scope is ImpactScope.SLOT or target.kind is TargetKind.SLOT

    def _racks_for_target(self, target: ChaosTarget, scope: ImpactScope) -> set[str]:
        if target.kind is TargetKind.TABLET or self._is_slot(target, scope):
            return set()
        if target.kind is TargetKind.DATACENTER or scope == ImpactScope.DATACENTER:
            dc = target.group_id or self._topology.dc_of(target.host)
            domains = self._topology.domains_in_dc(dc)
            return set(domains) if domains else {self._synthetic_key(target.host)}
        return self._racks_for_host(target.host, scope)

    def _synthetic_key(self, host: str) -> str:
        """Fail-domain key for a host with no rack in ``cluster.yaml``; still namespaced by realm."""
        return f"{_SYNTHETIC_DOMAIN_PREFIX}{fail_domain_key(self._topology.dc_of(host), host)}"

    def _realm_of_domain(self, domain: str) -> str | None:
        """Realm of a fail-domain key, synthetic ones included."""
        if domain.startswith(_SYNTHETIC_DOMAIN_PREFIX):
            dc = domain[len(_SYNTHETIC_DOMAIN_PREFIX):].split("/", 1)[0]
            return None if dc == _UNKNOWN_DC else dc
        return self._topology.dc_of_domain(domain)

    def footprint_for(self, target: ChaosTarget, scope: ImpactScope) -> Footprint:
        """Empty for tablets, one slot for slot kills, the host's fail domain(s) otherwise."""
        return Footprint(
            racks=frozenset(self._racks_for_target(target, scope)),
            slots=1 if self._is_slot(target, scope) else 0,
        )

    # -- impairment bookkeeping (call under _lock) --------------------------

    def _active_racks(self) -> set[str]:
        active: set[str] = set()
        for imp in self._impairments:
            active |= imp.racks
        return active

    def _active_slots(self) -> int:
        return sum(imp.slots for imp in self._impairments)

    def _slots_ok(self, add_slots: int) -> bool:
        """Whether ``add_slots`` more slots stay within the budget."""
        if add_slots <= 0 or self._max_slots <= 0:
            return True
        return _slots_within_budget(self._active_slots(), add_slots, self._max_slots)

    def _touched_keys(self) -> set[str]:
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
            # One realm may be lost entirely, plus max_extra_domains elsewhere.
            by_dc: dict[str | None, int] = {}
            for domain in impaired_racks:
                realm = self._realm_of_domain(domain)
                by_dc[realm] = by_dc.get(realm, 0) + 1
            if not by_dc:
                return True
            sacrificial = max(by_dc.values())  # the realm we allow to fail fully
            remaining = n - sacrificial
            return remaining <= tol.max_extra_domains
        return True  # unreachable: unknown modes never get past parsing

    # -- public API ---------------------------------------------------------

    def filter_safe(
        self,
        candidates: Iterable[ChaosTarget],
        scope: ImpactScope,
        *,
        jointly: bool = True,
    ) -> list[ChaosTarget]:
        """Safe subset of ``candidates`` against the current fail-domain budget.

        Skips already-impaired identities. Fail domains only — a slot candidate has none, so it is
        always admitted here and :meth:`reserve` is what refuses it.

        ``jointly=True`` (default): each admission narrows the budget for the next — a maximal
        packing in candidate order (first hosts win). Use when a planner may act on several
        returned targets in one step.

        ``jointly=False``: each candidate is checked only against the current impairments — for
        single-inject ticks that then ``random.choice`` among the result.
        """
        candidates = list(candidates)
        safe: list[ChaosTarget] = []
        with self._lock:
            active = self._active_racks()
            touched = self._touched_keys()
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
                    if jointly:
                        active = hypothetical
        return safe

    # -- lease-based budget API ---------------------------------------------

    def _snapshot_unlocked(self) -> dict:
        active = sorted(self._active_racks())
        return {
            "enabled": self.enabled,
            "erasure": self._topology.tolerance.erasure,
            "impaired_racks": active,
            "impaired_slots": self._active_slots(),
            "total_slots": self._total_slots,
            "max_slots": self._max_slots,
            "tracked_executions": len(self._impairments),
        }

    def reserve(
        self,
        footprint: Footprint,
        identity_key: str | None = None,
        *,
        target: ChaosTarget | None = None,
        nemesis_type: str | None = None,
        source: str | None = None,
    ) -> str | None:
        """Claim ``footprint``; return lease id or None. Held until :meth:`release`."""
        metrics = self._metrics
        with self._lock:
            if not self._is_tolerable(self._active_racks() | set(footprint.racks)):
                snap = self._snapshot_unlocked() if metrics is not None else None
                rejected = True
                lease_id = None
                key = identity_key
            elif not self._slots_ok(footprint.slots):
                snap = self._snapshot_unlocked() if metrics is not None else None
                rejected = True
                lease_id = None
                key = identity_key
            else:
                rejected = False
                lease_id = uuid.uuid4().hex
                key = identity_key or f"lease:{lease_id}"
                self._impairments.append(
                    _Impairment(
                        execution_id=lease_id,
                        racks=set(footprint.racks),
                        identity_key=key,
                        slots=footprint.slots,
                    )
                )
                snap = self._snapshot_unlocked() if metrics is not None else None
        if metrics is None:
            return lease_id
        if rejected:
            metrics.budget_acquire_rejected(
                footprint=footprint,
                identity_key=key,
                budget_after=snap,
                target=target,
                nemesis_type=nemesis_type,
                source=source,
            )
            return None
        metrics.budget_acquired(
            lease_id=lease_id,
            footprint=footprint,
            identity_key=key,
            budget_after=snap,
            target=target,
            nemesis_type=nemesis_type,
            source=source,
        )
        return lease_id

    def budget_view(self) -> BudgetView:
        """Snapshot both budgets and the impaired identities under one lock."""
        with self._lock:
            return BudgetView(
                impaired_racks=frozenset(self._active_racks()),
                impaired_slots=self._active_slots(),
                touched=frozenset(self._touched_keys()),
                is_tolerable=self._is_tolerable,
                max_slots=self._max_slots,
            )

    def release(
        self,
        lease_id: str | None,
        *,
        reason: str = "released",
        target: ChaosTarget | None = None,
        nemesis_type: str | None = None,
        source: str | None = None,
    ) -> bool:
        if not lease_id:
            return False
        metrics = self._metrics
        removed: _Impairment | None = None
        with self._lock:
            kept: list[_Impairment] = []
            for imp in self._impairments:
                if imp.execution_id == lease_id and removed is None:
                    removed = imp
                    continue
                kept.append(imp)
            self._impairments = kept
            snap = self._snapshot_unlocked() if (metrics is not None and removed is not None) else None
        if removed is None:
            return False
        if metrics is not None:
            metrics.budget_released(
                lease_id=lease_id,
                footprint=Footprint(racks=frozenset(removed.racks), slots=removed.slots),
                identity_key=removed.identity_key,
                budget_after=snap,
                reason=reason,
                target=target,
                nemesis_type=nemesis_type,
                source=source,
            )
        return True

    def record_inject(
        self,
        execution_id: str,
        target: ChaosTarget | str,
        scope: ImpactScope,
        *,
        nemesis_type: str | None = None,
        source: str | None = None,
    ) -> None:
        """Account for an already-dispatched fault; held until extract / probe release."""
        chaos_target = (
            target if isinstance(target, ChaosTarget) else ChaosTarget.for_host(str(target))
        )
        footprint = self.footprint_for(chaos_target, scope)
        if not footprint:
            return
        metrics = self._metrics
        identity = chaos_target.identity_key()
        with self._lock:
            self._impairments = [
                imp for imp in self._impairments if imp.execution_id != execution_id
            ]
            self._impairments.append(
                _Impairment(
                    execution_id=execution_id,
                    racks=set(footprint.racks),
                    identity_key=identity,
                    slots=footprint.slots,
                )
            )
            snap = self._snapshot_unlocked() if metrics is not None else None
        if metrics is not None:
            metrics.budget_acquired(
                lease_id=execution_id,
                footprint=footprint,
                identity_key=identity,
                budget_after=snap,
                target=chaos_target,
                nemesis_type=nemesis_type,
                source=source,
            )

    def record_extract(
        self,
        execution_id: str,
        target: ChaosTarget | str,
        scope: ImpactScope,
        *,
        reason: str = "extract",
        nemesis_type: str | None = None,
        source: str | None = None,
    ) -> None:
        """Drop impairment by ``execution_id``, else by identity."""
        chaos_target = (
            target if isinstance(target, ChaosTarget) else ChaosTarget.for_host(str(target))
        )
        metrics = self._metrics
        removed: _Impairment | None = None
        with self._lock:
            before = list(self._impairments)
            self._impairments = [
                imp for imp in self._impairments if imp.execution_id != execution_id
            ]
            if len(self._impairments) == len(before):
                # Identity fallback: one domain can hold unrelated impairments.
                key = chaos_target.identity_key()
                kept: list[_Impairment] = []
                for imp in self._impairments:
                    if removed is None and imp.identity_key == key:
                        removed = imp
                        continue
                    kept.append(imp)
                self._impairments = kept
            else:
                for imp in before:
                    if imp.execution_id == execution_id:
                        removed = imp
                        break
            snap = self._snapshot_unlocked() if (metrics is not None and removed is not None) else None
        if removed is not None and metrics is not None:
            metrics.budget_released(
                lease_id=removed.execution_id,
                footprint=Footprint(racks=frozenset(removed.racks), slots=removed.slots),
                identity_key=removed.identity_key,
                budget_after=snap,
                reason=reason,
                target=chaos_target,
                nemesis_type=nemesis_type,
                source=source,
            )

    def snapshot(self) -> dict:
        with self._lock:
            return self._snapshot_unlocked()


__all__ = [
    "ImpactScope",
    "GuardMode",
    "Footprint",
    "BudgetView",
    "FailureModelConfigError",
    "fail_domain_key",
    "FailureTolerance",
    "HostTopology",
    "ClusterTopologyModel",
    "FailureModelGuard",
    "DEFAULT_RECOVERY_SEC",
    "DEFAULT_SLOT_FRACTION",
]
