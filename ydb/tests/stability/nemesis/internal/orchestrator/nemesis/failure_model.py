"""Failure-model guard: constrain nemesis chaos to the cluster's declared fault tolerance.

Parses ``cluster.yaml`` into a :class:`ClusterTopologyModel` and exposes a
:class:`FailureModelGuard` that vetoes/filters chaos exceeding the tolerated
simultaneous-failure budget (fail domain = rack, fail realm = datacenter):

    mirror-3-dc : 1 realm fully + 1 domain in another realm
    block-4-2   : any 2 domains
    none        : 0

A fail domain is identified by :func:`fail_domain_key` — ``"<data_center>/<rack>"`` — because
rack labels are only unique *within* a datacenter (``rack: '1'`` in every DC is a normal YDB
config). Keying by the bare rack label would collapse one rack per DC into a single domain and
let the guard admit chaos in every realm at once.

Only static (storage) node loss touches this erasure budget. Dynamic-node (slot) kills draw
from a separate, independent budget: up to a fraction (default 30%) of the cluster's slots may
be down at once. A :class:`Footprint` carries both dimensions so one lease covers either.

**Where the slot budget applies.** It is enforced by the lease API only — :meth:`fits` and
:meth:`reserve`, i.e. the boundary scheduler's path, which is what runs stability chaos. The
plan-time helpers do *not* enforce it: :meth:`filter_safe` (legacy per-type schedule loop in
``schedule_loop.py`` and the manual ``POST /api/hosts/process`` pre-check) only reasons about
fail domains, and :meth:`record_inject` records a slot fault's identity without charging a slot.
So slot chaos driven through the legacy schedule or manual injects is bounded by nothing but its
own dedup, and mixing both paths under-counts ``impaired_slots``. Fine while the boundary
scheduler owns scheduled chaos; revisit if the legacy loop is used for slot types again.

**The failure model is a hard requirement, not best effort.** An unusable ``cluster.yaml``
(missing, unparsable, unknown ``static_erasure``, hosts without ``location.rack``) raises
:class:`FailureModelConfigError` from :class:`ClusterTopologyModel`, and the orchestrator refuses
to start — see ``app.create_app``. Running chaos without a fault-tolerance ceiling is worse than
not running it at all, so there is no "guard disabled" mode: once the app is up, every guard API
enforces the budget.

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
    """``cluster.yaml`` cannot back a failure model — the orchestrator must not start.

    Raised for a missing/unparsable file, an unsupported ``static_erasure``, or hosts without the
    ``location`` data the guard needs. Never caught to keep chaos running: unbounded chaos would
    silently exceed the cluster's fault tolerance and turn every stability failure into noise.
    """


class ImpactScope(enum.Enum):
    """Topology level a nemesis affects (orchestrator-side annotation)."""

    NODE = "node"
    SLOT = "slot"
    DISK = "disk"
    RACK = "rack"
    DATACENTER = "datacenter"
    PILE = "pile"
    UNKNOWN = "unknown"


class GuardMode(enum.Enum):
    """How the failure-model guard treats a nemesis type.

    FULL   : pre-filter candidates and account for the impact (``reserve`` / ``record_inject``).
    BYPASS : the type costs no budget (tablet chaos) — offered every tick, never reserved.
    """

    FULL = "full"
    BYPASS = "bypass"


# Fallback recovery window (seconds) when a nemesis type has no ``auto_recovery_sec`` annotation.
# Conservatively longer than a systemd restart so a node stays "impaired" until it likely rejoined.
DEFAULT_RECOVERY_SEC: float = 120.0

# Fraction of the cluster's dynamic-node slots that may be down simultaneously (separate from the
# erasure/rack budget). Killing a slot doesn't reduce storage redundancy, so it's cheap chaos.
DEFAULT_SLOT_FRACTION: float = 0.3

# Datacenter placeholder in a fail-domain key for hosts whose ``location`` has no ``data_center``.
_UNKNOWN_DC = "?"

# Prefix of a fail-domain key synthesized for a host that is not in ``cluster.yaml``.
_SYNTHETIC_DOMAIN_PREFIX = "__host__:"


def fail_domain_key(datacenter: str | None, rack: str) -> str:
    """Fail-domain identity: ``"<data_center>/<rack>"``.

    Rack labels in ``cluster.yaml`` are only unique inside a datacenter, so the realm has to be
    part of the key — otherwise rack ``1`` of every DC would be one domain and ``mirror-3-dc``
    would tolerate losing all three realms at once.
    """
    return f"{datacenter or _UNKNOWN_DC}/{rack}"


@dataclass(frozen=True)
class Footprint:
    """What one fault consumes from the budget: fail domains and/or dynamic-node slots.

    Rack faults (static nodes, disks, datacenters) fill ``racks`` with :func:`fail_domain_key`
    keys (``"<dc>/<rack>"``); a slot (dynamic node) kill fills ``slots`` and leaves ``racks``
    empty; a tablet touches neither. The two dimensions are checked independently by the guard.
    """

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
    """Parses ``cluster.yaml``: erasure mode and host -> rack -> datacenter mapping.

    Raises :class:`FailureModelConfigError` for anything that would leave the guard unable to
    decide — missing/unparsable file, unsupported ``static_erasure``, no hosts, a host without
    ``location.rack``, or (for ``mirror-3-dc``) without ``location.data_center``. Construction
    therefore either yields a usable model or takes the orchestrator down with it.

    ``rack_of`` / ``dc_of`` return the raw ``location`` labels (inventory, UI); the guard reasons
    in :func:`fail_domain_key` keys via ``domain_of`` / ``domains_in_dc`` / ``dc_of_domain``.
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
        self.tolerance = FailureTolerance.from_erasure(doc.get("static_erasure", ""))
        if not self.tolerance.guards:
            raise FailureModelConfigError(
                f"{self.yaml_path}: static_erasure={doc.get('static_erasure')!r} is not a mode the "
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
        """Always True: an unusable config raises instead of degrading into a no-op guard.

        Kept because the guard state is reported through ``snapshot()`` / the API, where "the guard
        is live" is worth stating explicitly.
        """
        return self.tolerance.guards and bool(self.hosts)

    def rack_of(self, host: str) -> str | None:
        """Raw ``location.rack`` label (not a fail-domain key) — inventory / UI."""
        t = self.hosts.get(host)
        return t.rack if t else None

    def dc_of(self, host: str) -> str | None:
        t = self.hosts.get(host)
        return t.datacenter if t else None

    def domain_of(self, host: str) -> str | None:
        """Fail-domain key of ``host``, or None when the host has no rack in ``cluster.yaml``."""
        t = self.hosts.get(host)
        if t is None or t.rack is None:
            return None
        return fail_domain_key(t.datacenter, t.rack)

    def domains_in_dc(self, dc: str | None) -> set[str]:
        """Every fail-domain key of realm ``dc``."""
        return {d for d, ddc in self._domain_to_dc.items() if ddc == dc}

    def dc_of_domain(self, domain: str) -> str | None:
        return self._domain_to_dc.get(domain)


class BudgetView:
    """Read-only snapshot of both budgets, for filtering many candidates at once.

    :meth:`FailureModelGuard.budget_view` builds one under a single lock; the scheduler then tests
    every (type, target) pair against it instead of locking per candidate. Each candidate is
    checked against the same captured state — exactly what per-candidate
    :meth:`FailureModelGuard.fits` calls did, minus the lock traffic.

    A view is advisory and may be stale: :meth:`FailureModelGuard.reserve` re-checks atomically and
    refuses the fault if the budget moved meanwhile.
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
        if not self._is_tolerable(set(self.impaired_racks) | set(footprint.racks)):
            return False
        if footprint.slots <= 0 or self._max_slots <= 0:
            return True
        return self.impaired_slots + footprint.slots <= self._max_slots


@dataclass
class _Impairment:
    """One recorded fault footprint. ``deadline`` is a ``time.monotonic()`` value; ``None`` = held
    until an explicit extract."""

    execution_id: str
    racks: set[str]
    identity_key: str
    deadline: float | None
    slots: int = 0


class FailureModelGuard:
    """Tracks impaired fail domains (racks) + dynamic-node slots against the failure model.

    Safety is applied at plan time via :meth:`filter_safe` (no dispatch-time veto).
    ``record_inject`` / ``record_extract`` update the touched set after dispatch.
    Tablet targets contribute nothing; slot (dynamic node) targets contribute no rack but
    draw from a separate slot budget (``total_slots`` × ``slot_fraction``).

    Assumes a single active nemesis in MVP (no allocate/reserve locks across types).
    """

    def __init__(
        self,
        topology: ClusterTopologyModel,
        *,
        total_slots: int = 0,
        slot_fraction: float = DEFAULT_SLOT_FRACTION,
    ) -> None:
        self._topology = topology
        self._lock = threading.Lock()
        self._impairments: list[_Impairment] = []
        self._total_slots = max(0, int(total_slots))
        self._slot_fraction = float(slot_fraction)
        # ≥1 when the cluster has any slots so slot chaos still runs on small test clusters;
        # 0 means no slots known -> the slot budget fails open (never blocks).
        self._max_slots = (
            max(1, int(self._total_slots * self._slot_fraction)) if self._total_slots > 0 else 0
        )

    @property
    def enabled(self) -> bool:
        """Always True — an unusable topology raises at construction (see the module docstring).

        Reported through :meth:`snapshot` and ``/api/scheduler`` / ``/api/problems`` so a report
        can state that chaos ran under a live guard.
        """
        return self._topology.guards

    # -- rack resolution ----------------------------------------------------

    def _racks_for_host(self, host: str, scope: ImpactScope) -> set[str]:
        """Fail domains (``<dc>/<rack>`` keys) touched by injecting ``scope`` on ``host``."""
        if scope == ImpactScope.DATACENTER:
            dc = self._topology.dc_of(host)
            domains = self._topology.domains_in_dc(dc)
            return set(domains) if domains else {self._synthetic_key(host)}
        # NODE / DISK / RACK — and PILE / UNKNOWN as best effort — collapse to the host's own fail
        # domain. SLOT never reaches here: ``_racks_for_target`` routes it to the slot budget.
        domain = self._topology.domain_of(host)
        return {domain if domain is not None else self._synthetic_key(host)}

    @staticmethod
    def _is_slot(target: ChaosTarget, scope: ImpactScope) -> bool:
        """A dynamic-node (slot) fault — draws from the slot budget, not a rack fail-domain."""
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
        """Fail-domain key for a host ``cluster.yaml`` has no rack for (e.g. an agent host that is
        not in the config at all).

        Namespaced by realm like a real domain key, so unknown-rack hosts in different DCs are not
        lumped into one sacrificial realm under ``mirror-3-dc``.
        """
        return f"{_SYNTHETIC_DOMAIN_PREFIX}{fail_domain_key(self._topology.dc_of(host), host)}"

    def _realm_of_domain(self, domain: str) -> str | None:
        """Realm (datacenter) of a fail-domain key, synthetic ones included."""
        if domain.startswith(_SYNTHETIC_DOMAIN_PREFIX):
            dc = domain[len(_SYNTHETIC_DOMAIN_PREFIX):].split("/", 1)[0]
            return None if dc == _UNKNOWN_DC else dc
        return self._topology.dc_of_domain(domain)

    def footprint_for(self, target: ChaosTarget, scope: ImpactScope) -> Footprint:
        """What ``target`` consumes at ``scope``: fail-domain racks and/or one slot.

        Empty for tablets; one slot (no rack) for dynamic-node kills; the host's rack(s)
        otherwise (static node / disk / datacenter)."""
        return Footprint(
            racks=frozenset(self._racks_for_target(target, scope)),
            slots=1 if self._is_slot(target, scope) else 0,
        )

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

    def _active_slots(self, now: float) -> int:
        self._purge_expired(now)
        return sum(imp.slots for imp in self._impairments)

    def _slots_ok(self, add_slots: int, now: float) -> bool:
        """Whether ``add_slots`` more slots stay within the 30% cluster budget (fail-open at 0)."""
        if add_slots <= 0 or self._max_slots <= 0:
            return True
        return self._active_slots(now) + add_slots <= self._max_slots

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
            for domain in impaired_racks:
                realm = self._realm_of_domain(domain)
                by_dc[realm] = by_dc.get(realm, 0) + 1
            if not by_dc:
                return True
            sacrificial = max(by_dc.values())  # the realm we allow to fail fully
            remaining = n - sacrificial
            return remaining <= tol.max_extra_domains
        # Unreachable: an erasure mode the model doesn't understand never gets past parsing.
        return True

    # -- public API ---------------------------------------------------------

    def filter_safe(
        self,
        candidates: Iterable[ChaosTarget],
        scope: ImpactScope,
    ) -> list[ChaosTarget]:
        """Return a jointly safe subset of ``candidates`` under the fail-domain budget.

        Candidates are checked in order. Each admitted target's fail domains are added to
        the running ``active`` set, so later candidates see earlier admissions (order-dependent).
        Already-touched identities (``ChaosTarget.identity_key``) are skipped.

        Only the erasure/fail-domain dimension is checked here: slot candidates carry no fail
        domain, so they are always admitted regardless of the slot budget. Use :meth:`fits` /
        :meth:`reserve` (boundary scheduler) when the slot budget must hold — see the module
        docstring.
        """
        candidates = list(candidates)
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

    # -- lease-based budget API ---------------------------------------------

    def fits(self, footprint: Footprint) -> bool:
        """True if adding ``footprint`` keeps both budgets within tolerance (read-only)."""
        now = time.monotonic()
        with self._lock:
            return self._is_tolerable(
                self._active_racks(now) | set(footprint.racks)
            ) and self._slots_ok(footprint.slots, now)

    def reserve(
        self,
        footprint: Footprint,
        recovery_sec: float | None = None,
        identity_key: str | None = None,
    ) -> str | None:
        """Atomically claim ``footprint`` (racks and/or slots) under one lock; return a lease id,
        or None if it exceeds either budget. ``recovery_sec`` auto-expires the lease after that
        many seconds; ``None`` holds it until :meth:`release`. ``identity_key`` records which target
        the lease covers so it shows up in :meth:`active_identities` (schedulers skip already-impaired
        targets).

        Every granted lease id is unique, so callers may key their own bookkeeping by it (the
        recovery probe tracks pending extracts that way)."""
        now = time.monotonic()
        deadline = None if recovery_sec is None else now + float(recovery_sec)
        with self._lock:
            if not self._is_tolerable(self._active_racks(now) | set(footprint.racks)):
                return None
            if not self._slots_ok(footprint.slots, now):
                return None
            lease_id = uuid.uuid4().hex
            self._impairments.append(
                _Impairment(
                    execution_id=lease_id,
                    racks=set(footprint.racks),
                    identity_key=identity_key or f"lease:{lease_id}",
                    deadline=deadline,
                    slots=footprint.slots,
                )
            )
            return lease_id

    def active_identities(self) -> set[str]:
        """Identity keys of every non-expired impairment."""
        now = time.monotonic()
        with self._lock:
            return self._touched_keys(now)

    def budget_view(self) -> BudgetView:
        """Snapshot both budgets and the touched identities under one lock (see :class:`BudgetView`)."""
        now = time.monotonic()
        with self._lock:
            return BudgetView(
                impaired_racks=frozenset(self._active_racks(now)),
                impaired_slots=self._active_slots(now),
                touched=frozenset(self._touched_keys(now)),
                is_tolerable=self._is_tolerable,
                max_slots=self._max_slots,
            )

    def release(self, lease_id: str | None) -> bool:
        if not lease_id:
            return False
        with self._lock:
            before = len(self._impairments)
            self._impairments = [
                imp for imp in self._impairments if imp.execution_id != lease_id
            ]
            return len(self._impairments) != before

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

        Charges both dimensions of the target's :meth:`footprint_for` — fail domains for static
        faults, one slot for a dynamic-node kill. A footprint that consumes nothing (tablets) is
        not recorded at all. Unlike :meth:`reserve` this never refuses: the fault was already
        dispatched, so the budget is told the truth even when that truth exceeds it.
        """
        chaos_target = (
            target if isinstance(target, ChaosTarget) else ChaosTarget.for_host(str(target))
        )
        footprint = self.footprint_for(chaos_target, scope)
        if not footprint:
            return
        deadline = None if recovery_sec is None else time.monotonic() + float(recovery_sec)
        with self._lock:
            self._impairments = [
                imp for imp in self._impairments if imp.execution_id != execution_id
            ]
            self._impairments.append(
                _Impairment(
                    execution_id=execution_id,
                    racks=set(footprint.racks),
                    identity_key=chaos_target.identity_key(),
                    deadline=deadline,
                    slots=footprint.slots,
                )
            )

    def record_extract(
        self,
        execution_id: str,
        target: ChaosTarget | str,
        scope: ImpactScope,
    ) -> None:
        """Release the impairment recorded for ``execution_id`` (early recovery)."""
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
                "impaired_slots": self._active_slots(now),
                "max_slots": self._max_slots,
                "touched_targets": touched,
                "tracked_executions": len(self._impairments),
            }


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
