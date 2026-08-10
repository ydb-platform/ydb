"""Boundary-walking nemesis scheduler: fill the failure budget each tick, release via RecoveryProbe.

``max_per_tick`` is a burst fuse; BYPASS (tablet) chaos is capped by ``max_bypass_per_tick``.
Without a probe, FULL-guarded types are not offered.
"""

from __future__ import annotations

import logging
import random
import threading
from dataclasses import replace
from typing import Callable, Sequence

from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    NEMESIS_TYPES,
    confirm_timeout_for,
    guard_mode_for,
    impact_scope_for,
    recovery_mode_for as catalog_recovery_mode_for,
    recovery_sec_for,
    stuck_timeout_for,
    supports_boundary_scheduler,
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    DEFAULT_RECOVERY_SEC,
    FailureModelGuard,
    Footprint,
    GuardMode,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
    hc_predicate_for,
    needs_baseline,
)

logger = logging.getLogger(__name__)

# Stability profile (DC type needs multi-DC; KickTabletsFromNode is FULL like a node kill).
_STABILITY_PROFILE: tuple[str, ...] = (
    "DataCenterStopNodesNemesis",
    "KillNodeNemesis",
    "KillSlotDaemonNemesis",
    "StopStartNodeNemesis",
    "SafelyBreakDiskNemesis",
    "SafelyCleanupDisksNemesis",
    "NetworkNemesis",
    "DnsNemesis",
    "TimeSkewNemesis",
    "KillHiveNemesis",
    "KillCoordinatorNemesis",
    "KillSchemeShardNemesis",
    "KillDataShardNemesis",
    "KillPersQueueNemesis",
    "ReBalanceTabletsNemesis",
    "KickTabletsFromNodeNemesis",
)

DEFAULT_STOP_JOIN_SEC: float = 10.0  # a tick is many HTTP POSTs
DEFAULT_RESTART_JOIN_SEC: float = 30.0  # wait out a stop() that gave up mid-tick


def default_enabled_types() -> list[str]:
    """The stability chaos profile: registered for this cluster and usable by this scheduler."""
    return [
        t for t in _STABILITY_PROFILE if t in NEMESIS_TYPES and supports_boundary_scheduler(t)
    ]


class BoundaryNemesisScheduler:
    def __init__(
        self,
        *,
        guard: FailureModelGuard,
        inventory,
        plan_inject: Callable[[str, ChaosTarget], list[DispatchCommand]],
        dispatch: Callable[[DispatchCommand], None],
        recovery_probe=None,
        plan_extract: Callable[[str, ChaosTarget], list[DispatchCommand]] | None = None,
        enabled_types: Sequence[str] | None = None,
        scope_for: Callable[[str], ImpactScope] = impact_scope_for,
        kind_for: Callable[[str], TargetKind] = target_kind_for,
        mode_for: Callable[[str], GuardMode] = guard_mode_for,
        recovery_sec_for: Callable[[str], float | None] = recovery_sec_for,
        recovery_mode_for: Callable[[str], str] = catalog_recovery_mode_for,
        stuck_timeout_for: Callable[[str], float] = stuck_timeout_for,
        confirm_timeout_for: Callable[[str], float] = confirm_timeout_for,
        predicate_for: Callable = hc_predicate_for,
        base_interval: float = 15.0,
        jitter: float = 0.5,
        max_per_tick: int = 16,
        max_bypass_per_tick: int = 1,
        rng: random.Random | None = None,
        stop_join_sec: float = DEFAULT_STOP_JOIN_SEC,
        restart_join_sec: float = DEFAULT_RESTART_JOIN_SEC,
    ) -> None:
        self._guard = guard
        self._inventory = inventory
        self._plan_inject = plan_inject
        self._plan_extract = plan_extract
        self._dispatch = dispatch
        self._recovery_probe = recovery_probe
        self._scope_for = scope_for
        self._kind_for = kind_for
        self._mode_for = mode_for
        self._recovery_sec_for = recovery_sec_for
        self._recovery_mode_for = recovery_mode_for
        self._stuck_timeout_for = stuck_timeout_for
        self._confirm_timeout_for = confirm_timeout_for
        self._predicate_for = predicate_for
        self._no_probe_warned = False
        self._rng = rng or random.Random()
        self._cfg_lock = threading.Lock()
        self._enabled = (
            list(enabled_types) if enabled_types is not None else default_enabled_types()
        )
        self._base_interval = float(base_interval)
        self._jitter = float(jitter)
        self._max_per_tick = max(1, int(max_per_tick))
        self._max_bypass_per_tick = max(1, int(max_bypass_per_tick))
        self._stop_join_sec = float(stop_join_sec)
        self._restart_join_sec = float(restart_join_sec)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def set_profile(
        self,
        *,
        enabled: Sequence[str] | None = None,
        base_interval: float | None = None,
        jitter: float | None = None,
        max_per_tick: int | None = None,
        max_bypass_per_tick: int | None = None,
    ) -> None:
        with self._cfg_lock:
            if enabled is not None:
                self._enabled = list(enabled)
            if base_interval is not None:
                self._base_interval = float(base_interval)
            if jitter is not None:
                self._jitter = float(jitter)
            if max_per_tick is not None:
                self._max_per_tick = max(1, int(max_per_tick))
            if max_bypass_per_tick is not None:
                self._max_bypass_per_tick = max(1, int(max_bypass_per_tick))

    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict:
        with self._cfg_lock:
            enabled = list(self._enabled)
            base, jitter = self._base_interval, self._jitter
            cap, bypass_cap = self._max_per_tick, self._max_bypass_per_tick
        out: dict = {
            "running": self.running(),
            "enabled_types": enabled,
            "base_interval": base,
            "jitter": jitter,
            "max_per_tick": cap,
            "max_bypass_per_tick": bypass_cap,
        }
        if self._recovery_probe is not None:
            out["recovery_probe"] = self._recovery_probe.snapshot()
        return out

    # -- tick ---------------------------------------------------------------

    def _can_extract(self) -> bool:
        return self._plan_extract is not None and self._recovery_probe is not None

    def _menu(
        self,
        bypass_used: frozenset[tuple[str, str]] = frozenset(),
        skip_types: frozenset[str] = frozenset(),
        include_bypass: bool = True,
    ) -> list[tuple[str, ChaosTarget, Footprint]]:
        with self._cfg_lock:
            enabled = list(self._enabled)
        view = self._guard.budget_view()  # reserve() re-checks atomically
        impaired = view.touched
        can_extract = self._can_extract()
        menu: list[tuple[str, ChaosTarget, Footprint]] = []
        for nemesis_type in enabled:
            if nemesis_type in skip_types:
                continue
            # Toggle without extract path would stay broken forever.
            if self._recovery_mode_for(nemesis_type) == "extract" and not can_extract:
                continue
            bypass = self._mode_for(nemesis_type) is GuardMode.BYPASS
            if bypass and not include_bypass:
                continue
            if not bypass and self._recovery_probe is None:
                # No probe → lease never released → mute FULL types.
                if not self._no_probe_warned:
                    self._no_probe_warned = True
                    logger.warning("no recovery probe wired: FULL-guarded types are muted")
                continue
            scope = self._scope_for(nemesis_type)
            kind = self._kind_for(nemesis_type)
            seen: set[str] = set()  # e.g. one DC target per host
            for target in self._inventory.entities(kind):
                key = target.identity_key()
                if key in seen:
                    continue
                seen.add(key)
                if bypass:
                    if (nemesis_type, key) not in bypass_used:
                        menu.append((nemesis_type, target, Footprint()))
                    continue
                if key in impaired:
                    continue
                footprint = self._guard.footprint_for(target, scope)
                if view.fits(footprint):
                    menu.append((nemesis_type, target, footprint))
        return menu

    def tick(self) -> int:
        """Fill the budget up to the boundary; return how many faults were injected."""
        with self._cfg_lock:
            fuse = self._max_per_tick
            bypass_cap = self._rng.randint(1, self._max_bypass_per_tick)
        budget_added = 0
        bypass_added = 0
        bypass_used: set[tuple[str, str]] = set()
        paused: set[str] = set()  # e.g. slots with no HC baseline
        while True:
            menu = self._menu(
                frozenset(bypass_used), frozenset(paused),
                include_bypass=bypass_added < bypass_cap,
            )
            if not menu:
                break
            nemesis_type, target, footprint = self._rng.choice(menu)
            if self._mode_for(nemesis_type) is GuardMode.BYPASS:
                for cmd in self._plan_inject(nemesis_type, target):
                    self._dispatch(cmd)
                bypass_used.add((nemesis_type, target.identity_key()))
                bypass_added += 1
                continue
            by_extract = (
                self._recovery_mode_for(nemesis_type) == "extract" and self._can_extract()
            )
            kind = self._kind_for(nemesis_type)
            scope = self._scope_for(nemesis_type)
            baseline = None
            if needs_baseline(kind, scope):
                baseline = self._recovery_probe.alive_compute_baseline()  # before inject
                if baseline is None:
                    if nemesis_type not in paused:
                        logger.warning(
                            "%s paused this tick: no fresh healthcheck data for a slot baseline",
                            nemesis_type,
                        )
                    paused.add(nemesis_type)
                    continue
            lease = self._guard.reserve(
                footprint,
                identity_key=target.identity_key(),
                target=target,
                nemesis_type=nemesis_type,
                source="boundary",
            )
            if lease is None:
                break
            # Release only if nothing landed; partial fanout must keep the budget + stay tracked.
            dispatched_any = False
            recovered = None
            try:
                recovered = self._predicate_for(
                    target,
                    kind=kind,
                    scope=scope,
                    inventory=self._inventory,
                    baseline=baseline,
                    nemesis_type=nemesis_type,
                )
                for cmd in self._plan_inject(nemesis_type, target):
                    self._dispatch(replace(cmd, lease_id=lease))
                    dispatched_any = True
                self._track_reserved(
                    lease, target, nemesis_type, recovered=recovered, by_extract=by_extract
                )
            except Exception:
                if not dispatched_any:
                    self._guard.release(
                        lease,
                        reason="abort",
                        target=target,
                        nemesis_type=nemesis_type,
                        source="boundary",
                    )
                    raise
                logger.exception(
                    "dispatch/track failed after applying %s on %s; holding budget",
                    nemesis_type, target.identity_key(),
                )
                try:
                    self._track_reserved(
                        lease, target, nemesis_type, recovered=recovered, by_extract=by_extract
                    )
                except Exception:
                    logger.exception(
                        "failed to track after partial dispatch of %s on %s; "
                        "budget held without probe",
                        nemesis_type, target.identity_key(),
                    )
                raise
            budget_added += 1
            if budget_added >= fuse:
                break
        return budget_added + bypass_added

    def _track_reserved(
        self,
        lease: str,
        target: ChaosTarget,
        nemesis_type: str,
        *,
        recovered: Callable,
        by_extract: bool,
    ) -> None:
        """Register a reserved lease with the recovery probe."""
        if by_extract:
            self._recovery_probe.track(
                lease, target, nemesis_type,
                recovered=recovered,
                stuck_timeout_sec=self._stuck_timeout_for(nemesis_type),
                recover_action=self._extract_action(nemesis_type, target, lease),
                extract_after_sec=self._extract_after_sec(nemesis_type),
                confirm_timeout_sec=self._confirm_timeout_for(nemesis_type),
            )
        else:
            self._recovery_probe.track(
                lease, target, nemesis_type,
                recovered=recovered,
                stuck_timeout_sec=self._stuck_timeout_for(nemesis_type),
            )

    def _extract_after_sec(self, nemesis_type: str) -> float:
        recovery = self._recovery_sec_for(nemesis_type)
        return float(recovery) if recovery is not None else DEFAULT_RECOVERY_SEC

    def _extract_action(self, nemesis_type: str, target: ChaosTarget, lease: str) -> Callable[[], None]:
        def _recover() -> None:
            for cmd in self._plan_extract(nemesis_type, target):
                ok = self._dispatch(replace(cmd, lease_id=lease))
                if ok is False:
                    raise RuntimeError(
                        f"extract dispatch failed for {nemesis_type} on {target.identity_key()}"
                    )
        return _recover

    # -- loop ---------------------------------------------------------------

    def _sleep_seconds(self) -> float:
        with self._cfg_lock:
            base, jitter = self._base_interval, self._jitter
        return max(0.5, base * (1.0 + self._rng.uniform(-jitter, jitter)))

    def _run(self) -> None:
        logger.info(
            "BoundaryNemesisScheduler started: %d type(s), base=%.1fs jitter=%.2f max_per_tick=%d",
            len(self._enabled), self._base_interval, self._jitter, self._max_per_tick,
        )
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                logger.exception("boundary scheduler tick raised")
            self._stop.wait(self._sleep_seconds())

    def start(self) -> None:
        t = self._thread
        if t is not None and t.is_alive():
            if not self._stop.is_set():
                return  # already running
            # Previous stop() gave up; wait so we do not start a second thread.
            logger.warning(
                "start(): previous scheduler thread is still stopping; waiting up to %.0fs",
                self._restart_join_sec,
            )
            t.join(timeout=self._restart_join_sec)
            if t.is_alive():
                raise RuntimeError(
                    f"previous scheduler thread is still shutting down after "
                    f"{self._restart_join_sec:.0f}s; not starting a second one"
                )
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if t and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=self._stop_join_sec)
            if t.is_alive():
                logger.warning(
                    "scheduler thread still inside a tick %.0fs after stop; draining extracts "
                    "anyway — start() will wait for it",
                    self._stop_join_sec,
                )
        if self._recovery_probe is not None:
            # Drain toggles after join so nothing is extracted twice; probe stays app-owned.
            try:
                drained = self._recovery_probe.drain_extracts()
            except Exception:
                logger.exception("failed to drain pending extracts on stop")
            else:
                if drained:
                    logger.info("scheduler stop: extracted %d in-flight toggle fault(s)", drained)


__all__ = ["BoundaryNemesisScheduler", "default_enabled_types"]
