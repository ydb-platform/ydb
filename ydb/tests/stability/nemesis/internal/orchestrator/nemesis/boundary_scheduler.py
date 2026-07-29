"""Single-threaded nemesis scheduler that walks the failure-model boundary.

Each tick breaks a random number of things (``cap``), picking uniformly from whatever fits the
budget, reserving atomically, then sleeps a randomized interval. Replaces the per-type threads in
``schedule_loop.py``.

Budget is released by the :class:`RecoveryProbe`: self-recovering faults once healthcheck sees the
host answer again, toggle faults after ``auto_recovery_sec`` via an extract. :meth:`stop` extracts
whatever is still held, so switching chaos off never leaves the cluster broken.
"""

from __future__ import annotations

import logging
import random
import threading
from typing import Callable, Sequence

from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    NEMESIS_TYPES,
    guard_mode_for,
    impact_scope_for,
    recovery_mode_for as catalog_recovery_mode_for,
    recovery_sec_for,
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

logger = logging.getLogger(__name__)

# Curated stability profile, filtered to what this cluster registers (the DC type needs multi-DC).
# Tablet types are BYPASS; KickTabletsFromNode kills the node, so it is budgeted like a node fault.
_STABILITY_PROFILE: tuple[str, ...] = (
    "DataCenterStopNodesNemesis",
    "KillNodeNemesis",
    "KillSlotDaemonNemesis",
    "StopStartNodeNemesis",
    "SafelyBreakDiskNemesis",
    "SafelyCleanupDisksNemesis",
    "TimeSkewNemesis",
    "KillHiveNemesis",
    "KillCoordinatorNemesis",
    "KillSchemeShardNemesis",
    "KillDataShardNemesis",
    "KillPersQueueNemesis",
    "ReBalanceTabletsNemesis",
    "KickTabletsFromNodeNemesis",
)

# A tick can be slow: every dispatch is an HTTP POST with its own timeout.
DEFAULT_STOP_JOIN_SEC: float = 10.0
# ``start()`` must not run on top of a thread a previous ``stop()`` gave up on.
DEFAULT_RESTART_JOIN_SEC: float = 30.0


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
        base_interval: float = 60.0,
        jitter: float = 0.5,
        max_per_tick: int = 3,
        default_recovery_sec: float = DEFAULT_RECOVERY_SEC,
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
        self._default_recovery_sec = float(default_recovery_sec)
        self._rng = rng or random.Random()
        self._cfg_lock = threading.Lock()
        self._enabled = (
            list(enabled_types) if enabled_types is not None else default_enabled_types()
        )
        self._base_interval = float(base_interval)
        self._jitter = float(jitter)
        self._max_per_tick = max(1, int(max_per_tick))
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

    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict:
        with self._cfg_lock:
            enabled = list(self._enabled)
            base, jitter, cap = self._base_interval, self._jitter, self._max_per_tick
        out: dict = {
            "running": self.running(),
            "enabled_types": enabled,
            "base_interval": base,
            "jitter": jitter,
            "max_per_tick": cap,
        }
        if self._recovery_probe is not None:
            out["recovery_probe"] = self._recovery_probe.snapshot()
        return out

    # -- tick ---------------------------------------------------------------

    def _can_extract(self) -> bool:
        return self._plan_extract is not None and self._recovery_probe is not None

    def _menu(
        self, bypass_used: frozenset[tuple[str, str]] = frozenset()
    ) -> list[tuple[str, ChaosTarget, Footprint]]:
        with self._cfg_lock:
            enabled = list(self._enabled)
        # One snapshot for the whole menu; reserve() re-checks atomically before injecting.
        view = self._guard.budget_view()
        impaired = view.touched
        can_extract = self._can_extract()
        menu: list[tuple[str, ChaosTarget, Footprint]] = []
        for nemesis_type in enabled:
            # A toggle fault with no way to auto-extract would stay broken forever — don't offer it.
            if self._recovery_mode_for(nemesis_type) == "extract" and not can_extract:
                continue
            bypass = self._mode_for(nemesis_type) is GuardMode.BYPASS
            scope = self._scope_for(nemesis_type)
            kind = self._kind_for(nemesis_type)
            seen: set[str] = set()  # collapse duplicate targets (e.g. one DC exposed per host)
            for target in self._inventory.entities(kind):
                key = target.identity_key()
                if key in seen:
                    continue
                seen.add(key)
                if bypass:
                    # Costs no budget, so it is always offered. Deduped per (type, target) because
                    # tablet types share one control-host target.
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
        """One scheduling tick: break up to a random ``cap`` faults. Returns how many were injected."""
        with self._cfg_lock:
            cap = self._rng.randint(1, self._max_per_tick)
        added = 0
        bypass_used: set[tuple[str, str]] = set()
        while added < cap:
            menu = self._menu(frozenset(bypass_used))
            if not menu:
                break
            nemesis_type, target, footprint = self._rng.choice(menu)
            if self._mode_for(nemesis_type) is GuardMode.BYPASS:
                # Nothing to reserve or track — fire and remember it fired.
                for cmd in self._plan_inject(nemesis_type, target):
                    self._dispatch(cmd)
                bypass_used.add((nemesis_type, target.identity_key()))
                added += 1
                continue
            recovery = self._recovery_sec_for(nemesis_type)
            if recovery is None:
                recovery = self._default_recovery_sec
            # Hold the budget (recovery_sec=None) when the probe will release it by fact: after a
            # timed extract, or once healthcheck sees the host again. Slot kills have no fail domain
            # and healthcheck cannot see a single slot restart, so they fall back to the timer.
            by_extract = (
                self._recovery_mode_for(nemesis_type) == "extract" and self._can_extract()
            )
            by_healthcheck = (
                not by_extract
                and self._recovery_probe is not None
                and bool(footprint.racks)
            )
            lease = self._guard.reserve(
                footprint,
                recovery_sec=None if (by_extract or by_healthcheck) else recovery,
                identity_key=target.identity_key(),
            )
            if lease is None:
                break
            for cmd in self._plan_inject(nemesis_type, target):
                self._dispatch(cmd)
            if by_extract:
                self._recovery_probe.track(
                    lease, target, nemesis_type, timeout_sec=recovery,
                    recover_action=self._extract_action(nemesis_type, target),
                )
            elif by_healthcheck:
                self._recovery_probe.track(lease, target, nemesis_type, timeout_sec=recovery)
            added += 1
        return added

    def _extract_action(self, nemesis_type: str, target: ChaosTarget) -> Callable[[], None]:
        def _recover() -> None:
            for cmd in self._plan_extract(nemesis_type, target):
                self._dispatch(cmd)
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
            # A previous stop() gave up on a slow tick. Returning here would report "started" while
            # the stop flag kills the old thread and the probe stays down — no budget ever released.
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
        if self._recovery_probe is not None:
            self._recovery_probe.start()
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
                    "scheduler thread still inside a tick %.0fs after stop; stopping the probe "
                    "anyway — start() will wait for it",
                    self._stop_join_sec,
                )
        if self._recovery_probe is not None:
            self._recovery_probe.stop()
            # Nothing else extracts them, so stopping chaos would leave a node stopped / disk
            # broken / clock skewed. After the join, so no lease is extracted twice.
            try:
                drained = self._recovery_probe.drain_extracts()
            except Exception:
                logger.exception("failed to drain pending extracts on stop")
            else:
                if drained:
                    logger.info("scheduler stop: extracted %d in-flight toggle fault(s)", drained)


__all__ = ["BoundaryNemesisScheduler", "default_enabled_types"]
