"""Single-threaded nemesis scheduler.

Walks the failure-model boundary: each tick breaks a random number of things (``cap``), picking
each fault uniformly at random from whatever currently fits the budget, reserving it atomically,
then sleeps a randomized interval. Replaces the per-type schedule threads in ``schedule_loop.py``.

Budget is released by the :class:`RecoveryProbe`, per the type's :func:`recovery_mode_for`:
self-recovering faults are released once healthcheck sees the host answer again; toggle faults
are held for their ``auto_recovery_sec`` then actively extracted. Without a probe (or a disabled,
fail-open guard) self-recovering faults fall back to the reserve timer so budget never sticks.
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
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    DEFAULT_RECOVERY_SEC,
    FailureModelGuard,
    GuardMode,
    ImpactScope,
)

logger = logging.getLogger(__name__)

# Curated stability chaos profile: datacenter stop, node kill, slot kill, node stop/start, disk
# break/cleanup, clock skew, plus tablet chaos. Filtered to what the cluster actually registers
# (e.g. the DC type only exists on multi-DC clusters). Toggle members are recovered via timed
# extract. Tablet types are BYPASS (injected without spending the failure-model budget), except
# KickTabletsFromNodeNemesis, which kills the node and so is counted like any other node fault.
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


def default_enabled_types() -> list[str]:
    """The stability chaos profile, restricted to types registered for this cluster."""
    return [t for t in _STABILITY_PROFILE if t in NEMESIS_TYPES]


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
    ) -> list[tuple[str, ChaosTarget, frozenset[str]]]:
        with self._cfg_lock:
            enabled = list(self._enabled)
        impaired = self._guard.active_identities()
        can_extract = self._can_extract()
        menu: list[tuple[str, ChaosTarget, frozenset[str]]] = []
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
                    # BYPASS: not counted against the failure budget, so it's offered every tick
                    # regardless of what's impaired. Deduped per (type, target) — tablet types all
                    # share one control-host target, so this keeps each firing at most once a tick.
                    if (nemesis_type, key) not in bypass_used:
                        menu.append((nemesis_type, target, frozenset()))
                    continue
                if key in impaired:
                    continue
                racks = self._guard.footprint_for(target, scope)
                if self._guard.fits(racks):
                    menu.append((nemesis_type, target, racks))
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
            nemesis_type, target, racks = self._rng.choice(menu)
            if self._mode_for(nemesis_type) is GuardMode.BYPASS:
                # No budget to reserve and no probe to track — just fire and remember it fired.
                for cmd in self._plan_inject(nemesis_type, target):
                    self._dispatch(cmd)
                bypass_used.add((nemesis_type, target.identity_key()))
                added += 1
                continue
            recovery = self._recovery_sec_for(nemesis_type)
            if recovery is None:
                recovery = self._default_recovery_sec
            # The probe releases the budget by fact, so hold it (recovery_sec=None):
            #   extract  — toggle fault; probe holds `recovery`s then dispatches the extract.
            #   self     — probe releases once healthcheck sees the host answer again.
            # A disabled (fail-open) guard, empty footprint, or missing probe falls back to the
            # reserve timer so self-recovering budget never sticks.
            by_extract = (
                self._recovery_mode_for(nemesis_type) == "extract" and self._can_extract()
            )
            by_healthcheck = (
                not by_extract
                and self._recovery_probe is not None
                and bool(racks)
                and self._guard.enabled
            )
            lease = self._guard.reserve(
                racks,
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
        if self._thread and self._thread.is_alive():
            return
        if self._recovery_probe is not None:
            self._recovery_probe.start()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if t and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=2.0)
        if self._recovery_probe is not None:
            self._recovery_probe.stop()


__all__ = ["BoundaryNemesisScheduler", "default_enabled_types"]
