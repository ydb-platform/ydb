"""Single-threaded weighted nemesis scheduler.

Walks the failure-model boundary: each tick breaks a random number of things (``cap``), picking
each fault by weight from whatever currently fits the budget, reserving it atomically, then sleeps
a randomized interval. Replaces the per-type schedule threads in ``schedule_loop.py``.

Recovery is timer-based here (the reserved budget auto-expires); Phase 2 swaps in real recovery
probes that release leases by fact.
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
    recovery_sec_for,
    target_kind_for,
    weight_for as catalog_weight_for,
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


def default_enabled_types() -> list[str]:
    """FULL-mode types (stateless, one-target injection) — the natural fit for weighted picking."""
    return [t for t in NEMESIS_TYPES if guard_mode_for(t) is GuardMode.FULL]


class WeightedNemesisScheduler:
    def __init__(
        self,
        *,
        guard: FailureModelGuard,
        inventory,
        plan_inject: Callable[[str, ChaosTarget], list[DispatchCommand]],
        dispatch: Callable[[DispatchCommand], None],
        recovery_probe=None,
        enabled_types: Sequence[str] | None = None,
        weight_for: Callable[[str], float] | None = None,
        scope_for: Callable[[str], ImpactScope] = impact_scope_for,
        kind_for: Callable[[str], TargetKind] = target_kind_for,
        recovery_sec_for: Callable[[str], float | None] = recovery_sec_for,
        base_interval: float = 60.0,
        jitter: float = 0.5,
        max_per_tick: int = 3,
        default_recovery_sec: float = DEFAULT_RECOVERY_SEC,
        rng: random.Random | None = None,
    ) -> None:
        self._guard = guard
        self._inventory = inventory
        self._plan_inject = plan_inject
        self._dispatch = dispatch
        self._recovery_probe = recovery_probe
        self._weight_for = weight_for or catalog_weight_for
        self._scope_for = scope_for
        self._kind_for = kind_for
        self._recovery_sec_for = recovery_sec_for
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
        weights: dict[str, float] | None = None,
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
        if weights is not None:
            wmap = {k: float(v) for k, v in weights.items()}
            self._weight_for = lambda t: wmap.get(t, 1.0)

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

    def _menu(self) -> list[tuple[str, ChaosTarget, frozenset[str], float]]:
        with self._cfg_lock:
            enabled = list(self._enabled)
        impaired = self._guard.active_identities()
        menu: list[tuple[str, ChaosTarget, frozenset[str], float]] = []
        for nemesis_type in enabled:
            weight = self._weight_for(nemesis_type)
            if weight <= 0:
                continue
            scope = self._scope_for(nemesis_type)
            kind = self._kind_for(nemesis_type)
            seen: set[str] = set()  # collapse duplicate targets (e.g. one DC exposed per host)
            for target in self._inventory.entities(kind):
                key = target.identity_key()
                if key in impaired or key in seen:
                    continue
                seen.add(key)
                racks = self._guard.footprint_for(target, scope)
                if self._guard.fits(racks):
                    menu.append((nemesis_type, target, racks, weight))
        return menu

    def _weighted_choice(
        self, menu: list[tuple[str, ChaosTarget, frozenset[str], float]]
    ) -> tuple[str, ChaosTarget, frozenset[str], float]:
        total = sum(item[3] for item in menu)
        if total <= 0:
            return self._rng.choice(menu)
        r = self._rng.random() * total
        acc = 0.0
        for item in menu:
            acc += item[3]
            if r <= acc:
                return item
        return menu[-1]

    def tick(self) -> int:
        """One scheduling tick: break up to a random ``cap`` faults. Returns how many were injected."""
        with self._cfg_lock:
            cap = self._rng.randint(1, self._max_per_tick)
        added = 0
        while added < cap:
            menu = self._menu()
            if not menu:
                break
            nemesis_type, target, racks, _weight = self._weighted_choice(menu)
            recovery = self._recovery_sec_for(nemesis_type)
            if recovery is None:
                recovery = self._default_recovery_sec
            # With a recovery probe, hold the budget (recovery_sec=None) until the fault is
            # observed recovered; the probe releases by fact. Tablets (empty footprint), a disabled
            # (fail-open) guard, and the probe-less path fall back to the timer so budget never sticks.
            use_probe = self._recovery_probe is not None and bool(racks) and self._guard.enabled
            lease = self._guard.reserve(
                racks,
                recovery_sec=None if use_probe else recovery,
                identity_key=target.identity_key(),
            )
            if lease is None:
                break
            for cmd in self._plan_inject(nemesis_type, target):
                self._dispatch(cmd)
            if use_probe:
                self._recovery_probe.track(lease, target, nemesis_type, timeout_sec=recovery)
            added += 1
        return added

    # -- loop ---------------------------------------------------------------

    def _sleep_seconds(self) -> float:
        with self._cfg_lock:
            base, jitter = self._base_interval, self._jitter
        return max(0.5, base * (1.0 + self._rng.uniform(-jitter, jitter)))

    def _run(self) -> None:
        logger.info(
            "WeightedNemesisScheduler started: %d type(s), base=%.1fs jitter=%.2f max_per_tick=%d",
            len(self._enabled), self._base_interval, self._jitter, self._max_per_tick,
        )
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                logger.exception("weighted scheduler tick raised")
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


__all__ = ["WeightedNemesisScheduler", "default_enabled_types"]
