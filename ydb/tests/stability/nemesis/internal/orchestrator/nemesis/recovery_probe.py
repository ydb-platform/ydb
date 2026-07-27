"""Fact-based recovery for reserved failure budget.

The boundary scheduler reserves budget per injected fault and holds it (``recovery_sec=None``)
until the fault is observed to have recovered. :class:`RecoveryProbe` polls a ``recovered(target)``
predicate and calls :meth:`FailureModelGuard.release` the moment a target is back — instead of
guessing a fixed timer. If a fault does NOT recover within its timeout the probe does not silently
release the budget (that would let the scheduler pile more chaos onto a cluster that is not
healing); it keeps holding and raises a stuck-fault problem for the warden to surface.

``recovered`` is injected so the criterion can be healthcheck-based (:func:`healthcheck_recovery`),
warden-based, or anything else without coupling this module to a data source.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import FailureModelGuard

logger = logging.getLogger(__name__)

# Healthcheck self_check_result values that mean "this host's endpoint did not answer".
_HC_ERROR_RESULTS = frozenset({"HC_REQUEST_ERROR", "HC_RESULT_ERROR"})

# Wait at least this long before trusting a recovery signal, so a stale pre-fault healthcheck
# result cannot be mistaken for "already recovered" right after injection.
DEFAULT_MIN_HOLD_SEC: float = 30.0
DEFAULT_RECOVERY_TIMEOUT_SEC: float = 300.0
DEFAULT_POLL_INTERVAL_SEC: float = 15.0


@dataclass(frozen=True)
class StuckFault:
    """A reserved fault that has not recovered within its timeout (budget still held)."""

    lease_id: str
    nemesis_type: str
    target: ChaosTarget
    held_sec: float
    timeout_sec: float


@dataclass
class _Pending:
    lease_id: str
    target: ChaosTarget
    nemesis_type: str
    reserved_at: float
    timeout_sec: float
    min_hold_sec: float
    stuck_reported: bool = False
    recover_action: Callable[[], None] | None = None


def healthcheck_recovery(
    reporter, error_results: frozenset[str] = _HC_ERROR_RESULTS
) -> Callable[[ChaosTarget], bool]:
    """``recovered`` predicate: a target is recovered once its host's healthcheck endpoint answers
    again (any non-error ``self_check_result``). ``reporter`` is anything exposing ``last_results``
    (e.g. ``HealthCheckReporter``)."""

    def recovered(target: ChaosTarget) -> bool:
        results = getattr(reporter, "last_results", None) or {}
        entry = results.get(target.host)
        if not isinstance(entry, dict):
            return False  # no data yet -> assume not recovered
        return entry.get("self_check_result") not in error_results

    return recovered


class RecoveryProbe:
    def __init__(
        self,
        *,
        guard: FailureModelGuard,
        recovered: Callable[[ChaosTarget], bool],
        on_stuck: Callable[[StuckFault], None] | None = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SEC,
        default_timeout_sec: float = DEFAULT_RECOVERY_TIMEOUT_SEC,
        min_hold_sec: float = DEFAULT_MIN_HOLD_SEC,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._guard = guard
        self._recovered = recovered
        self._on_stuck = on_stuck
        self._poll_interval = float(poll_interval)
        self._default_timeout_sec = float(default_timeout_sec)
        self._min_hold_sec = float(min_hold_sec)
        self._clock = clock
        self._lock = threading.Lock()
        self._pending: dict[str, _Pending] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def track(
        self,
        lease_id: str,
        target: ChaosTarget,
        nemesis_type: str,
        timeout_sec: float | None = None,
        recover_action: Callable[[], None] | None = None,
    ) -> None:
        """Track a reserved fault until its budget can be released.

        ``recover_action`` distinguishes the two recovery classes:
        - ``None`` (self-recovering): released once ``recovered(target)`` is true; a fault that
          overshoots ``timeout_sec`` is reported stuck and keeps holding budget.
        - callable (toggle): after holding for ``timeout_sec`` the probe runs the action
          (dispatch an extract) and releases the budget — no healthcheck, never stuck.
        """
        if not lease_id:
            return
        timeout = float(timeout_sec) if timeout_sec is not None else self._default_timeout_sec
        with self._lock:
            self._pending[lease_id] = _Pending(
                lease_id=lease_id,
                target=target,
                nemesis_type=nemesis_type,
                reserved_at=self._clock(),
                timeout_sec=timeout,
                min_hold_sec=min(self._min_hold_sec, timeout),
                recover_action=recover_action,
            )

    def forget(self, lease_id: str) -> None:
        with self._lock:
            self._pending.pop(lease_id, None)

    def tick(self) -> list[StuckFault]:
        """Poll every tracked fault once. Releases recovered ones; returns newly-stuck ones."""
        now = self._clock()
        with self._lock:
            items = list(self._pending.values())
        stuck: list[StuckFault] = []
        for p in items:
            held = now - p.reserved_at
            if held < p.min_hold_sec:
                continue
            if p.recover_action is not None:
                if held >= p.timeout_sec:
                    self._auto_extract(p, held)
                continue
            try:
                is_recovered = self._recovered(p.target)
            except Exception:
                logger.exception("recovery check raised for %s", p.target.identity_key())
                is_recovered = False
            if is_recovered:
                self._guard.release(p.lease_id)
                with self._lock:
                    self._pending.pop(p.lease_id, None)
                logger.info(
                    "recovered: %s (%s) after %.0fs; budget released",
                    p.target.host, p.nemesis_type, held,
                )
                continue
            if held > p.timeout_sec and not p.stuck_reported:
                p.stuck_reported = True
                logger.error(
                    "fault did not recover within %.0fs; holding budget: %s (%s)",
                    p.timeout_sec, p.target.host, p.nemesis_type,
                )
                stuck.append(
                    StuckFault(
                        lease_id=p.lease_id,
                        nemesis_type=p.nemesis_type,
                        target=p.target,
                        held_sec=held,
                        timeout_sec=p.timeout_sec,
                    )
                )
        for info in stuck:
            if self._on_stuck is not None:
                try:
                    self._on_stuck(info)
                except Exception:
                    logger.exception("on_stuck callback raised")
        return stuck

    def drain_extracts(self) -> int:
        """Extract every tracked toggle fault right now, regardless of its remaining hold window.

        Called when chaos is switched off (scheduler ``stop()`` / app teardown): a fault that is
        still waiting out its hold — stopped node, broken disk, skewed clock — would otherwise
        never be extracted, and the cluster would stay broken after nemesis was disabled. Nothing
        else extracts them: the boundary scheduler dispatches toggle injects directly, so the
        planners' ``extract_all_on_disable`` bookkeeping never saw them.

        Self-recovering faults are left tracked: they heal on their own, and a probe that is
        started again keeps polling them.

        Returns the number of extracts dispatched.
        """
        with self._lock:
            pending = [p for p in self._pending.values() if p.recover_action is not None]
        if not pending:
            return 0
        logger.info("draining %d pending extract(s) on shutdown", len(pending))
        now = self._clock()
        for p in pending:
            self._auto_extract(p, now - p.reserved_at, reason="drained on shutdown")
        return len(pending)

    def _auto_extract(self, p: _Pending, held: float, reason: str = "hold elapsed") -> None:
        """Toggle fault held long enough: dispatch its extract and release the budget."""
        try:
            p.recover_action()
        except Exception:
            logger.exception("auto-extract action raised for %s", p.target.identity_key())
        self._guard.release(p.lease_id)
        with self._lock:
            self._pending.pop(p.lease_id, None)
        logger.info(
            "auto-extracted: %s (%s) after %.0fs hold (%s); budget released",
            p.target.host, p.nemesis_type, held, reason,
        )

    def pending(self) -> list[_Pending]:
        with self._lock:
            return list(self._pending.values())

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "tracked": len(self._pending),
                "stuck": sum(1 for p in self._pending.values() if p.stuck_reported),
            }

    # -- loop ---------------------------------------------------------------

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                logger.exception("recovery probe tick raised")
            self._stop.wait(self._poll_interval)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if t and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=2.0)


__all__ = [
    "RecoveryProbe",
    "StuckFault",
    "healthcheck_recovery",
    "DEFAULT_MIN_HOLD_SEC",
    "DEFAULT_RECOVERY_TIMEOUT_SEC",
]
