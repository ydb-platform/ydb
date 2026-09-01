"""Fact-based recovery: release reserved budget when an HcSnapshot predicate says the target is back.

Self-healing faults wait on the predicate; toggle faults extract after ``extract_after_sec``, then
confirm. Blind (stale HC): releases/stuck pause, scheduled extracts still fire.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import FailureModelGuard
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
    HcSnapshot,
    build_snapshot,
)

logger = logging.getLogger(__name__)

# Ignore recovery signals this long so a stale pre-fault HC is not read as recovery.
DEFAULT_MIN_HOLD_SEC: float = 30.0
DEFAULT_POLL_INTERVAL_SEC: float = 15.0
# Stale beyond this → blind (HC period 15s + up to ~50s per tick).
DEFAULT_MAX_HC_AGE_SEC: float = 180.0

PHASE_HOLD = "hold"
PHASE_CONFIRM = "confirm"


@dataclass(frozen=True)
class StuckFault:
    """A reserved fault that has not recovered within its timeout (budget still held)."""

    lease_id: str
    nemesis_type: str
    target: ChaosTarget
    held_sec: float
    timeout_sec: float
    phase: str = PHASE_HOLD


@dataclass
class _Pending:
    lease_id: str
    target: ChaosTarget
    nemesis_type: str
    reserved_at: float
    recovered: Callable[[HcSnapshot], bool]
    stuck_timeout_sec: float
    min_hold_sec: float
    # toggle faults: dispatch recover_action after extract_after_sec, then confirm by predicate
    recover_action: Callable[[], None] | None = None
    extract_after_sec: float | None = None
    confirm_timeout_sec: float | None = None
    phase: str = PHASE_HOLD
    confirm_since: float | None = None
    extract_ok: bool = False  # True only after recover_action() succeeds
    extracting: bool = False  # in-flight guard vs concurrent drain/tick
    stuck_reported: bool = False


class RecoveryProbe:
    def __init__(
        self,
        *,
        guard: FailureModelGuard,
        hc_source,
        on_stuck: Callable[[StuckFault], None] | None = None,
        on_blind: Callable[[], None] | None = None,
        on_sighted: Callable[[], None] | None = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SEC,
        min_hold_sec: float = DEFAULT_MIN_HOLD_SEC,
        max_hc_age_sec: float = DEFAULT_MAX_HC_AGE_SEC,
        clock: Callable[[], float] = time.monotonic,
        metrics=None,
    ) -> None:
        self._guard = guard
        self._hc_source = hc_source  # duck-typed: .last_results (dict), .last_update (monotonic)
        self._on_stuck = on_stuck
        self._on_blind = on_blind
        self._on_sighted = on_sighted
        self._poll_interval = float(poll_interval)
        self._min_hold_sec = float(min_hold_sec)
        self._max_hc_age_sec = float(max_hc_age_sec)
        self._clock = clock
        self._metrics = metrics
        self._lock = threading.Lock()
        self._pending: dict[str, _Pending] = {}
        self._blind = False
        self._blind_since: float | None = None
        self._blind_reported = False
        self._ever_fresh = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def set_metrics(self, metrics) -> None:
        self._metrics = metrics

    # -- healthcheck view -----------------------------------------------------

    def snapshot_now(self) -> HcSnapshot:
        """Current healthcheck snapshot (also used to capture pre-inject baselines)."""
        return build_snapshot(
            getattr(self._hc_source, "last_results", None) or {},
            now=self._clock(),
            last_update=getattr(self._hc_source, "last_update", None),
            max_age_sec=self._max_hc_age_sec,
        )

    def alive_compute_baseline(self) -> int | None:
        """Alive compute nodes right now; None when blind (a slot inject must not proceed)."""
        snap = self.snapshot_now()
        return snap.alive_compute if snap.fresh else None

    # -- tracking ---------------------------------------------------------------

    def track(
        self,
        lease_id: str,
        target: ChaosTarget,
        nemesis_type: str,
        *,
        recovered: Callable[[HcSnapshot], bool],
        stuck_timeout_sec: float,
        recover_action: Callable[[], None] | None = None,
        extract_after_sec: float | None = None,
        confirm_timeout_sec: float | None = None,
    ) -> None:
        """Track until HC confirms recovery. Toggle: extract after ``extract_after_sec``, then confirm."""
        if not lease_id:
            return
        timeout = float(stuck_timeout_sec)
        with self._lock:
            self._pending[lease_id] = _Pending(
                lease_id=lease_id,
                target=target,
                nemesis_type=nemesis_type,
                reserved_at=self._clock(),
                recovered=recovered,
                stuck_timeout_sec=timeout,
                min_hold_sec=min(self._min_hold_sec, timeout),
                recover_action=recover_action,
                extract_after_sec=(
                    float(extract_after_sec) if extract_after_sec is not None else None
                ),
                confirm_timeout_sec=(
                    float(confirm_timeout_sec) if confirm_timeout_sec is not None else timeout
                ),
            )

    def tick(self) -> list[StuckFault]:
        """Poll once: release recovered, return newly-stuck."""
        now = self._clock()
        snap = self.snapshot_now()
        self._set_blind(not snap.fresh, now)
        with self._lock:
            items = list(self._pending.values())
        stuck: list[StuckFault] = []
        for p in items:
            held = now - p.reserved_at
            if p.recover_action is not None and p.phase == PHASE_HOLD:
                # Extracts still fire while blind — do not extend the fault.
                if held >= (p.extract_after_sec or 0.0):
                    self._dispatch_extract(p, now)
                continue
            if p.recover_action is not None and p.phase == PHASE_CONFIRM and not p.extract_ok:
                # Previous recover_action failed: retry; confirm_timeout still runs from first try.
                self._dispatch_extract(p, now)
                if not p.extract_ok:
                    fault = self._confirm_stuck_if_due(p, held, now)
                    if fault is not None:
                        stuck.append(fault)
                    continue  # no HC release until an extract actually landed
            if not snap.fresh:
                continue  # blind: no releases, no stuck
            if held < p.min_hold_sec:
                continue
            # Only trust HC data produced after the fault / extract.
            evidence_after = p.confirm_since if p.phase == PHASE_CONFIRM else p.reserved_at
            if snap.data_at is not None and snap.data_at < evidence_after:
                continue
            try:
                is_recovered = p.recovered(snap)
            except Exception:
                logger.exception("recovery check raised for %s", p.target.identity_key())
                is_recovered = False
            if is_recovered:
                self._release(p, held)
                continue
            fault: StuckFault | None = None
            if p.phase == PHASE_CONFIRM:
                fault = self._confirm_stuck_if_due(p, held, now)
            elif held > p.stuck_timeout_sec and not p.stuck_reported:
                fault = self._mark_stuck(p, held, p.stuck_timeout_sec)
            if fault is not None:
                stuck.append(fault)
        for info in stuck:
            if self._on_stuck is not None:
                try:
                    self._on_stuck(info)
                except Exception:
                    logger.exception("on_stuck callback raised")
        return stuck

    def drain_extracts(self, nemesis_type: str | None = None) -> int:
        """Extract toggles still needing it (HOLD, or CONFIRM after a failed recover_action).

        If ``nemesis_type`` is set, only that type is drained (legacy per-type schedule disable).
        """
        with self._lock:
            pending = [
                p
                for p in self._pending.values()
                if p.recover_action is not None
                and (nemesis_type is None or p.nemesis_type == nemesis_type)
                and (p.phase == PHASE_HOLD or (p.phase == PHASE_CONFIRM and not p.extract_ok))
            ]
        if not pending:
            return 0
        logger.info(
            "draining %d pending extract(s)%s",
            len(pending),
            f" for {nemesis_type}" if nemesis_type else " on shutdown",
        )
        now = self._clock()
        for p in pending:
            self._dispatch_extract(p, now, reason="drained on shutdown")
        return len(pending)

    def untrack_identity(self, identity_key: str) -> int:
        """Drop pendings for ``identity_key`` (pairs with guard ``record_extract`` by identity)."""
        with self._lock:
            doomed = [
                lease_id
                for lease_id, p in self._pending.items()
                if p.target.identity_key() == identity_key
            ]
            for lease_id in doomed:
                self._pending.pop(lease_id, None)
        if doomed:
            logger.info("untracked %d pending fault(s) for %s (explicit extract)", len(doomed), identity_key)
        return len(doomed)

    def pending(self) -> list[_Pending]:
        with self._lock:
            return list(self._pending.values())

    def snapshot(self) -> dict:
        now = self._clock()
        with self._lock:
            faults = [
                {
                    "nemesis_type": p.nemesis_type,
                    "host": p.target.host,
                    "identity_key": p.target.identity_key(),
                    "phase": p.phase,
                    "held_sec": round(now - p.reserved_at, 1),
                    "stuck": p.stuck_reported,
                    "toggle": p.recover_action is not None,
                }
                for p in self._pending.values()
            ]
            faults.sort(key=lambda f: (-f["held_sec"], f["nemesis_type"], f["identity_key"]))
            return {
                "tracked": len(self._pending),
                "stuck": sum(1 for p in self._pending.values() if p.stuck_reported),
                "confirming": sum(1 for p in self._pending.values() if p.phase == PHASE_CONFIRM),
                "blind": self._blind,
                "faults": faults,
            }

    # -- internals ----------------------------------------------------------------

    def _release(self, p: _Pending, held: float) -> None:
        # Skip if manual extract already untracked us (avoids double fault_ended).
        with self._lock:
            if self._pending.get(p.lease_id) is not p:
                return
            self._pending.pop(p.lease_id, None)
        released = self._guard.release(
            p.lease_id,
            reason="recovered",
            target=p.target,
            nemesis_type=p.nemesis_type,
            source="probe",
        )
        metrics = self._metrics
        if metrics is not None and released:
            metrics.fault_ended(
                target=p.target,
                nemesis_type=p.nemesis_type,
                reason="recovered",
                lease_id=p.lease_id,
                execution_id=p.lease_id,
                held_sec=held,
                source="probe",
                guard_mode="full",
            )
        logger.info(
            "recovered: %s (%s) after %.0fs [%s]; budget released",
            p.target.host, p.nemesis_type, held, p.phase,
        )

    def _dispatch_extract(self, p: _Pending, now: float, reason: str = "hold elapsed") -> None:
        """Dispatch extract; CONFIRM starts on first attempt, extract_ok only after success."""
        with self._lock:
            if p.phase == PHASE_HOLD:
                p.phase = PHASE_CONFIRM
                p.confirm_since = now
            elif p.phase != PHASE_CONFIRM or p.extract_ok or p.extracting:
                return
            p.extracting = True
        try:
            p.recover_action()
        except Exception:
            # Leave extract_ok False: tick/drain will retry; confirm_timeout → stuck.
            logger.exception("extract action raised for %s", p.target.identity_key())
            with self._lock:
                p.extracting = False
            return
        with self._lock:
            p.extract_ok = True
            p.extracting = False
        logger.info(
            "extract dispatched: %s (%s) after %.0fs hold (%s); awaiting healthcheck confirm",
            p.target.host, p.nemesis_type, now - p.reserved_at, reason,
        )

    def _confirm_stuck_if_due(self, p: _Pending, held: float, now: float) -> StuckFault | None:
        if (
            p.confirm_since is not None
            and now - p.confirm_since > (p.confirm_timeout_sec or 0.0)
            and not p.stuck_reported
        ):
            return self._mark_stuck(p, held, p.confirm_timeout_sec or 0.0)
        return None

    def _mark_stuck(self, p: _Pending, held: float, timeout_sec: float) -> StuckFault | None:
        with self._lock:
            if self._pending.get(p.lease_id) is not p:  # raced with untrack_identity
                return None
            p.stuck_reported = True
        logger.error(
            "fault did not recover within %.0fs [%s]; holding budget: %s (%s)",
            timeout_sec, p.phase, p.target.host, p.nemesis_type,
        )
        metrics = self._metrics
        if metrics is not None:
            metrics.fault_stuck(
                target=p.target,
                nemesis_type=p.nemesis_type,
                lease_id=p.lease_id,
                held_sec=held,
                timeout_sec=timeout_sec,
                phase=p.phase,
                source="probe",
            )
        return StuckFault(
            lease_id=p.lease_id,
            nemesis_type=p.nemesis_type,
            target=p.target,
            held_sec=held,
            timeout_sec=timeout_sec,
            phase=p.phase,
        )

    def _set_blind(self, blind: bool, now: float) -> None:
        """Track blindness; report only after grace on first boot, immediately if sight was lost."""
        report = False
        sighted = False
        with self._lock:
            if blind:
                if self._blind_since is None:
                    self._blind_since = now
                self._blind = True
                if not self._blind_reported and (
                    self._ever_fresh or now - self._blind_since >= self._max_hc_age_sec
                ):
                    self._blind_reported = True
                    report = True
            else:
                sighted = self._blind
                self._blind = False
                self._blind_since = None
                self._blind_reported = False
                self._ever_fresh = True
        if report:
            logger.error("recovery probe is blind: no fresh healthcheck data; releases paused")
            if self._on_blind is not None:
                try:
                    self._on_blind()
                except Exception:
                    logger.exception("on_blind callback raised")
        elif sighted:
            logger.info("recovery probe can see again; releases resumed")
            if self._on_sighted is not None:
                try:
                    self._on_sighted()
                except Exception:
                    logger.exception("on_sighted callback raised")

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
    "PHASE_HOLD",
    "PHASE_CONFIRM",
    "DEFAULT_MIN_HOLD_SEC",
    "DEFAULT_MAX_HC_AGE_SEC",
]
