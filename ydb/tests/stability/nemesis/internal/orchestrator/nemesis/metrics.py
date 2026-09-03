"""Orchestrator-side chaos metrics: structured events + monlib sensors.

Legacy agent counters (``AbstractMonitoredNemesis``) stay as execution health.
This module covers fault lifecycle and failure-model budget leases — scrape
``/sensors`` on ``nemesis_mon_port`` and/or follow ``nemesis_metric`` log lines.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from ydb.tests.tools.nemesis.library import monitor as nemesis_monitor
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget

logger = logging.getLogger(__name__)

# Event names (stable contract for dashboards / log parsers).
EVENT_BUDGET_ACQUIRED = "budget.acquired"
EVENT_BUDGET_RELEASED = "budget.released"
EVENT_BUDGET_ACQUIRE_REJECTED = "budget.acquire_rejected"
EVENT_FAULT_STARTED = "fault.started"
EVENT_FAULT_EXTRACT_DISPATCHED = "fault.extract_dispatched"
EVENT_FAULT_ENDED = "fault.ended"
EVENT_FAULT_STUCK = "fault.stuck"
EVENT_FAULT_DISPATCH_FAILED = "fault.dispatch_failed"

DEFAULT_EVENT_LIMIT = 200


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _target_payload(target: ChaosTarget | None) -> dict[str, Any] | None:
    if target is None:
        return None
    d = target.to_dict()
    d["identity_key"] = target.identity_key()
    return d


def _footprint_payload(footprint: Any) -> dict[str, Any] | None:
    if footprint is None:
        return None
    racks = getattr(footprint, "racks", None)
    slots = getattr(footprint, "slots", 0)
    return {
        "racks": sorted(racks) if racks is not None else [],
        "slots": int(slots or 0),
    }


def _kind_of(target: ChaosTarget | None, identity_key: str | None = None) -> str:
    if target is not None:
        return target.kind.value
    if identity_key:
        return identity_key.split(":", 1)[0]
    return "unknown"


def _nemesis_label(nemesis_type: str | None) -> str:
    """Always present: Monium legends use ``{{nemesis}}`` (legacy Inject* sensors)."""
    return nemesis_type or "unknown"


@dataclass
class MetricsEvent:
    """One chaos/budget transition; serialised as a ``nemesis_metric`` log line."""

    event: str
    ts: str
    run_id: str
    lease_id: str | None = None
    execution_id: str | None = None
    scenario_id: str | None = None
    source: str | None = None
    nemesis_type: str | None = None
    guard_mode: str | None = None
    reason: str | None = None
    held_sec: float | None = None
    target: dict[str, Any] | None = None
    footprint: dict[str, Any] | None = None
    budget_after: dict[str, Any] | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "ts": self.ts,
            "event": self.event,
            "run_id": self.run_id,
        }
        for key in (
            "lease_id",
            "execution_id",
            "scenario_id",
            "source",
            "nemesis_type",
            "guard_mode",
            "reason",
            "held_sec",
            "target",
            "footprint",
            "budget_after",
        ):
            val = getattr(self, key)
            if val is not None:
                d[key] = val
        if self.extra:
            d.update(self.extra)
        return d


class NemesisMetrics:
    """Emit lifecycle events and update monlib gauges/counters (orchestrator process)."""

    def __init__(
        self,
        *,
        mon=None,
        run_id: str | None = None,
        event_limit: int = DEFAULT_EVENT_LIMIT,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._run_id = run_id or uuid.uuid4().hex
        self._mon = mon if mon is not None else nemesis_monitor.monitor()
        self._clock = clock
        self._lock = threading.Lock()
        self._events: deque[dict[str, Any]] = deque(maxlen=max(1, int(event_limit)))
        # identity_key → label sets for clearing gauges
        self._fault_active: dict[str, dict[str, str]] = {}
        self._budget_held: dict[str, dict[str, str]] = {}
        self._stuck_count = 0
        self._gauge_cache: dict[tuple, Any] = {}
        self._counter_cache: dict[tuple, Any] = {}

        base = {"component": "nemesis_orchestrator"}
        self._impaired_racks = self._mon.int_gauge("NemesisBudgetImpairedRacks", base)
        self._impaired_slots = self._mon.int_gauge("NemesisBudgetImpairedSlots", base)
        self._max_slots = self._mon.int_gauge("NemesisBudgetMaxSlots", base)
        self._active_faults_total = self._mon.int_gauge("NemesisFaultActiveTotal", base)
        self._stuck_total = self._mon.int_gauge("NemesisFaultStuckTotal", base)
        self._impaired_racks.set(0)
        self._impaired_slots.set(0)
        self._max_slots.set(0)
        self._active_faults_total.set(0)
        self._stuck_total.set(0)

    @property
    def run_id(self) -> str:
        return self._run_id

    def recent_events(self, n: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            items = list(self._events)
        return items[-n:][::-1]

    # -- budget ---------------------------------------------------------------

    def budget_acquired(
        self,
        *,
        lease_id: str,
        footprint: Any,
        identity_key: str,
        budget_after: dict[str, Any],
        target: ChaosTarget | None = None,
        nemesis_type: str | None = None,
        source: str | None = None,
        guard_mode: str = "full",
    ) -> None:
        kind = _kind_of(target, identity_key)
        nemesis = _nemesis_label(nemesis_type)
        labels = self._entity_labels(kind, identity_key, target, nemesis)
        with self._lock:
            self._budget_held[identity_key] = labels
            self._set_gauge("NemesisBudgetHeld", labels, 1)
            self._inc_counter(
                "NemesisBudgetAcquired",
                {"kind": kind, "nemesis": nemesis},
            )
            self._apply_budget_snapshot(budget_after)
        self._emit(
            EVENT_BUDGET_ACQUIRED,
            lease_id=lease_id,
            execution_id=lease_id,
            source=source,
            nemesis_type=nemesis,
            guard_mode=guard_mode,
            target=_target_payload(target) or {"identity_key": identity_key},
            footprint=_footprint_payload(footprint),
            budget_after=dict(budget_after),
        )

    def budget_released(
        self,
        *,
        lease_id: str,
        footprint: Any | None,
        identity_key: str,
        budget_after: dict[str, Any],
        reason: str = "released",
        target: ChaosTarget | None = None,
        nemesis_type: str | None = None,
        source: str | None = None,
        guard_mode: str = "full",
    ) -> None:
        kind = _kind_of(target, identity_key)
        nemesis = _nemesis_label(nemesis_type)
        with self._lock:
            labels = self._budget_held.pop(identity_key, None) or self._entity_labels(
                kind, identity_key, target, nemesis
            )
            # Clear the same label set that was set on acquire (monlib keys by full labels).
            self._set_gauge("NemesisBudgetHeld", labels, 0)
            self._inc_counter(
                "NemesisBudgetReleased",
                {"kind": kind, "nemesis": nemesis, "reason": reason},
            )
            self._apply_budget_snapshot(budget_after)
        self._emit(
            EVENT_BUDGET_RELEASED,
            lease_id=lease_id,
            execution_id=lease_id,
            source=source,
            nemesis_type=nemesis,
            guard_mode=guard_mode,
            reason=reason,
            target=_target_payload(target) or {"identity_key": identity_key},
            footprint=_footprint_payload(footprint),
            budget_after=dict(budget_after),
        )

    def budget_acquire_rejected(
        self,
        *,
        footprint: Any,
        identity_key: str | None = None,
        budget_after: dict[str, Any] | None = None,
        target: ChaosTarget | None = None,
        nemesis_type: str | None = None,
        source: str | None = None,
    ) -> None:
        kind = _kind_of(target, identity_key)
        nemesis = _nemesis_label(nemesis_type)
        self._inc_counter(
            "NemesisBudgetAcquireRejected",
            {"kind": kind, "nemesis": nemesis},
        )
        self._emit(
            EVENT_BUDGET_ACQUIRE_REJECTED,
            source=source,
            nemesis_type=nemesis,
            target=_target_payload(target)
            or ({"identity_key": identity_key} if identity_key else None),
            footprint=_footprint_payload(footprint),
            budget_after=dict(budget_after) if budget_after else None,
        )

    def sync_budget_gauges(self, snapshot: dict[str, Any]) -> None:
        """Refresh aggregate budget gauges from ``FailureModelGuard.snapshot()``."""
        with self._lock:
            self._apply_budget_snapshot(snapshot)

    # -- fault lifecycle ------------------------------------------------------

    def fault_started(
        self,
        *,
        target: ChaosTarget,
        nemesis_type: str,
        execution_id: str,
        scenario_id: str | None = None,
        lease_id: str | None = None,
        source: str | None = None,
        guard_mode: str | None = None,
    ) -> None:
        identity = target.identity_key()
        nemesis = _nemesis_label(nemesis_type)
        labels = self._entity_labels(target.kind.value, identity, target, nemesis)
        with self._lock:
            self._fault_active[identity] = labels
            self._set_gauge("NemesisFaultActive", labels, 1)
            self._active_faults_total.set(len(self._fault_active))
            self._inc_counter(
                "NemesisFaultStarted",
                {"kind": target.kind.value, "nemesis": nemesis},
            )
        self._emit(
            EVENT_FAULT_STARTED,
            lease_id=lease_id or execution_id,
            execution_id=execution_id,
            scenario_id=scenario_id,
            source=source,
            nemesis_type=nemesis,
            guard_mode=guard_mode,
            target=_target_payload(target),
        )

    def fault_extract_dispatched(
        self,
        *,
        target: ChaosTarget,
        nemesis_type: str,
        execution_id: str,
        scenario_id: str | None = None,
        lease_id: str | None = None,
        source: str | None = None,
        reason: str | None = None,
    ) -> None:
        nemesis = _nemesis_label(nemesis_type)
        self._inc_counter(
            "NemesisFaultExtractDispatched",
            {"kind": target.kind.value, "nemesis": nemesis},
        )
        self._emit(
            EVENT_FAULT_EXTRACT_DISPATCHED,
            lease_id=lease_id,
            execution_id=execution_id,
            scenario_id=scenario_id,
            source=source,
            nemesis_type=nemesis,
            reason=reason,
            target=_target_payload(target),
        )

    def fault_ended(
        self,
        *,
        target: ChaosTarget,
        nemesis_type: str,
        reason: str = "recovered",
        lease_id: str | None = None,
        execution_id: str | None = None,
        held_sec: float | None = None,
        source: str | None = None,
        guard_mode: str | None = None,
    ) -> None:
        identity = target.identity_key()
        kind = target.kind.value
        nemesis = _nemesis_label(nemesis_type)
        with self._lock:
            labels = self._fault_active.pop(identity, None) or self._entity_labels(
                kind, identity, target, nemesis
            )
            self._set_gauge("NemesisFaultActive", labels, 0)
            self._active_faults_total.set(len(self._fault_active))
            self._inc_counter(
                "NemesisFaultEnded",
                {"kind": kind, "nemesis": nemesis, "reason": reason},
            )
            if held_sec is not None:
                hold_labels = {"kind": kind, "nemesis": nemesis}
                self._inc_counter("NemesisFaultHoldCount", hold_labels)
                # Integer seconds are enough for Solomon averages (sum/count).
                self._add_counter("NemesisFaultHoldSecondsSum", hold_labels, int(held_sec))
        self._emit(
            EVENT_FAULT_ENDED,
            lease_id=lease_id,
            execution_id=execution_id or lease_id,
            source=source,
            nemesis_type=nemesis,
            guard_mode=guard_mode,
            reason=reason,
            held_sec=held_sec,
            target=_target_payload(target),
        )

    def fault_stuck(
        self,
        *,
        target: ChaosTarget,
        nemesis_type: str,
        lease_id: str,
        held_sec: float,
        timeout_sec: float,
        phase: str,
        source: str | None = "probe",
    ) -> None:
        nemesis = _nemesis_label(nemesis_type)
        with self._lock:
            self._stuck_count += 1
            self._stuck_total.set(self._stuck_count)
            self._inc_counter(
                "NemesisFaultStuck",
                {"kind": target.kind.value, "nemesis": nemesis, "phase": phase},
            )
        self._emit(
            EVENT_FAULT_STUCK,
            lease_id=lease_id,
            execution_id=lease_id,
            source=source,
            nemesis_type=nemesis,
            reason=phase,
            held_sec=held_sec,
            target=_target_payload(target),
            extra={"timeout_sec": timeout_sec, "phase": phase},
        )

    def fault_dispatch_failed(
        self,
        *,
        target: ChaosTarget,
        nemesis_type: str,
        action: str,
        execution_id: str,
        scenario_id: str | None = None,
        source: str | None = None,
        error: str | None = None,
    ) -> None:
        nemesis = _nemesis_label(nemesis_type)
        self._inc_counter(
            "NemesisFaultDispatchFailed",
            {"kind": target.kind.value, "nemesis": nemesis, "action": action},
        )
        self._emit(
            EVENT_FAULT_DISPATCH_FAILED,
            execution_id=execution_id,
            scenario_id=scenario_id,
            source=source,
            nemesis_type=nemesis,
            reason=action,
            target=_target_payload(target),
            extra={"error": error} if error else {},
        )

    # -- internals ------------------------------------------------------------

    def _entity_labels(
        self,
        kind: str,
        identity_key: str,
        target: ChaosTarget | None,
        nemesis_type: str | None,
    ) -> dict[str, str]:
        return {
            "component": "nemesis_orchestrator",
            "kind": kind,
            "identity": identity_key,
            "host": target.host if target is not None else "",
            "nemesis": _nemesis_label(nemesis_type),
        }

    def _gauge_key(self, name: str, labels: dict[str, str]) -> tuple:
        return (name, tuple(sorted(labels.items())))

    def _set_gauge(self, name: str, labels: dict[str, str], value: int) -> None:
        key = self._gauge_key(name, labels)
        g = self._gauge_cache.get(key)
        if g is None:
            g = self._mon.int_gauge(name, labels)
            self._gauge_cache[key] = g
        g.set(int(value))

    def _inc_counter(self, name: str, labels: dict[str, str], delta: int = 1) -> None:
        self._add_counter(name, labels, delta)

    def _add_counter(self, name: str, labels: dict[str, str], delta: int) -> None:
        if delta <= 0:
            return
        full = dict(labels)
        full.setdefault("component", "nemesis_orchestrator")
        key = self._gauge_key(name, full)
        c = self._counter_cache.get(key)
        if c is None:
            c = self._mon.counter(name, full)
            self._counter_cache[key] = c
        if delta == 1:
            c.inc()
        else:
            c.add(int(delta))

    def _apply_budget_snapshot(self, snapshot: dict[str, Any]) -> None:
        racks = snapshot.get("impaired_racks") or []
        self._impaired_racks.set(len(racks) if not isinstance(racks, int) else racks)
        self._impaired_slots.set(int(snapshot.get("impaired_slots") or 0))
        if "max_slots" in snapshot:
            self._max_slots.set(int(snapshot.get("max_slots") or 0))

    def _emit(self, event: str, **kwargs: Any) -> None:
        ev = MetricsEvent(event=event, ts=_utc_now_iso(), run_id=self._run_id, **kwargs)
        payload = ev.to_dict()
        with self._lock:
            self._events.append(payload)
        self._log_event(event, payload)

    def _log_event(self, event: str, payload: dict[str, Any]) -> None:
        """Human-readable line + machine-readable JSON (both always emitted)."""
        target = payload.get("target") or {}
        identity = target.get("identity_key") or ""
        host = target.get("host") or ""
        nemesis = payload.get("nemesis_type") or "unknown"
        lease = payload.get("lease_id") or payload.get("execution_id") or "-"
        reason = payload.get("reason")
        source = payload.get("source") or "-"
        held = payload.get("held_sec")
        fp = payload.get("footprint") or {}
        racks = ",".join(fp.get("racks") or []) or "-"
        slots = fp.get("slots", 0)

        if event == EVENT_BUDGET_ACQUIRED:
            summary = (
                f"budget acquired: {nemesis} identity={identity} host={host} "
                f"lease={lease} racks=[{racks}] slots={slots} source={source}"
            )
        elif event == EVENT_BUDGET_RELEASED:
            summary = (
                f"budget released: {nemesis} identity={identity} host={host} "
                f"lease={lease} reason={reason} source={source}"
            )
        elif event == EVENT_BUDGET_ACQUIRE_REJECTED:
            summary = (
                f"budget acquire rejected: {nemesis} identity={identity or '-'} "
                f"host={host or '-'} racks=[{racks}] slots={slots} source={source}"
            )
        elif event == EVENT_FAULT_STARTED:
            summary = (
                f"fault started: {nemesis} identity={identity} host={host} "
                f"lease={lease} source={source}"
            )
        elif event == EVENT_FAULT_EXTRACT_DISPATCHED:
            summary = (
                f"fault extract dispatched: {nemesis} identity={identity} host={host} "
                f"lease={lease} source={source}"
            )
        elif event == EVENT_FAULT_ENDED:
            held_s = f" held={held:.0f}s" if isinstance(held, (int, float)) else ""
            summary = (
                f"fault ended: {nemesis} identity={identity} host={host} "
                f"lease={lease} reason={reason}{held_s} source={source}"
            )
        elif event == EVENT_FAULT_STUCK:
            timeout = payload.get("timeout_sec")
            summary = (
                f"fault stuck: {nemesis} identity={identity} host={host} "
                f"lease={lease} phase={reason} held={held:.0f}s timeout={timeout}s"
                if isinstance(held, (int, float))
                else (
                    f"fault stuck: {nemesis} identity={identity} host={host} "
                    f"lease={lease} phase={reason}"
                )
            )
        elif event == EVENT_FAULT_DISPATCH_FAILED:
            err = payload.get("error") or ""
            summary = (
                f"fault dispatch failed: {nemesis} identity={identity} host={host} "
                f"action={reason} error={err}"
            )
        else:
            summary = f"nemesis event: {event} nemesis={nemesis} identity={identity}"

        try:
            raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        except Exception:
            logger.exception("failed to serialise nemesis_metric event=%s", event)
            logger.info("%s", summary)
            return
        # Two lines: readable ops log + parseable payload for log pipelines.
        logger.info("%s", summary)
        logger.info("nemesis_metric %s", raw)


class NullNemesisMetrics:
    """No-op stand-in for tests that do not care about metrics."""

    run_id = ""

    def recent_events(self, n: int = 50) -> list[dict[str, Any]]:
        return []

    def budget_acquired(self, **kwargs: Any) -> None:
        return None

    def budget_released(self, **kwargs: Any) -> None:
        return None

    def budget_acquire_rejected(self, **kwargs: Any) -> None:
        return None

    def sync_budget_gauges(self, snapshot: dict[str, Any]) -> None:
        return None

    def fault_started(self, **kwargs: Any) -> None:
        return None

    def fault_extract_dispatched(self, **kwargs: Any) -> None:
        return None

    def fault_ended(self, **kwargs: Any) -> None:
        return None

    def fault_stuck(self, **kwargs: Any) -> None:
        return None

    def fault_dispatch_failed(self, **kwargs: Any) -> None:
        return None


def is_metrics(obj: Any) -> bool:
    return obj is not None and not isinstance(obj, NullNemesisMetrics)


__all__ = [
    "NemesisMetrics",
    "NullNemesisMetrics",
    "MetricsEvent",
    "is_metrics",
    "EVENT_BUDGET_ACQUIRED",
    "EVENT_BUDGET_RELEASED",
    "EVENT_BUDGET_ACQUIRE_REJECTED",
    "EVENT_FAULT_STARTED",
    "EVENT_FAULT_EXTRACT_DISPATCHED",
    "EVENT_FAULT_ENDED",
    "EVENT_FAULT_STUCK",
    "EVENT_FAULT_DISPATCH_FAILED",
]
