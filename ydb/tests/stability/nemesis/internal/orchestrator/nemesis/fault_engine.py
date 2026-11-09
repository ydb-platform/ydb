"""Shared fault admission, reservation, dispatch and recovery ownership."""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import Callable

from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    confirm_timeout_for,
    guard_mode_for,
    impact_scope_for,
    recovery_mode_for as catalog_recovery_mode_for,
    recovery_sec_for,
    stuck_timeout_for,
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    DEFAULT_RECOVERY_SEC,
    Footprint,
    GuardMode,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
    hc_predicate_for,
    needs_baseline,
)

class FaultRejected(RuntimeError):
    status_code = 409


class RecoveryUnavailable(FaultRejected):
    status_code = 503


class DispatchFailed(RuntimeError):
    status_code = 502


@dataclass(frozen=True)
class FaultPlan:
    nemesis_type: str
    target: ChaosTarget
    commands: tuple[DispatchCommand, ...]
    footprint: Footprint
    source: str


class FaultEngine:
    """Own admission, reservation, delivery and recovery for every new fault.

    `admission` is an extension point, not a CMS implementation. It receives the
    complete plan, including BYPASS faults; rejection must happen before dispatch.
    A future permission provider must also integrate renewal/completion with probe
    tracking before it can be enabled. No scheduler may call CMS independently.
    """

    def __init__(self, *, guard, inventory, plan_inject, dispatch, recovery_probe=None,
                 plan_extract=None, scope_for=impact_scope_for, kind_for=target_kind_for,
                 mode_for=guard_mode_for, recovery_sec_for=recovery_sec_for,
                 recovery_mode_for=catalog_recovery_mode_for,
                 stuck_timeout_for=stuck_timeout_for, confirm_timeout_for=confirm_timeout_for,
                 predicate_for=hc_predicate_for, admission=None):
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
        self._admission = admission
        self._lock = threading.RLock()
        self._stopping = threading.Event()
        self._operations_lock = threading.Lock()
        self._operations = {}

    def _record_operation(self, operation_id, plan, *, tracked, restorable):
        piles = set(p for p, _ in plan.footprint.slots_by_pile)
        piles.update(int(d.split("/", 1)[0][5:]) for d in plan.footprint.racks if d.startswith("pile:"))
        with self._operations_lock:
            # Never evict active operations; retain the latest 100 completed records.
            terminal = [key for key, row in self._operations.items()
                        if row["state"] in ("recovered", "accepted", "aborted", "delivery_failed")]
            for key in terminal[:-99]:
                self._operations.pop(key, None)
            self._operations[operation_id] = {
                "operation_id": operation_id, "type": plan.nemesis_type,
                "source": plan.source, "target": plan.target.to_dict(),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "state": "injecting", "delivery": "sending", "error": None,
                "tracked": tracked, "restorable": restorable, "piles": sorted(piles),
                "commands": [{"execution_id": c.execution_id, "target": c.target.to_dict()}
                             for c in plan.commands],
            }
        return sorted(piles)

    def _update_operation(self, operation_id, **fields):
        with self._operations_lock:
            if operation_id in self._operations:
                self._operations[operation_id].update(fields)

    def operations(self):
        pending = (self._recovery_probe.snapshot().get("faults", [])
                   if self._recovery_probe is not None else [])
        by_id = {p["operation_id"]: p for p in pending if "operation_id" in p}
        with self._operations_lock:
            rows = deepcopy(list(self._operations.values()))
        for row in rows:
            recovery = by_id.get(row["operation_id"])
            row["recovery"] = recovery
            row["can_restore"] = bool(recovery and recovery["toggle"] and not recovery["dispatching"]
                                      and not recovery["extracting"] and not recovery["extract_ok"])
        return rows[::-1]

    def restore_operation(self, operation_id):
        with self._operations_lock:
            row = self._operations.get(operation_id)
            if row is None:
                raise KeyError(operation_id)
            if row["state"] == "recovered":
                return
            if not row["restorable"]:
                raise FaultRejected("This operation recovers without an explicit restore")
        if self._recovery_probe is None or not self._recovery_probe.restore_operation(operation_id):
            raise FaultRejected("Operation is no longer pending restoration")


    def begin_shutdown(self):
        self._stopping.set()

    def quiesce(self, timeout):
        self.begin_shutdown()
        acquired = self._lock.acquire(timeout=max(0, timeout))
        if acquired:
            self._lock.release()
        return acquired

    def inject(self, nemesis_type, target, *, source="boundary", hold_sec=None):
        # Serializes manual/scheduler admission and dispatch across the shared engine.
        with self._lock:
            if self._stopping.is_set():
                raise FaultRejected("Nemesis is stopping")
            scope, kind = self._scope_for(nemesis_type), self._kind_for(nemesis_type)
            full = self._mode_for(nemesis_type) is not GuardMode.BYPASS
            by_extract = self._recovery_mode_for(nemesis_type) == "extract"
            if hold_sec is not None:
                if not by_extract or isinstance(hold_sec, bool) or not isinstance(hold_sec, (int, float)) or not 1 <= hold_sec <= 86400:
                    raise FaultRejected("hold_sec must be 1..86400 seconds for an extract-mode fault")
            if full and self._recovery_probe is None:
                raise RecoveryUnavailable("No recovery probe")
            if by_extract and (self._plan_extract is None or self._recovery_probe is None):
                raise RecoveryUnavailable("No restore path")
            commands = tuple(self._plan_inject(nemesis_type, target))
            if not commands:
                raise FaultRejected("Planner produced no commands")
            targets = {}
            for t in [target, *(c.target for c in commands)]:
                key = (t.kind, t.group_id) if t.kind in (TargetKind.PILE, TargetKind.DATACENTER) else t.identity_key()
                targets[key] = t
            if any(c.action != "inject" or c.nemesis_type != nemesis_type for c in commands):
                raise FaultRejected("Planner changed fault type or action")
            footprint = Footprint()
            baseline = None
            if full:
                view = self._guard.budget_view()
                if any(t.identity_key() in view.touched for t in targets.values()):
                    raise FaultRejected("Target already has an active fault")
                footprints = [self._guard.footprint_for(t, scope) for t in targets.values()]
                counts = {}
                for fp in footprints:
                    for pile, count in fp.slots_by_pile:
                        counts[pile] = counts.get(pile, 0) + count
                footprint = Footprint(
                    racks=frozenset(d for fp in footprints for d in fp.racks),
                    slots=sum(fp.slots for fp in footprints), slots_by_pile=tuple(counts.items()),
                )
                if footprint.slots_by_pile:
                    baseline = self._recovery_probe.pile_compute_baseline(counts)
                    if baseline is None:
                        raise RecoveryUnavailable("No fresh per-pile compute baseline")
                elif needs_baseline(kind, scope):
                    baseline = self._recovery_probe.alive_compute_baseline()
                    if baseline is None:
                        raise RecoveryUnavailable("No fresh compute baseline")
            plan = FaultPlan(nemesis_type, target, commands, footprint, source)
            if self._admission is not None:
                self._admission.check(plan)
            if not full:
                operation_id = uuid.uuid4().hex
                self._record_operation(operation_id, plan, tracked=False, restorable=False)
                commands = [replace(c, lease_id=operation_id) for c in commands]
                try:
                    for cmd in commands:
                        self._send(cmd)
                except Exception as error:
                    self._update_operation(operation_id, state="delivery_failed", delivery="failed", error=str(error))
                    error.operation_id = operation_id
                    raise
                self._update_operation(operation_id, state="accepted", delivery="accepted")
                return commands
            predicates = [self._predicate_for(t, kind=t.kind, scope=scope,
                inventory=self._inventory, baseline=baseline, nemesis_type=nemesis_type)
                for t in targets.values()]

            def recovered(snap):
                return all(predicate(snap) for predicate in predicates)

            lease = self._guard.reserve(
                footprint, identity_key=target.identity_key(),
                identity_keys=[t.identity_key() for t in targets.values()],
                target=target, nemesis_type=nemesis_type, source=source,
            )
            if lease is None:
                raise FaultRejected("Rejected by failure model")
            commands = [replace(c, lease_id=lease) for c in commands]
            piles = self._record_operation(lease, plan, tracked=True, restorable=by_extract)
            try:
                # Register before dispatch so even the first timeout retains recovery ownership.
                self._track_reserved(lease, target, nemesis_type, recovered=recovered,
                                     by_extract=by_extract, commands=commands, piles=piles,
                                     baseline=baseline, hold_sec=hold_sec)
            except Exception as error:
                self._update_operation(lease, state="aborted", delivery="not_sent", error=str(error))
                self._guard.release(lease, reason="abort", target=target,
                                    nemesis_type=nemesis_type, source=source)
                raise
            try:
                for cmd in commands:
                    self._send(cmd)
                self._update_operation(lease, state="recovering", delivery="accepted")
            except Exception as error:
                self._update_operation(lease, state="recovering", delivery="failed", error=str(error))
                error.operation_id = lease
                raise
            finally:
                # Confirmation must be newer than the last delivery attempt, including failures.
                if hasattr(self._recovery_probe, "dispatched"):
                    self._recovery_probe.dispatched(lease)
            return commands

    def _send(self, cmd):
        if cmd.action == "inject" and self._stopping.is_set():
            raise FaultRejected("Nemesis is stopping")
        if self._dispatch(cmd) is False:
            raise DispatchFailed(f"dispatch failed for {cmd.nemesis_type}")

    def extract(self, nemesis_type, target):
        # The probe arbitrates automatic/manual restore; keep the lease until HC confirmation.
        if self._recovery_probe is not None and self._recovery_probe.restore_identity(
                target.identity_key(), nemesis_type):
            return []
        with self._lock:
            if self._plan_extract is None:
                raise FaultRejected("No restore path")
            commands = list(self._plan_extract(nemesis_type, target))
            if not commands:
                raise FaultRejected("Planner produced no restore commands")
            for cmd in commands:
                self._send(cmd)
            if self._recovery_probe is not None:
                self._recovery_probe.confirm_identity(target.identity_key(), nemesis_type)
            return commands

    def _track_reserved(
        self,
        lease: str,
        target: ChaosTarget,
        nemesis_type: str,
        *,
        recovered: Callable,
        by_extract: bool,
        commands,
        piles,
        baseline,
        hold_sec,
    ) -> None:
        """Register a reserved lease with the recovery probe."""
        if by_extract:
            self._recovery_probe.track(
                lease, target, nemesis_type,
                recovered=recovered,
                dispatching=True, piles=piles,
                pile_baseline=baseline if isinstance(baseline, dict) else {},
                on_recovered=lambda: self._update_operation(lease, state="recovered"),
                stuck_timeout_sec=self._stuck_timeout_for(nemesis_type),
                recover_action=self._extract_action(nemesis_type, commands, lease),
                extract_after_sec=hold_sec if hold_sec is not None else self._extract_after_sec(nemesis_type),
                confirm_timeout_sec=self._confirm_timeout_for(nemesis_type),
            )
        else:
            self._recovery_probe.track(
                lease, target, nemesis_type,
                recovered=recovered,
                dispatching=True, piles=piles,
                pile_baseline=baseline if isinstance(baseline, dict) else {},
                on_recovered=lambda: self._update_operation(lease, state="recovered"),
                stuck_timeout_sec=self._stuck_timeout_for(nemesis_type),
            )

    def _extract_after_sec(self, nemesis_type: str) -> float:
        recovery = self._recovery_sec_for(nemesis_type)
        return float(recovery) if recovery is not None else DEFAULT_RECOVERY_SEC

    def _extract_action(self, nemesis_type, inject_commands, lease):
        # Freeze restore commands for every actual destination, including group fanout.
        targets = {(c.target.identity_key(), c.host): c.target for c in inject_commands}
        commands = []
        for target in targets.values():
            restore = list(self._plan_extract(nemesis_type, target))
            if not restore or any(c.action != "extract" or c.nemesis_type != nemesis_type for c in restore):
                raise FaultRejected("Planner produced no valid restore commands")
            commands.extend(replace(c, lease_id=lease) for c in restore)

        def recover():
            with self._lock:
                for cmd in commands:
                    self._send(cmd)
        return recover
