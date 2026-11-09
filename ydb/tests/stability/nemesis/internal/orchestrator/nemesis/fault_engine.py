"""Shared fault admission, reservation, dispatch and recovery ownership."""

from __future__ import annotations

import threading
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

    def begin_shutdown(self):
        self._stopping.set()

    def quiesce(self, timeout):
        self.begin_shutdown()
        acquired = self._lock.acquire(timeout=max(0, timeout))
        if acquired:
            self._lock.release()
        return acquired

    def inject(self, nemesis_type, target, *, source="boundary"):
        # Serializes manual/scheduler admission and dispatch across the shared engine.
        with self._lock:
            if self._stopping.is_set():
                raise FaultRejected("Nemesis is stopping")
            scope, kind = self._scope_for(nemesis_type), self._kind_for(nemesis_type)
            full = self._mode_for(nemesis_type) is not GuardMode.BYPASS
            by_extract = self._recovery_mode_for(nemesis_type) == "extract"
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
                for cmd in commands:
                    self._send(cmd)
                return list(commands)
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
            try:
                # Register before dispatch so even the first timeout retains recovery ownership.
                self._track_reserved(lease, target, nemesis_type, recovered=recovered,
                                     by_extract=by_extract, commands=commands)
            except Exception:
                self._guard.release(lease, reason="abort", target=target,
                                    nemesis_type=nemesis_type, source=source)
                raise
            try:
                for cmd in commands:
                    self._send(cmd)
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
    ) -> None:
        """Register a reserved lease with the recovery probe."""
        if by_extract:
            self._recovery_probe.track(
                lease, target, nemesis_type,
                recovered=recovered,
                dispatching=True,
                stuck_timeout_sec=self._stuck_timeout_for(nemesis_type),
                recover_action=self._extract_action(nemesis_type, commands, lease),
                extract_after_sec=self._extract_after_sec(nemesis_type),
                confirm_timeout_sec=self._confirm_timeout_for(nemesis_type),
            )
        else:
            self._recovery_probe.track(
                lease, target, nemesis_type,
                recovered=recovered,
                dispatching=True,
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
