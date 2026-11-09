"""
Periodic nemesis scheduling: orchestrator-owned state, dispatch to agents, background loop.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, List

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.command_dispatcher import CommandDispatcher
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand
from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    NEMESIS_TYPES,
    confirm_timeout_for,
    guard_mode_for,
    impact_scope_for,
    recovery_mode_for,
    recovery_sec_for,
    stuck_timeout_for,
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import ChaosOrchestratorStore
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    DEFAULT_RECOVERY_SEC,
    FailureModelGuard,
    GuardMode,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
    hc_predicate_for,
    needs_baseline,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class OrchestratorNemesisSchedule:
    """
    Legacy per-type schedule threads. Delivery and history belong to the shared dispatcher.
    """

    def __init__(
        self,
        *,
        chaos_store: ChaosOrchestratorStore,
        get_hosts: Callable[[], List[str]],
        dispatcher: CommandDispatcher,
        failure_guard: FailureModelGuard | None = None,
        recovery_probe=None,
        inventory=None,
        predicate_for: Callable = hc_predicate_for,
        stuck_timeout_for: Callable[[str], float] = stuck_timeout_for,
        metrics=None,
    ) -> None:
        self._lock = threading.RLock()
        self._tasks: dict[str, dict] = {}
        self._dispatcher = dispatcher
        self._chaos_store = chaos_store
        self._get_hosts = get_hosts
        self._failure_guard = failure_guard
        self._recovery_probe = recovery_probe
        self._inventory = inventory
        self._predicate_for = predicate_for
        self._stuck_timeout_for = stuck_timeout_for
        self._metrics = metrics

    def set_metrics(self, metrics) -> None:
        self._metrics = metrics

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def is_schedule_enabled(self, process_type: str) -> bool:
        with self._lock:
            info = self._tasks.get(process_type)
            return bool(info and info.get("enabled"))

    def has_task(self, process_type: str) -> bool:
        with self._lock:
            return process_type in self._tasks

    def enable_schedule(
        self,
        process_type: str,
        interval: int | None,
    ) -> bool:
        """
        Start a daemon thread running _run_schedule_loop and record the task.
        Returns False if this process_type is already scheduled as enabled.
        """
        with self._lock:
            if process_type in self._tasks and self._tasks[process_type].get("enabled"):
                return False
            thread = threading.Thread(target=lambda: self._run_schedule_loop(process_type, interval), daemon=True)
            thread.start()
            self._tasks[process_type] = {
                "enabled": True,
                "interval": interval,
                "thread": thread,
            }
            return True

    def disable_and_remove(self, process_type: str) -> bool:
        with self._lock:
            if process_type not in self._tasks:
                return False
            self._tasks[process_type]["enabled"] = False
            del self._tasks[process_type]
            return True

    def mark_disabled_before_flush(self, process_type: str) -> bool:
        with self._lock:
            if process_type not in self._tasks:
                return False
            self._tasks[process_type]["enabled"] = False
            return True

    def remove_task_entry(self, process_type: str) -> None:
        with self._lock:
            self._tasks.pop(process_type, None)

    def should_stop_loop(self, process_type: str) -> bool:
        with self._lock:
            if process_type not in self._tasks:
                return True
            return not self._tasks[process_type].get("enabled", False)

    def schedule_status_for_types(self, process_types: Iterable[str]) -> dict[str, dict]:
        with self._lock:
            result = {}
            for pt in process_types:
                if pt in self._tasks and self._tasks[pt].get("enabled"):
                    result[pt] = {
                        "enabled": True,
                        "interval": self._tasks[pt].get("interval"),
                    }
                else:
                    result[pt] = {"enabled": False, "interval": None}
            return result

    def shutdown_disable_all(self) -> None:
        """App teardown: flush extract commands to agents, then disable all tasks."""
        with self._lock:
            to_flush = [pt for pt, info in self._tasks.items() if info.get("enabled")]
        for process_type in to_flush:
            try:
                self.flush_disable_extracts(process_type)
            except Exception as e:
                logger.error("Failed to flush extracts for %s on shutdown: %s", process_type, e)
        with self._lock:
            for info in self._tasks.values():
                info["enabled"] = False

    def enable_all_schedules(self, *, uniform_interval: int | None = None) -> dict[str, str]:
        """
        Enable schedule for every type in NEMESIS_TYPES.
        If uniform_interval is set, use it for all; otherwise each type uses catalog ``schedule`` (default 60).
        Returns process_type -> ``started`` or ``already_enabled``.
        """
        results: dict[str, str] = {}
        for process_type, defn in NEMESIS_TYPES.items():
            interval = (
                uniform_interval
                if uniform_interval is not None
                else int(defn.get("schedule") or 60)
            )
            started = self.enable_schedule(process_type, interval)
            results[process_type] = "started" if started else "already_enabled"
        return results

    def disable_all_schedules(self) -> list[str]:
        """
        Same as turning off schedule for each enabled type: flush planner extracts, remove tasks.
        Returns nemesis types that were scheduled and are now stopped.
        """
        with self._lock:
            to_stop = list(self._tasks.keys())
        stopped: list[str] = []
        for process_type in to_stop:
            with self._lock:
                if not self.has_task(process_type):
                    continue
                self.mark_disabled_before_flush(process_type)
                self.flush_disable_extracts(process_type)
                self.remove_task_entry(process_type)
            stopped.append(process_type)
        return stopped

    def flush_disable_extracts(self, process_type: str) -> None:
        """Extract still-held faults when schedule is turned off.

        Planner ``extract_all_on_disable`` covers types that bookkeep hosts themselves.
        Extract-mode toggles (Network/Dns/TimeSkew) inject via ``_direct_commands`` and never
        fill that set — the recovery probe is the source of truth for those.
        """
        if process_type not in NEMESIS_TYPES:
            return
        cmds = self._chaos_store.plan_disable_schedule(process_type)
        if cmds:
            logger.info("Disable schedule: %d extract dispatch(es) for %s", len(cmds), process_type)
            for cmd in cmds:
                self._dispatch_and_record(cmd)
        probe = self._recovery_probe
        if probe is not None:
            drained = probe.drain_extracts(nemesis_type=process_type)
            if drained:
                logger.info(
                    "Disable schedule: drained %d probe toggle(s) for %s",
                    drained,
                    process_type,
                )

    def _extract_action(self, nemesis_type: str, cmd: DispatchCommand) -> Callable[[], None]:
        """Probe-driven extract for toggle faults (lease blocks planner toggle-back)."""
        target = cmd.target

        def _recover() -> None:
            for extract_cmd in self._chaos_store.plan_extract_target(nemesis_type, target):
                if not self._dispatcher.dispatch(extract_cmd, track_history=True):
                    raise RuntimeError(
                        f"extract dispatch failed for {nemesis_type} on {target.identity_key()}"
                    )

        return _recover

    def _dispatch_and_record(self, cmd: DispatchCommand) -> None:
        """Dispatch and account for impact (no veto)."""
        guard = self._failure_guard
        full = guard is not None and guard_mode_for(cmd.nemesis_type) is GuardMode.FULL
        probe = self._recovery_probe
        kind = target_kind_for(cmd.nemesis_type)
        scope = impact_scope_for(cmd.nemesis_type)
        baseline = None
        if full and cmd.action == "inject" and probe is not None and needs_baseline(kind, scope):
            # Pre-dispatch; skip when blind — same as the boundary scheduler.
            footprint = guard.footprint_for(cmd.target, scope)
            baseline = (probe.pile_compute_baseline(dict(footprint.slots_by_pile))
                        if footprint.slots_by_pile else probe.alive_compute_baseline())
            if baseline is None:
                logger.warning(
                    "skipping %s on %s: no fresh healthcheck data for a slot baseline",
                    cmd.nemesis_type, cmd.target.identity_key(),
                )
                return
        if not self._dispatcher.dispatch(cmd, track_history=True):
            return
        if not full:
            return
        if cmd.action == "extract":
            guard.record_extract(
                cmd.execution_id,
                cmd.target,
                scope,
                nemesis_type=cmd.nemesis_type,
                source="legacy",
            )
            if probe is not None:
                probe.untrack_identity(cmd.target.identity_key())
            metrics = self._metrics
            if metrics is not None:
                metrics.fault_ended(
                    target=cmd.target,
                    nemesis_type=cmd.nemesis_type,
                    reason="extract",
                    lease_id=cmd.execution_id,
                    execution_id=cmd.execution_id,
                    source="legacy",
                    guard_mode="full",
                )
        elif cmd.action == "inject":
            # Held until HC confirms; toggle also gets a timed extract via the probe.
            guard.record_inject(
                cmd.execution_id,
                cmd.target,
                scope,
                nemesis_type=cmd.nemesis_type,
                source="legacy",
            )
            if probe is not None:
                track_kwargs: dict = {}
                if recovery_mode_for(cmd.nemesis_type) == "extract":
                    track_kwargs = {
                        "recover_action": self._extract_action(cmd.nemesis_type, cmd),
                        "extract_after_sec": self._extract_after_sec(cmd.nemesis_type),
                        "confirm_timeout_sec": confirm_timeout_for(cmd.nemesis_type),
                    }
                probe.track(
                    cmd.execution_id,
                    cmd.target,
                    cmd.nemesis_type,
                    recovered=self._predicate_for(
                        cmd.target, kind=kind, scope=scope,
                        inventory=self._inventory, baseline=baseline,
                        nemesis_type=cmd.nemesis_type,
                    ),
                    stuck_timeout_sec=self._stuck_timeout_for(cmd.nemesis_type),
                    **track_kwargs,
                )

    @staticmethod
    def _extract_after_sec(nemesis_type: str) -> float:
        recovery = recovery_sec_for(nemesis_type)
        return float(recovery) if recovery is not None else DEFAULT_RECOVERY_SEC

    def _run_planned_tick(self, process_type: str) -> None:
        hosts = self._get_hosts()
        cmds = self._chaos_store.plan_scheduled_tick(process_type, hosts)
        if not cmds:
            logger.info("Planner %s produced no commands this tick (hosts=%d)", process_type, len(hosts))
            return
        logger.info("Running %d dispatch(es) for %s", len(cmds), process_type)
        with ThreadPoolExecutor(max_workers=min(len(cmds), 10)) as executor:
            futures = [executor.submit(self._dispatch_and_record, cmd) for cmd in cmds]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error("Scheduled tick task failed: %s", e)

    def _run_schedule_loop(self, process_type: str, interval: int | None) -> None:
        """
        Daemon thread body: while schedule is enabled, plan a tick and sleep.
        Disable-time extract runs in set_schedule (flush_disable_extracts) before task removal.
        """
        sleep_s = float(interval) if interval is not None else 60.0
        if sleep_s < 1.0:
            sleep_s = 1.0

        while True:
            if self.should_stop_loop(process_type):
                break
            try:
                self._run_planned_tick(process_type)
            except Exception:
                logger.exception("Scheduled tick for %s raised", process_type)
            time.sleep(sleep_s)
