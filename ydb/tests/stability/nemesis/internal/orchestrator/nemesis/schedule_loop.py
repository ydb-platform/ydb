"""
Periodic nemesis scheduling: orchestrator-owned state, dispatch to agents, background loop.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable, Iterable, List

import requests

from ydb.tests.stability.nemesis.routers.agent_router import create_process_helper
from ydb.tests.stability.nemesis.internal.nemesis.chaos_dispatch import DispatchCommand
from ydb.tests.stability.nemesis.internal.nemesis.catalog import NEMESIS_TYPES
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import ChaosOrchestratorStore

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

HISTORY_LIMIT = 50


class OrchestratorNemesisSchedule:
    """
    Orchestrator-side scheduler: enabled schedule threads, recent dispatch history,
    and HTTP/local dispatch to agents. Initialized from app.py (see orchestrator_router.nemesis_schedule).
    """

    def __init__(
        self,
        *,
        chaos_store: ChaosOrchestratorStore,
        get_hosts: Callable[[], List[str]],
        is_local_host: Callable[[str], bool],
        get_app_port: Callable[[], int],
        history_limit: int = HISTORY_LIMIT,
    ) -> None:
        self._lock = threading.RLock()
        self._tasks: dict[str, dict] = {}
        self._executions_history: list[dict] = []
        self._history_limit = history_limit
        self._chaos_store = chaos_store
        self._get_hosts = get_hosts
        self._is_local_host = is_local_host
        self._get_app_port = get_app_port

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

    def append_executions_history(self, entry: dict) -> None:
        with self._lock:
            self._executions_history.append(entry)
            if len(self._executions_history) > self._history_limit:
                self._executions_history.pop(0)

    def recent_history(self, n: int = 15) -> list:
        with self._lock:
            return self._executions_history[-n:][::-1]

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

    def dispatch_command(self, cmd: DispatchCommand, *, track_history: bool) -> None:
        """POST (or local helper) to start one execution on cmd.host."""
        try:
            payload = dict(cmd.payload or {})
            payload["host"] = cmd.host
            body = {
                "type": cmd.nemesis_type,
                "action": cmd.action,
                "payload": payload,
            }
            if self._is_local_host(cmd.host):
                create_process_helper(
                    cmd.nemesis_type,
                    cmd.action,
                    payload=payload,
                )
                logger.debug("Started %s on local host (%s)", cmd.nemesis_type, cmd.action)
            else:
                port = self._get_app_port()
                requests.post(
                    f"http://{cmd.host}:{port}/api/processes",
                    json=body,
                    timeout=5,
                )

            if track_history:
                self.append_executions_history(
                    {
                        "type": cmd.nemesis_type,
                        "action": cmd.action,
                        "host": cmd.host,
                        "execution_id": cmd.execution_id,
                        "scenario_id": cmd.scenario_id,
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }
                )
        except Exception as e:
            logger.error("Failed to dispatch %s to %s: %s", cmd.nemesis_type, cmd.host, e)

    def flush_disable_extracts(self, process_type: str) -> None:
        """Plan and run extract on all tracked hosts when schedule is turned off."""
        if process_type not in NEMESIS_TYPES:
            return
        cmds = self._chaos_store.plan_disable_schedule(process_type)
        if not cmds:
            return
        logger.info("Disable schedule: %d extract dispatch(es) for %s", len(cmds), process_type)
        for cmd in cmds:
            self.dispatch_command(cmd, track_history=True)

    def _run_planned_tick(self, process_type: str) -> None:
        hosts = self._get_hosts()
        cmds = self._chaos_store.plan_scheduled_tick(process_type, hosts)
        if not cmds:
            return
        logger.info("Running %d dispatch(es) for %s", len(cmds), process_type)
        with ThreadPoolExecutor(max_workers=min(len(cmds), 10)) as executor:
            futures = [
                executor.submit(self.dispatch_command, cmd, track_history=True)
                for cmd in cmds
            ]
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
            self._run_planned_tick(process_type)
            time.sleep(sleep_s)
