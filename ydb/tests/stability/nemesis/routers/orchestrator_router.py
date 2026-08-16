import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from flask import Blueprint, request, jsonify

from ydb.tests.stability.nemesis.internal.config import Settings
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import ChaosOrchestratorStore
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.schedule_loop import OrchestratorNemesisSchedule
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_checker import OrchestratorWardenChecker
from ydb.tests.stability.nemesis.internal.nemesis.catalog import (
    NEMESIS_TYPES,
    guard_mode_for,
    impact_scope_for,
    nemesis_types_flat_for_api,
    nemesis_types_grouped_for_api,
    stuck_timeout_for,
    supports_boundary_scheduler,
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_problems import ChaosProblemStore
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import FailureModelGuard, GuardMode
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
    hc_predicate_for,
    needs_baseline,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.boundary_scheduler import (
    BoundaryNemesisScheduler,
    default_enabled_types,
)
import ydb.tests.stability.nemesis.routers.agent_router as agent_router


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


blueprint = Blueprint('orchestrator', __name__)

# Module-level state (orchestrator wiring; see app.initialize_app)
hosts: list[str] = []
# Logical hostname → HTTP host (IP, IPv6 bracketed). Empty → fall back to hostname.
host_endpoints: dict[str, str] = {}
mon_port = 8765  # Default monitoring port
orchestrator_warden_checker: OrchestratorWardenChecker | None = None
nemesis_schedule: OrchestratorNemesisSchedule | None = None
nemesis_scheduler: BoundaryNemesisScheduler | None = None
chaos_store: ChaosOrchestratorStore | None = None
failure_guard: FailureModelGuard | None = None
cluster_inventory: ClusterInventory | None = None
chaos_problems: ChaosProblemStore | None = None
healthcheck_reporter: Any = None
recovery_probe: Any = None
nemesis_metrics: Any = None


def get_app_port() -> int:
    """Get the configured app port from settings"""
    return Settings().app_port


def agent_http_host(host: str) -> str:
    """Address used in orchestrator→agent HTTP URLs (cached IP when available)."""
    return host_endpoints.get(host) or host


def agent_url(host: str, path: str = "") -> str:
    """``http://<endpoint>:<port><path>`` for an agent identified by logical ``host``."""
    p = path if not path or path.startswith("/") else f"/{path}"
    return f"http://{agent_http_host(host)}:{get_app_port()}{p}"


def is_local_host(host: str) -> bool:
    """
    True if ``host`` is served by this process's agent API (in-process call, no HTTP loopback).

    Nemesis deploys the orchestrator on the first entry in ``cluster.yaml``; ``hosts`` is that list
    (see ``install_on_hosts`` / ``app.initialize_app``). Only ``hosts[0]`` is local here.
    """
    if not host:
        return False
    if host.strip() in ("localhost", "127.0.0.1", "::1"):
        return True
    if not hosts:
        return False
    return host.strip() == hosts[0].strip()


def fetch_agent_warden_result(host: str) -> dict[str, Any]:
    """HTTP or in-process: last warden JSON from an agent host (injected into OrchestratorWardenChecker)."""
    try:
        if is_local_host(host):
            wc = agent_router.warden_checker
            if wc is None:
                return {"status": "error", "error_message": "warden_checker not initialized"}
            return wc.get_last_result()
        resp = requests.get(agent_url(host, "/api/warden/result"), timeout=10)
        return resp.json()
    except Exception as e:
        logger.error(f"Failed to get warden result from {host}: {e}")
        return {"status": "error", "error_message": str(e)}


@blueprint.route("/api/hosts/<host>/processes", methods=["GET"])
def get_all_host_processes(host: str):
    if is_local_host(host):
        # Direct call to avoid HTTP deadlock
        return jsonify(agent_router.get_all_processes_helper())
    else:
        resp = requests.get(agent_url(host, "/api/processes"), timeout=5)
        return jsonify(resp.json())


def fetch_host_processes(host):
    try:
        if is_local_host(host):
            # Direct call to avoid HTTP deadlock
            return host, agent_router.get_all_processes_helper()
        else:
            resp = requests.get(agent_url(host, "/api/processes"), timeout=5)
            return host, resp.json()
    except Exception as e:
        print(f"Failed to fetch processes from {host}: {e}")
        return host, []


@blueprint.route("/api/hosts/processes", methods=["GET"])
def get_all_processes():
    if not hosts:
        return jsonify({})
    with ThreadPoolExecutor(max_workers=min(len(hosts), 10)) as executor:
        futures = [executor.submit(fetch_host_processes, host) for host in hosts]
        results = {}
        for future in as_completed(futures):
            host, procs = future.result()
            results[host] = procs
    return jsonify(results)


@blueprint.route("/api/hosts/process", methods=["POST"])
def create_host_process():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    process_type = data.get("type")
    host = data.get("host")
    action = data.get("action", "inject")
    force = bool(data.get("force", False))
    target_data = data.get("target")

    if not process_type:
        return jsonify({"status": "error", "message": "Missing type field"}), 400
    if not host and not target_data:
        return jsonify({"status": "error", "message": "Missing host or target field"}), 400

    if process_type not in NEMESIS_TYPES:
        return jsonify({"status": "error", "message": "Invalid process type"}), 400

    try:
        chaos_target = ChaosTarget.from_host_or_dict(host, target_data)
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    # Expand a bare host into a concrete entity when the type needs one.
    if target_data is None and cluster_inventory is not None:
        kind = target_kind_for(process_type)
        if kind is not TargetKind.HOST:
            matching = [t for t in cluster_inventory.entities(kind) if t.host == chaos_target.host]
            if len(matching) == 1:
                chaos_target = matching[0]
            elif len(matching) > 1:
                return jsonify(
                    {
                        "status": "error",
                        "message": (
                            f"Host {chaos_target.host} has {len(matching)} {kind.value} "
                            f"entities; pass an explicit target (node_id / slot_idx / …)."
                        ),
                    }
                ), 400
            elif kind is TargetKind.TABLET:
                control = cluster_inventory.control_host()
                if control:
                    chaos_target = ChaosTarget.for_tablet(control)

    if chaos_target.host not in hosts:
        return jsonify({"status": "error", "message": "Invalid host"}), 400
    host = chaos_target.host

    if nemesis_schedule is None:
        return jsonify({"status": "error", "message": "Schedule not initialized"}), 500

    if nemesis_schedule.is_schedule_enabled(process_type):
        return jsonify(
            {
                "status": "error",
                "message": f"Cannot manually run {process_type}: it is currently scheduled. Disable scheduling first.",
            }
        ), 400

    try:
        if chaos_store is None:
            return jsonify({"status": "error", "message": "Chaos store not initialized"}), 500
        # Plan-time safety for a FULL-mode manual inject; force=true skips it.
        if (
            not force
            and action == "inject"
            and failure_guard is not None
            and guard_mode_for(process_type) is GuardMode.FULL
        ):
            scope = impact_scope_for(process_type)
            safe = failure_guard.filter_safe([chaos_target], scope)
            if not safe:
                return jsonify(
                    {
                        "status": "error",
                        "message": (
                            f"Rejected by failure model: injecting {process_type} on "
                            f"{chaos_target.identity_key()} would exceed the cluster's fault "
                            f"tolerance (kind={target_kind_for(process_type).value}). "
                            f"Retry with force=true to override."
                        ),
                    }
                ), 409

        cmds = chaos_store.plan_manual(process_type, host, action)
        if not cmds:
            return jsonify(
                {"status": "error", "message": "Could not plan manual execution for this type/action"}
            ), 400
        # Prefer the request's ChaosTarget over whatever the planner picked.
        cmds = [
            type(c)(
                execution_id=c.execution_id,
                scenario_id=c.scenario_id,
                nemesis_type=c.nemesis_type,
                action=c.action,
                target=chaos_target,
                payload=c.payload,
            )
            for c in cmds
        ]
        record_scope = impact_scope_for(process_type) if failure_guard is not None else None
        for cmd in cmds:
            full = failure_guard is not None and guard_mode_for(process_type) is GuardMode.FULL
            baseline = None
            if (
                full and cmd.action == "inject" and recovery_probe is not None
                and needs_baseline(target_kind_for(process_type), record_scope)
            ):
                baseline = recovery_probe.alive_compute_baseline()  # before inject
                if baseline is None:
                    return jsonify(
                        {
                            "status": "error",
                            "message": (
                                f"Cannot inject {process_type}: no fresh healthcheck data "
                                f"for a slot baseline (recovery probe is blind)"
                            ),
                        }
                    ), 503
            if not nemesis_schedule.dispatch_command(cmd, track_history=False):
                return jsonify(
                    {"status": "error", "message": f"dispatch failed for {process_type}"},
                ), 502
            if full:
                if cmd.action == "extract":
                    failure_guard.record_extract(
                        cmd.execution_id,
                        cmd.target,
                        record_scope,
                        nemesis_type=cmd.nemesis_type,
                        source="manual",
                    )
                    if recovery_probe is not None:
                        recovery_probe.untrack_identity(cmd.target.identity_key())
                    if nemesis_metrics is not None:
                        nemesis_metrics.fault_ended(
                            target=cmd.target,
                            nemesis_type=cmd.nemesis_type,
                            reason="extract",
                            lease_id=cmd.execution_id,
                            execution_id=cmd.execution_id,
                            source="manual",
                            guard_mode="full",
                        )
                elif cmd.action == "inject":
                    # Held until HC confirms (or a manual extract); never on a timer.
                    failure_guard.record_inject(
                        cmd.execution_id,
                        cmd.target,
                        record_scope,
                        nemesis_type=cmd.nemesis_type,
                        source="manual",
                    )
                    if recovery_probe is not None:
                        recovery_probe.track(
                            cmd.execution_id,
                            cmd.target,
                            cmd.nemesis_type,
                            recovered=hc_predicate_for(
                                cmd.target,
                                kind=target_kind_for(cmd.nemesis_type),
                                scope=record_scope,
                                inventory=cluster_inventory,
                                baseline=baseline,
                                nemesis_type=cmd.nemesis_type,
                            ),
                            stuck_timeout_sec=stuck_timeout_for(cmd.nemesis_type),
                        )
        return jsonify(
            {
                "status": "ok",
                "executions": [
                    {
                        "execution_id": c.execution_id,
                        "scenario_id": c.scenario_id,
                        "host": c.host,
                        "target": c.target.to_dict(),
                        "action": c.action,
                    }
                    for c in cmds
                ],
            }
        )
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@blueprint.route("/api/process_types", methods=["GET"])
def get_process_types():
    """Return process types with their descriptions"""
    return jsonify(nemesis_types_flat_for_api())


@blueprint.route("/api/process_types/grouped", methods=["GET"])
def get_process_types_grouped():
    """Return process types grouped by category with descriptions (from catalog NEMESIS_UI_GROUPS)."""
    return jsonify(nemesis_types_grouped_for_api())


@blueprint.route("/api/inventory", methods=["GET"])
def get_cluster_inventory():
    """Host/node/slot inventory used for planning."""
    if cluster_inventory is None:
        return jsonify(
            {
                "hosts": list(hosts),
                "nodes": [],
                "slots": [],
            }
        )
    return jsonify(
        {
            "hosts": cluster_inventory.hosts,
            "nodes": [
                {
                    "node_id": n.node_id,
                    "host": n.host,
                    "ic_port": n.ic_port,
                    "rack": n.rack,
                    "datacenter": n.datacenter,
                    "bridge_pile_id": n.bridge_pile_id,
                }
                for n in cluster_inventory.nodes.values()
            ],
            "slots": [
                {
                    "slot_idx": s.slot_idx,
                    "host": s.host,
                    "ic_port": s.ic_port,
                    "node_id": s.node_id,
                    "rack": s.rack,
                    "datacenter": s.datacenter,
                    "bridge_pile_id": s.bridge_pile_id,
                }
                for s in cluster_inventory.slots.values()
            ],
        }
    )


@blueprint.route("/api/hosts/health", methods=["GET"])
def get_hosts_health():
    aggregated_health = {}
    for host in hosts:
        try:
            if is_local_host(host):
                # Direct response for local host
                aggregated_health[host] = {"status": "ok"}
            else:
                resp = requests.get(agent_url(host, "/health"), timeout=5)
                aggregated_health[host] = resp.json()
        except Exception as e:
            aggregated_health[host] = {"status": "error", "message": str(e)}
    return jsonify(aggregated_health)


@blueprint.route("/api/schedule", methods=["POST"])
def set_schedule():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    process_type = data.get("type")
    enabled = data.get("enabled")
    interval = data.get("interval")
    params = data.get("params") or {}

    if not process_type:
        return jsonify({"status": "error", "message": "Missing type field"}), 400
    if enabled is None:
        return jsonify({"status": "error", "message": "Missing enabled field"}), 400

    if process_type not in NEMESIS_TYPES:
        return jsonify({"status": "error", "message": "Invalid process type"}), 400
    if not isinstance(params, dict):
        return jsonify({"status": "error", "message": "params must be an object"}), 400

    if enabled:
        # Rebuild planner with user-supplied params (if any) before starting the schedule.
        if chaos_store is not None and params:
            if not chaos_store.rebuild_planner(process_type, params):
                return jsonify(
                    {"status": "error", "message": "Failed to apply params to planner"}
                ), 400
        started = nemesis_schedule.enable_schedule(
            process_type,
            interval
        )
        if not started:
            return jsonify({"status": "ok", "message": "Already enabled"})
    else:
        with nemesis_schedule.lock:
            if nemesis_schedule.has_task(process_type):
                nemesis_schedule.mark_disabled_before_flush(process_type)
                nemesis_schedule.flush_disable_extracts(process_type)
                nemesis_schedule.remove_task_entry(process_type)

    return jsonify({"status": "ok"})


@blueprint.route("/api/schedule/all", methods=["POST"])
def set_schedule_all():
    """
    Enable or disable scheduled nemesis for all registered types at once.

    Body JSON: ``enabled`` (bool, required). When enabling: optional ``interval`` (int) — same
    interval for every type; if omitted, each type uses its catalog default from NEMESIS_TYPES.
    """
    if nemesis_schedule is None:
        return jsonify({"status": "error", "message": "Schedule not initialized"}), 500

    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    enabled = data.get("enabled")
    if enabled is None:
        return jsonify({"status": "error", "message": "Missing enabled field"}), 400
    if not isinstance(enabled, bool):
        return jsonify({"status": "error", "message": "enabled must be a boolean"}), 400

    interval = data.get("interval")
    if interval is not None and not isinstance(interval, int):
        return jsonify({"status": "error", "message": "interval must be an integer or omitted"}), 400

    if enabled:
        results = nemesis_schedule.enable_all_schedules(uniform_interval=interval)
        return jsonify({"status": "ok", "results": results})
    stopped = nemesis_schedule.disable_all_schedules()
    return jsonify({"status": "ok", "stopped": stopped})


@blueprint.route("/api/schedule", methods=["GET"])
def get_schedule():
    """Return schedule status with intervals"""
    return jsonify(
        nemesis_schedule.schedule_status_for_types(NEMESIS_TYPES.keys())
    )


@blueprint.route("/api/schedule/history", methods=["GET"])
def get_schedule_history():
    """Return last scheduled executions"""
    return jsonify(nemesis_schedule.recent_history(15))


def _scheduler_state() -> dict:
    """Scheduler status plus the failure-budget snapshot."""
    if nemesis_scheduler is None:
        return {"available": False}
    state = {"available": True, **nemesis_scheduler.status()}
    if failure_guard is not None:
        state["failure_budget"] = failure_guard.snapshot()
    return state


@blueprint.route("/api/scheduler", methods=["GET"])
def get_scheduler():
    """Nemesis scheduler status (running, profile, budget, recovery probe)."""
    return jsonify(_scheduler_state())


def _validated_profile(data: dict) -> tuple[dict, str | None]:
    """``(profile, error)`` from a request body. Malformed input is rejected, not silently ignored:
    an unknown type would simply never fire, and a bare string in ``enabled`` would be split into
    single-character "types"."""
    profile: dict = {}

    if "enabled" in data:
        enabled = data["enabled"]
        if isinstance(enabled, str) or not isinstance(enabled, (list, tuple)):
            return {}, "'enabled' must be a list of nemesis type names"
        names = [str(t) for t in enabled]
        unknown = [t for t in names if t not in NEMESIS_TYPES]
        if unknown:
            return {}, (
                f"unknown nemesis type(s): {', '.join(sorted(unknown))}. "
                f"See GET /api/process_types; default profile: {', '.join(default_enabled_types())}"
            )
        unsupported = [t for t in names if not supports_boundary_scheduler(t)]
        if unsupported:
            return {}, (
                f"type(s) not usable by the boundary scheduler: {', '.join(sorted(unsupported))}. "
                f"Their planners keep their own target state, so they would inject somewhere other "
                f"than the target the guard reserved. Run them through the per-type schedule instead."
            )
        profile["enabled"] = names

    for key, lo, hi in (("base_interval", 0.5, 86400.0), ("jitter", 0.0, 1.0)):
        if key in data:
            try:
                value = float(data[key])
            except (TypeError, ValueError):
                return {}, f"'{key}' must be a number"
            if not lo <= value <= hi:
                return {}, f"'{key}' must be between {lo} and {hi}"
            profile[key] = value

    for key in ("max_per_tick", "max_bypass_per_tick"):
        if key in data:
            try:
                cap = int(data[key])
            except (TypeError, ValueError):
                return {}, f"'{key}' must be an integer"
            if not 1 <= cap <= 100:
                return {}, f"'{key}' must be between 1 and 100"
            profile[key] = cap

    unknown_keys = sorted(
        set(data) - {"enabled", "base_interval", "jitter", "max_per_tick", "max_bypass_per_tick"}
    )
    if unknown_keys:
        return {}, f"unknown profile field(s): {', '.join(unknown_keys)}"

    return profile, None


@blueprint.route("/api/scheduler/start", methods=["POST"])
def start_scheduler():
    """Apply an optional profile and start the scheduler.

    Body (all optional): ``enabled``, ``base_interval``, ``jitter``, ``max_per_tick``,
    ``max_bypass_per_tick``. Omitted fields keep their current value; invalid ones are
    rejected with 400.
    """
    if nemesis_scheduler is None:
        return jsonify(
            {"status": "error", "message": "Nemesis scheduler not initialized (orchestrator startup did not complete)"}
        ), 500

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        return jsonify({"status": "error", "message": "Body must be a JSON object"}), 400

    profile, error = _validated_profile(data)
    if error is not None:
        return jsonify({"status": "error", "message": error}), 400
    try:
        if profile:
            nemesis_scheduler.set_profile(**profile)
        nemesis_scheduler.start()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"status": "ok", "scheduler": _scheduler_state()})


@blueprint.route("/api/scheduler/stop", methods=["POST"])
def stop_scheduler():
    """Stop the nemesis scheduler (the app-owned recovery probe keeps running)."""
    if nemesis_scheduler is None:
        return jsonify({"status": "ok", "message": "Scheduler not initialized"})
    nemesis_scheduler.stop()
    return jsonify({"status": "ok", "scheduler": _scheduler_state()})


@blueprint.route("/api/problems", methods=["GET"])
def get_chaos_problems():
    """Chaos-side problems for the stability test report: stuck faults, degraded inventory."""
    problems = chaos_problems.snapshot() if chaos_problems is not None else []
    payload = {
        "count": len(problems),
        "by_kind": chaos_problems.counts_by_kind() if chaos_problems is not None else {},
        "problems": problems,
        "guard_enabled": bool(failure_guard is not None and failure_guard.enabled),
        "scheduler_running": bool(nemesis_scheduler is not None and nemesis_scheduler.running()),
    }
    if failure_guard is not None:
        payload["failure_budget"] = failure_guard.snapshot()
    if chaos_problems is not None and chaos_problems.dropped:
        payload["dropped"] = chaos_problems.dropped
    return jsonify(payload)


@blueprint.route("/api/healthcheck", methods=["GET"])
def get_healthcheck():
    rep = healthcheck_reporter
    if rep:
        return jsonify(rep.last_results)
    return jsonify({})


@blueprint.route("/api/hosts/warden/start", methods=["POST"])
def start_warden_checks_on_all_hosts():
    """
    Start warden checks:
    - Liveness checks run centrally on orchestrator (HTTP monitoring)
    - Safety checks run on each agent (local log/dmesg access)
    """
    logger.info(f"Starting warden checks on all hosts. Total hosts: {len(hosts)}")
    results = {"agents": {}, "orchestrator": {}}

    # Start safety checks on all agents
    def start_safety_on_host(host):
        try:
            logger.debug(f"Starting safety checks on agent: {host}")
            if is_local_host(host):
                # Direct call to avoid HTTP deadlock
                result = agent_router.start_warden_checks_helper()
                logger.debug(f"Agent {host} (local): {result.get('status', 'unknown')}")
                return host, result
            else:
                resp = requests.post(agent_url(host, "/api/warden/start"), timeout=10)
                result = resp.json()
                logger.debug(f"Agent {host} (remote): {result.get('status', 'unknown')}")
                return host, result
        except Exception as e:
            logger.error(f"Failed to start safety checks on agent {host}: {e}")
            return host, {"status": "error", "message": str(e)}

    # Use ThreadPoolExecutor to run tasks in parallel (since start_warden_checks_helper is now sync)
    with ThreadPoolExecutor() as executor:
        executor.map(start_safety_on_host, hosts)

    logger.info("Agent safety checks initiated")

    # Start orchestrator checks (liveness + orchestrator safety)
    logger.info("Starting orchestrator warden checks (liveness + PDisk + aggregated)")

    orchestrator_started = orchestrator_warden_checker.start_checks()
    results["orchestrator"] = {
        "status": "started" if orchestrator_started else "already_running",
        "type": "liveness"
    }
    logger.info(f"Orchestrator checks: {'started' if orchestrator_started else 'already running'}")

    return jsonify({"status": "ok", "results": results})


@blueprint.route("/api/hosts/warden/results", methods=["GET"])
def get_warden_results_from_all_hosts():
    """
    Get combined warden check results:
    - Liveness checks from orchestrator
    - Safety checks from each agent
    - Aggregated safety checks from orchestrator (e.g., UnifiedAgentVerifyFailedAggregated)
    """
    logger.debug("Fetching warden results from all hosts")

    # Get orchestrator results (liveness + safety including aggregated checks)
    orchestrator_result = orchestrator_warden_checker.get_last_result()
    logger.debug(f"Orchestrator status: {orchestrator_result.get('status', 'unknown')}")

    # Get safety results from all agents
    agent_results = {}

    def get_safety_from_host(host):
        try:
            if is_local_host(host):
                # Direct call to avoid HTTP deadlock
                result = agent_router.get_warden_result_helper()
                logger.debug(f"Agent {host} (local): status={result.get('status', 'unknown')}, checks={len(result.get('safety_checks', []))}")
                return host, result
            else:
                resp = requests.get(agent_url(host, "/api/warden/result"), timeout=10)
                return host, resp.json()
        except Exception as e:
            logger.error(f"Failed to get warden result from {host}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return host, {"status": "error", "error_message": str(e)}

    if hosts:
        with ThreadPoolExecutor(max_workers=min(len(hosts), 10)) as executor:
            futures = [executor.submit(get_safety_from_host, host) for host in hosts]
            for future in as_completed(futures):
                host, result = future.result()
                agent_results[host] = result

    # Log summary of agent statuses
    status_summary = {}
    for host, result in agent_results.items():
        status = result.get("status", "unknown")
        status_summary[status] = status_summary.get(status, 0) + 1
    logger.debug(f"Agent results summary: {status_summary}")

    # Combine results: orchestrator liveness + agent safety per host
    combined_results = {}

    # Add orchestrator as a special entry with liveness checks and safety checks
    combined_results["_orchestrator"] = {
        "status": orchestrator_result.get("status", "idle"),
        "started_at": orchestrator_result.get("started_at"),
        "completed_at": orchestrator_result.get("completed_at"),
        "liveness_checks": orchestrator_result.get("liveness_checks", []),
        "safety_checks": orchestrator_result.get("safety_checks", []),  # PDisk checks + aggregated checks
        "error_message": orchestrator_result.get("error_message")
    }

    # Add agent results (safety checks only)
    for host, result in agent_results.items():
        combined_results[host] = {
            "status": result.get("status", "idle"),
            "started_at": result.get("started_at"),
            "completed_at": result.get("completed_at"),
            "liveness_checks": [],  # Agents don't run liveness checks
            "safety_checks": result.get("safety_checks", []),
            "error_message": result.get("error_message")
        }

    return jsonify(combined_results)
