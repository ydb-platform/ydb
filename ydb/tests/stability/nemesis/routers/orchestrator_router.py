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
    recovery_sec_for,
    target_kind_for,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import FailureModelGuard, GuardMode
import ydb.tests.stability.nemesis.routers.agent_router as agent_router


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


blueprint = Blueprint('orchestrator', __name__)

# Module-level state (orchestrator wiring; see app.initialize_app)
hosts: list[str] = []
mon_port = 8765  # Default monitoring port
orchestrator_warden_checker: OrchestratorWardenChecker | None = None
nemesis_schedule: OrchestratorNemesisSchedule | None = None
chaos_store: ChaosOrchestratorStore | None = None
failure_guard: FailureModelGuard | None = None
cluster_inventory: ClusterInventory | None = None
healthcheck_reporter: Any = None


def get_app_port() -> int:
    """Get the configured app port from settings"""
    return Settings().app_port


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
        port = get_app_port()
        resp = requests.get(f"http://{host}:{port}/api/warden/result", timeout=10)
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
        port = get_app_port()
        resp = requests.get(f"http://{host}:{port}/api/processes", timeout=5)
        return jsonify(resp.json())


def fetch_host_processes(host):
    try:
        if is_local_host(host):
            # Direct call to avoid HTTP deadlock
            return host, agent_router.get_all_processes_helper()
        else:
            port = get_app_port()
            resp = requests.get(f"http://{host}:{port}/api/processes", timeout=5)
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

    # Expand bare host → concrete entity via inventory when target_kind needs it.
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
        # Plan-time safety for FULL-mode manual inject (no dispatch veto).
        # force=True skips the filter check.
        if (
            not force
            and action == "inject"
            and failure_guard is not None
            and failure_guard.enabled
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
        # Prefer the explicit ChaosTarget from the request on planned commands.
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
            nemesis_schedule.dispatch_command(cmd, track_history=False)
            if (
                failure_guard is not None
                and failure_guard.enabled
                and guard_mode_for(process_type) is GuardMode.FULL
            ):
                if cmd.action == "extract":
                    failure_guard.record_extract(cmd.execution_id, cmd.target, record_scope)
                elif cmd.action == "inject":
                    failure_guard.record_inject(
                        cmd.execution_id,
                        cmd.target,
                        record_scope,
                        recovery_sec=recovery_sec_for(cmd.nemesis_type),
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
    """Return host/node/slot inventory used for ChaosTarget planning."""
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
                port = get_app_port()
                resp = requests.get(f"http://{host}:{port}/health", timeout=5)
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
                port = get_app_port()
                resp = requests.post(f"http://{host}:{port}/api/warden/start", timeout=10)
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
                port = get_app_port()
                resp = requests.get(f"http://{host}:{port}/api/warden/result", timeout=10)
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
