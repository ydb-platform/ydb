import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Blueprint, request, jsonify

from ydb.tests.stability.nemesis.internal.nemesis.catalog import PROCESS_TYPES
from ydb.tests.stability.nemesis.internal.orchestrator_warden_checker import (
    OrchestratorWardenChecker,
    get_all_warden_definitions,
)


logger = logging.getLogger(__name__)


blueprint = Blueprint('orchestrator', __name__)

# Module-level state
hosts = []
scheduled_tasks = {}
scheduled_executions_history = []  # List of {type, action, host, timestamp}
mon_port = 8765  # Default monitoring port
orchestrator_warden_checker: OrchestratorWardenChecker = None  # initialized in app.py
scheduled_tasks_lock = threading.Lock()


def get_app_port() -> int:
    """Get the configured app port from settings"""
    from ydb.tests.stability.nemesis.internal.config import Settings
    return Settings().app_port


def is_local_host(host: str) -> bool:
    """Check if the host is the local machine"""
    try:
        # Get configured app_host from settings
        from ydb.tests.stability.nemesis.internal.config import Settings
        settings = Settings()
        app_host = settings.app_host
        return host in ('localhost', '127.0.0.1', '::1', app_host)
    except Exception:
        return False


def run_process_on_host(host, process_type, action='run', track_history=False):
    """Run process on host, using direct call if it's the local host to avoid deadlock"""
    try:
        # Check if this is a call to ourselves
        if is_local_host(host):
            # Direct call to avoid HTTP deadlock with single worker
            from ydb.tests.stability.nemesis.routers.agent_router import create_process_helper

            # Call the helper function directly
            result = create_process_helper(process_type, action)
            print(f"Started process {process_type} locally with action {action}: {result}")
        else:
            # Remote call via HTTP
            port = get_app_port()
            requests.post(f"http://{host}:{port}/api/processes", json={'type': process_type, 'action': action}, timeout=5)

        # Track in history if requested (for scheduled executions)
        if track_history:
            from datetime import datetime
            with scheduled_tasks_lock:
                scheduled_executions_history.append({
                    "type": process_type,
                    "action": action,
                    "host": host,
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                })
                # Keep only last 50 entries
                if len(scheduled_executions_history) > 50:
                    scheduled_executions_history.pop(0)

    except Exception as e:
        print(f"Failed to start process {process_type} on {host}: {e}")


def schedule_process(process_type: str, interval: int = None):
    while True:
        with scheduled_tasks_lock:
            if process_type not in scheduled_tasks or not scheduled_tasks[process_type]['enabled']:
                if not scheduled_tasks[process_type]['enabled']:
                    target_hosts = PROCESS_TYPES[process_type]['runner'].affected_hosts
                    logger.info(f"Extracting {process_type} from {target_hosts}")
                    if not target_hosts or len(target_hosts) == 0:
                        break
                    with ThreadPoolExecutor(max_workers=min(len(target_hosts), 10)) as executor:
                        futures = [
                            executor.submit(run_process_on_host, host, process_type, "extract", True)
                            for host in target_hosts
                        ]
                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                print(f"Error in scheduled task: {e}")
                break

        # Get config for this process type
        process_def = PROCESS_TYPES[process_type]

        if 'runner' in process_def:
            runner = process_def['runner']
            # Delegate logic to the runner
            action, target_hosts = runner.prepare_fault(hosts)

            if action and target_hosts:
                # Run processes on hosts in parallel using thread pool
                logger.info(f"Running {action} of {process_type} into {target_hosts}")
                with ThreadPoolExecutor(max_workers=min(len(target_hosts), 10)) as executor:
                    futures = [
                        executor.submit(run_process_on_host, host, process_type, action, True)
                        for host in target_hosts
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"Error in scheduled task: {e}")

        time.sleep(interval)


@blueprint.route("/api/hosts/<host>/processes", methods=["GET"])
def get_all_host_processes(host: str):
    if is_local_host(host):
        # Direct call to avoid HTTP deadlock
        from ydb.tests.stability.nemesis.routers.agent_router import get_all_processes_helper
        return jsonify(get_all_processes_helper())
    else:
        port = get_app_port()
        resp = requests.get(f"http://{host}:{port}/api/processes", timeout=5)
        return jsonify(resp.json())


def fetch_host_processes(host):
    try:
        if is_local_host(host):
            # Direct call to avoid HTTP deadlock
            from ydb.tests.stability.nemesis.routers.agent_router import get_all_processes_helper
            return host, get_all_processes_helper()
        else:
            port = get_app_port()
            resp = requests.get(f"http://{host}:{port}/api/processes", timeout=5)
            return host, resp.json()
    except Exception as e:
        print(f"Failed to fetch processes from {host}: {e}")
        return host, []


@blueprint.route("/api/hosts/processes", methods=["GET"])
def get_all_processes():
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

    if not process_type:
        return jsonify({"status": "error", "message": "Missing type field"}), 400
    if not host:
        return jsonify({"status": "error", "message": "Missing host field"}), 400

    if process_type not in PROCESS_TYPES:
        return jsonify({"status": "error", "message": "Invalid process type"}), 400
    if host not in hosts:
        return jsonify({"status": "error", "message": "Invalid host"}), 400

    # Check if this nemesis type is currently scheduled
    with scheduled_tasks_lock:
        if process_type in scheduled_tasks and scheduled_tasks[process_type].get('enabled', False):
            return jsonify({"status": "error", "message": f"Cannot manually run {process_type}: it is currently scheduled. Disable scheduling first."}), 400

    try:
        if is_local_host(host):
            # Direct call to avoid HTTP deadlock
            from ydb.tests.stability.nemesis.routers.agent_router import create_process_helper
            result = create_process_helper(process_type, action)
            return jsonify(result)
        else:
            port = get_app_port()
            requests.post(f"http://{host}:{port}/api/processes", json={'type': process_type, 'action': action}, timeout=5)
            return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# Nemesis group definitions
NEMESIS_GROUPS = {
    "TabletNemesis": {
        "description": "Kill tablet processes via gRPC",
        "patterns": ["KillCoordinator", "KillMediator", "KillDataShard", "KillHive",
                     "KillBsController", "KillSchemeShard", "KillPersQueue", "KillKeyValue",
                     "KillTxAllocator", "KillNodeBroker", "KillTenantSlotBroker",
                     "KillBlockstoreVolume", "KillBlockstorePartition"]
    },
    "NodeNemesis": {
        "description": "Node and process management",
        "patterns": ["NodeKiller", "KillSlot", "Suspend", "Restart", "StopStart"]
    },
    "NetworkNemesis": {
        "description": "Network fault injection",
        "patterns": ["Network"]
    },
    "HiveNemesis": {
        "description": "Hive tablet management operations",
        "patterns": ["ReBalance", "ChangeTabletGroup", "BulkChangeTabletGroup"]
    },
    "TestNemesis": {
        "description": "Test and debug nemesis",
        "patterns": ["Test", "Throwing", "Shell"]
    }
}


def get_nemesis_group(nemesis_name: str) -> str:
    """Determine which group a nemesis belongs to based on its name."""
    for group_name, group_info in NEMESIS_GROUPS.items():
        for pattern in group_info["patterns"]:
            if pattern in nemesis_name:
                return group_name
    return "Other"


@blueprint.route("/api/process_types", methods=["GET"])
def get_process_types():
    """Return process types with their descriptions"""
    result = []
    for name, definition in PROCESS_TYPES.items():
        runner = definition.get('runner')
        description = runner.nemesis_description if runner and hasattr(runner, 'nemesis_description') else ""
        result.append({
            "name": name,
            "description": description
        })
    return jsonify(result)


@blueprint.route("/api/process_types/grouped", methods=["GET"])
def get_process_types_grouped():
    """Return process types grouped by category with descriptions"""
    groups = {}

    # Initialize groups
    for group_name, group_info in NEMESIS_GROUPS.items():
        groups[group_name] = {
            "description": group_info["description"],
            "nemesis": []
        }
    groups["Other"] = {
        "description": "Other nemesis types",
        "nemesis": []
    }

    # Categorize each nemesis
    for name, definition in PROCESS_TYPES.items():
        runner = definition.get('runner')
        description = runner.nemesis_description if runner and hasattr(runner, 'nemesis_description') else ""
        group = get_nemesis_group(name)

        groups[group]["nemesis"].append({
            "name": name,
            "description": description
        })

    # Remove empty groups
    groups = {k: v for k, v in groups.items() if v["nemesis"]}

    return jsonify(groups)


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

    if not process_type:
        return jsonify({"status": "error", "message": "Missing type field"}), 400
    if enabled is None:
        return jsonify({"status": "error", "message": "Missing enabled field"}), 400

    if process_type not in PROCESS_TYPES:
        return jsonify({"status": "error", "message": "Invalid process type"}), 400

    with scheduled_tasks_lock:
        if enabled:
            if process_type in scheduled_tasks and scheduled_tasks[process_type]['enabled']:
                return jsonify({"status": "ok", "message": "Already enabled"})

            scheduled_tasks[process_type] = {
                'enabled': True,
                'interval': interval
            }
            # Start scheduling in a daemon thread
            thread = threading.Thread(
                target=schedule_process,
                args=(process_type, interval)
            )
            thread.daemon = True
            thread.start()
            scheduled_tasks[process_type]['thread'] = thread
        else:
            if process_type in scheduled_tasks:
                scheduled_tasks[process_type]['enabled'] = False
                if 'thread' in scheduled_tasks[process_type]:
                    # Thread will exit on next iteration due to enabled=False
                    pass
                del scheduled_tasks[process_type]

    return jsonify({"status": "ok"})


@blueprint.route("/api/schedule", methods=["GET"])
def get_schedule():
    """Return schedule status with intervals"""
    result = {}
    with scheduled_tasks_lock:
        for pt in PROCESS_TYPES:
            if pt in scheduled_tasks and scheduled_tasks[pt]['enabled']:
                result[pt] = {
                    "enabled": True,
                    "interval": scheduled_tasks[pt].get('interval')
                }
            else:
                result[pt] = {"enabled": False, "interval": None}
    return jsonify(result)


@blueprint.route("/api/schedule/history", methods=["GET"])
def get_schedule_history():
    """Return last scheduled executions"""
    with scheduled_tasks_lock:
        # Return last 15 in reverse order (newest first)
        return jsonify(scheduled_executions_history[-15:][::-1])


@blueprint.route("/api/healthcheck", methods=["GET"])
def get_healthcheck():
    # Import here to get the current healthcheck_reporter
    from ydb.tests.stability.nemesis.app import healthcheck_reporter

    if healthcheck_reporter:
        return jsonify(healthcheck_reporter.last_results)
    return jsonify({})


@blueprint.route("/api/hosts/warden/start", methods=["POST"])
def start_warden_checks_on_all_hosts():
    """
    Start warden checks:
    - Liveness checks run centrally on master (HTTP monitoring)
    - Safety checks run on each agent (local log/dmesg access)
    """
    logger.info(f"Starting warden checks on all hosts. Total hosts: {len(hosts)}")
    results = {"agents": {}, "master": {}}

    # 2. Start safety checks on all agents
    def start_safety_on_host(host):
        try:
            logger.debug(f"Starting safety checks on agent: {host}")
            if is_local_host(host):
                # Direct call to avoid HTTP deadlock
                from ydb.tests.stability.nemesis.routers.agent_router import start_warden_checks_helper
                result = start_warden_checks_helper()
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
        task_results = list(executor.map(start_safety_on_host, hosts))

    started_count = 0
    error_count = 0
    for host, result in task_results:
        results["agents"][host] = result
        if result.get("status") == "started":
            started_count += 1
        elif result.get("status") == "error":
            error_count += 1

    logger.info(f"Agent safety checks initiated: {started_count} started, {error_count} errors, {len(hosts) - started_count - error_count} already running")

    # 1. Start orchestrator checks (liveness + orchestrator safety)
    logger.info("Starting orchestrator warden checks (liveness + PDisk + aggregated)")

    # start_checks() is now synchronous - it submits to background event loop
    orchestrator_started = orchestrator_warden_checker.start_checks()
    results["master"] = {
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
                from ydb.tests.stability.nemesis.routers.agent_router import get_warden_result_helper
                result = get_warden_result_helper()
                logger.debug(f"Agent {host} (local): status={result.get('status', 'unknown')}, checks={len(result.get('safety_checks', []))}")
                return host, result
            else:
                port = get_app_port()
                resp = requests.get(f"http://{host}:{port}/api/warden/result", timeout=10)
                return host, resp.json()
        except Exception as e:
            logger.error(f"Failed to get warden result from {host}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return host, {"status": "error", "error_message": str(e)}

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


@blueprint.route("/api/warden/checks", methods=["GET"])
def get_all_available_warden_checks():
    """
    Get list of all available warden checks across the system.

    Returns checks from:
    - Agent safety wardens (run on each agent)
    - Orchestrator liveness wardens (run centrally)
    - Orchestrator safety wardens (run centrally via HTTP)
    """
    return jsonify(get_all_warden_definitions())
