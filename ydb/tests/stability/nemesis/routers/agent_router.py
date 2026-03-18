import logging
import socket

from flask import Blueprint, request, jsonify

from ydb.tests.stability.nemesis.internal.nemesis.catalog import PROCESS_TYPES
from ydb.tests.stability.nemesis.internal.nemesis.runner import NemesisManager
from ydb.tests.stability.nemesis.internal.agent_warden_checker import AgentWardenChecker


logger = logging.getLogger(__name__)


manager = NemesisManager()
blueprint = Blueprint('agent', __name__)
warden_checker: AgentWardenChecker = None  # initialized in app.py


# Helper functions that can be called directly (without Flask request context)
def get_all_processes_helper():
    """Helper function to get all processes (can be called directly)"""
    return manager.get_all()


def create_process_helper(process_type: str, action: str = 'inject'):
    """Helper function to create a process (can be called directly)"""
    if process_type not in PROCESS_TYPES:
        return {"status": "error", "message": "Invalid process type"}

    process_def = PROCESS_TYPES[process_type]
    runner = process_def['runner']

    manager.start_process(process_type, runner, action)
    return {"status": "started"}


def start_warden_checks_helper():
    """Helper function to start warden checks (can be called directly)"""
    hostname = socket.gethostname()
    logger.info(f"[{hostname}] Agent warden checks start requested")

    # start_checks() is now synchronous - it submits to background event loop
    started = warden_checker.start_checks()

    if started:
        logger.info(f"[{hostname}] Agent warden checks started successfully")
        return {"status": "started"}
    else:
        logger.info(f"[{hostname}] Agent warden checks already running")
        return {"status": "already_running"}


def get_warden_result_helper():
    """Helper function to get warden result (can be called directly)"""
    hostname = socket.gethostname()
    result = warden_checker.get_last_result()
    status = result.get("status", "unknown")
    safety_count = len(result.get("safety_checks", []))
    logger.debug(f"[{hostname}] Agent warden result requested: status={status}, safety_checks={safety_count}")
    return result


# Flask route functions (call the helper functions)
@blueprint.route("/api/processes", methods=["GET"])
def get_all_processes():
    return jsonify(get_all_processes_helper())


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


@blueprint.route("/api/processes", methods=["POST"])
def create_process():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    process_type = data.get("type")
    if not process_type:
        return jsonify({"status": "error", "message": "Missing type field"}), 400

    action = data.get("action", "inject")

    result = create_process_helper(process_type, action)
    if result.get("status") == "error":
        return jsonify(result), 400
    return jsonify(result)


@blueprint.route("/api/warden/start", methods=["POST"])
def start_warden_checks():
    """Start warden checks."""
    return jsonify(start_warden_checks_helper())


@blueprint.route("/api/warden/result", methods=["GET"])
def get_warden_result():
    """Get the last warden check result."""
    return jsonify(get_warden_result_helper())
