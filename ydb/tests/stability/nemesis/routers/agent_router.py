from __future__ import annotations

import logging

from flask import Blueprint, request, jsonify

from ydb.tests.stability.nemesis.internal.models import ProcessInfo, WardenTimeWindow, parse_warden_time_window
from ydb.tests.stability.nemesis.internal.agent.agent_warden_checker import AgentWardenChecker
from ydb.tests.stability.nemesis.internal.agent.nemesis.service import AgentExecutionService


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


execution_service: AgentExecutionService | None = None  # initialized in app.py
blueprint = Blueprint('agent', __name__)
warden_checker: AgentWardenChecker = None  # initialized in app.py


# Compatibility helpers for the HTTP adapter; execution is owned by the app.
def get_all_processes_helper():
    return [ProcessInfo(**row).to_json() for row in execution_service.get_all()]


def create_process_helper(process_type: str, action: str = "inject", payload=None):
    return execution_service.start(process_type, action, payload=payload)


def wait_for_local_processes(timeout: float = 20.0, poll_interval: float = 0.2) -> int:
    if execution_service is None:
        return 0
    return execution_service.wait_for_processes(timeout, poll_interval)


def start_warden_checks_helper(time_window: WardenTimeWindow | None = None):
    """Helper function to start warden checks (can be called directly)"""
    window = time_window or WardenTimeWindow.from_hours_back(24)
    logger.info(
        "Agent warden checks start requested start_time=%s end_time=%s",
        window.start_ts,
        window.end_ts,
    )

    # start_checks() is now synchronous - it submits to background event loop
    started = warden_checker.start_checks(window)

    if started:
        logger.info("Agent warden checks started successfully")
        return {"status": "started", **window.to_json()}
    else:
        logger.info("Agent warden checks already running")
        return {"status": "already_running"}


def get_warden_result_helper():
    """Helper function to get warden result (can be called directly)"""
    result = warden_checker.get_last_result()
    logger.debug(f"Agent warden result requested: result={result}")
    return result


# Flask route functions (call the helper functions)
@blueprint.route("/api/processes", methods=["GET"])
def get_all_processes():
    return jsonify(get_all_processes_helper())


@blueprint.route("/api/processes", methods=["POST"])
def create_process():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    process_type = data.get("type")
    if not process_type:
        return jsonify({"status": "error", "message": "Missing type field"}), 400

    action = data.get("action", "inject")
    payload = data.get("payload")

    result = create_process_helper(
        process_type,
        action,
        payload=payload,
    )
    if result.get("status") == "error":
        return jsonify(result), 400
    return jsonify(result)


@blueprint.route("/api/warden/start", methods=["POST"])
def start_warden_checks():
    """Start warden checks.

    Optional JSON body:
    - ``start_time`` / ``end_time`` — unix timestamps
    - ``since`` / ``until`` — ISO-8601 datetimes
    - ``hours_back`` — relative window ending at now (default 24)
    """
    data = request.get_json(silent=True) or {}
    ok, error, window = parse_warden_time_window(data)
    if not ok:
        return jsonify({"status": "error", "message": error}), 400
    return jsonify(start_warden_checks_helper(window))


@blueprint.route("/api/warden/result", methods=["GET"])
def get_warden_result():
    """Get the last warden check result."""
    return jsonify(get_warden_result_helper())


@blueprint.route("/api/agent/drain", methods=["POST"])
def drain_agent():
    """Internal service lifecycle endpoint; test clients keep their old API."""
    if execution_service is None:
        return jsonify({"status": "ok"})
    result = execution_service.shutdown(timeout=60)
    return jsonify(result), (200 if result["status"] == "ok" else 503)


@blueprint.route("/api/agent/resume", methods=["POST"])
def resume_agent():
    """A new orchestrator can reopen a drained agent, but never one handling SIGTERM."""
    try:
        execution_service.resume()
        return jsonify({"status": "ok"})
    except RuntimeError as error:
        return jsonify({"status": "error", "message": str(error)}), 409
