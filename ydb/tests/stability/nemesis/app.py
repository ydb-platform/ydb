from ydb.tests.stability.nemesis.routers.orchestrator_router import blueprint as orchestrator_blueprint
from ydb.tests.stability.nemesis.routers.agent_router import blueprint as agent_blueprint
import ydb.tests.stability.nemesis.routers.orchestrator_router as orchestrator_router
import ydb.tests.stability.nemesis.routers.agent_router as agent_router
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_checker import OrchestratorWardenChecker
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import ChaosOrchestratorStore
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.boundary_scheduler import BoundaryNemesisScheduler
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.fault_engine import FaultEngine
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.runtime_topology import RuntimeNodePiles
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import RecoveryProbe
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelConfigError,
    FailureModelGuard,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_problems import (
    KIND_PROBE_BLIND,
    ChaosProblemStore,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.metrics import NemesisMetrics
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import cluster_yaml_path
from ydb.tests.stability.nemesis.internal.orchestrator.install import get_hosts_from_yaml
from ydb.tests.stability.nemesis.internal.orchestrator.agent_endpoints import resolve_agent_endpoints
from ydb.tests.stability.nemesis.internal.config import AgentSettings
from ydb.tests.stability.nemesis.internal.agent.agent_warden_checker import AgentWardenChecker
from ydb.tests.stability.nemesis.internal.agent.nemesis.runner import NemesisManager
from ydb.tests.stability.nemesis.internal.agent.nemesis.service import AgentExecutionService
from ydb.tests.stability.nemesis.internal.nemesis.catalog import NEMESIS_TYPES, supports_boundary_scheduler
from ydb.tests.stability.nemesis.internal.nemesis.command_transport import AgentCommandTransport
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.command_dispatcher import CommandDispatcher
from ydb.tests.stability.nemesis.internal import config
from ydb.tests.library.stability.healthcheck.healthcheck_reporter import HealthCheckReporter
from flask import Flask, current_app, jsonify, request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import requests
from ydb.tests.stability.nemesis.internal.shutdown import ShutdownCoordinator
from ydb.tests.stability.nemesis.internal.agent.nemesis.local_process import (
    load_process_specs, ProcessController, local_definitions,
)
import atexit
import logging
import sys
import threading
from functools import lru_cache

# --- Nemesis logging (must run before other ydb.tests.stability.nemesis imports) ---------------
#
# Setting logger.setLevel(DEBUG) on a module logger alone often shows nothing: the LogRecord
# still propagates toward the root logger; if the root (or an intermediate ancestor) has
# effective level WARNING, DEBUG records are dropped there. A handler must exist on an ancestor
# that accepts DEBUG. We attach one handler to the package logger and set propagate=False on it
# so Nemesis output does not reach root (no flood from urllib3 / requests / etc.).
#
_NEMESIS_LOGGER_ROOT = "ydb.tests.stability.nemesis"
_nemesis_stderr_handler_installed = False


def _ensure_nemesis_logging_emits_to_stderr() -> None:
    global _nemesis_stderr_handler_installed
    if _nemesis_stderr_handler_installed:
        return
    nem = logging.getLogger(_NEMESIS_LOGGER_ROOT)
    nem.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    nem.addHandler(handler)
    nem.propagate = False
    _nemesis_stderr_handler_installed = True


_ensure_nemesis_logging_emits_to_stderr()


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@lru_cache
def get_settings():
    settings = config.Settings()
    print(settings)
    return settings


# before_request overlaps on startup; without the lock each request builds its own checker.
_initialize_lock = threading.Lock()
_shutdown_coordinator = None


def initialize_app():
    """Initialize application components once, even if the first requests overlap."""
    if current_app.config.get("NEMESIS_INITIALIZED"):
        return
    with _initialize_lock:
        if current_app.config.get("NEMESIS_INITIALIZED"):
            return
        _initialize_app_body()
        current_app.config["NEMESIS_INITIALIZED"] = True


def _initialize_app_body():
    settings = get_settings()

    process_specs = load_process_specs(settings.local_process_config) if settings.local_process_config else None
    definitions = (local_definitions(NEMESIS_TYPES, ProcessController(process_specs))
                   if process_specs is not None else NEMESIS_TYPES)
    execution_service = AgentExecutionService(NemesisManager(), definitions)
    agent_router.execution_service = execution_service
    global _shutdown_coordinator
    _shutdown_coordinator = ShutdownCoordinator(
        _begin_shutdown, _finish_shutdown,
        report_path=Path(settings.install_root) / "shutdown.json", timeout=settings.shutdown_timeout_sec,
    )

    agent_router.warden_checker = AgentWardenChecker(
        log_directory=AgentSettings().kikimr_logs_directory,
    )

    if settings.nemesis_type != "agent":
        loaded_hosts = ([process_specs[0].host] if process_specs is not None
                        else get_hosts_from_yaml(settings.yaml_config_location))
        print(f"Loaded hosts: {loaded_hosts}")

        orchestrator_router.mon_port = settings.mon_port
        orchestrator_router.hosts = loaded_hosts
        # Resolve once at boot: agent HTTP must not depend on live DNS (DnsNemesis).
        orchestrator_router.host_endpoints = resolve_agent_endpoints(loaded_hosts)
        for host in loaded_hosts:
            if not orchestrator_router.is_local_host(host):
                try:
                    response = requests.post(orchestrator_router.agent_url(host, "/api/agent/resume"), timeout=5)
                    response.raise_for_status()
                except requests.RequestException:
                    logger.exception("Could not resume agent %s; injections there remain unavailable", host)
        orchestrator_router.healthcheck_reporter = HealthCheckReporter(loaded_hosts, store_results=True)
        orchestrator_router.healthcheck_reporter.start_healthchecks()

        # Surfaced to the stability test report via GET /api/problems.
        problems = ChaosProblemStore()
        orchestrator_router.chaos_problems = problems

        # Normally validated at startup by require_failure_model_or_die(); parsed here too for
        # embedded/test use. Either way the guard is live past this line.
        topology = current_app.config.get("NEMESIS_TOPOLOGY")
        if topology is None:
            topology = ClusterTopologyModel(cluster_yaml_path())
            current_app.config["NEMESIS_TOPOLOGY"] = topology
        inventory = ClusterInventory(topology, agent_hosts=loaded_hosts, process_specs=process_specs)
        metrics = NemesisMetrics()
        failure_guard = FailureModelGuard(
            topology, total_slots=len(inventory.slots), inventory=inventory, metrics=metrics
        )
        metrics.sync_budget_gauges(failure_guard.snapshot())
        logger.info("Failure model guard: %s", failure_guard.snapshot())
        orchestrator_router.failure_guard = failure_guard
        orchestrator_router.cluster_inventory = inventory
        orchestrator_router.nemesis_metrics = metrics

        # Synthesized targets are guesses and slot chaos is off — not fatal, but must be visible.
        if inventory.degraded_reason:
            problems.record_inventory_degraded(
                inventory.degraded_reason,
                {
                    "hosts": len(inventory.hosts),
                    "nodes": len(inventory.nodes),
                    "slots": len(inventory.slots),
                },
            )

        orchestrator_router.available_fault_types = {
            name for name in definitions if supports_boundary_scheduler(name)
        }
        orchestrator_router.chaos_store = ChaosOrchestratorStore(
            enabled_types=orchestrator_router.available_fault_types,
            failure_guard=failure_guard,
            inventory=inventory,
        )
        orchestrator_router.orchestrator_warden_checker = OrchestratorWardenChecker(
            hosts=loaded_hosts,
            mon_port=orchestrator_router.mon_port,
            fetch_agent_warden_result=orchestrator_router.fetch_agent_warden_result,
            get_monitored_hosts=lambda: orchestrator_router.hosts,
        )
        # App-owned probe: boundary and manual operations release through it.
        probe = RecoveryProbe(
            guard=failure_guard,
            hc_source=orchestrator_router.healthcheck_reporter,
            node_piles_source=(RuntimeNodePiles(
                [f"http://{orchestrator_router.agent_http_host(h)}:{orchestrator_router.mon_port}"
                 for h in loaded_hosts[:2]], topology.pile_names,
            ) if topology.pile_names else None),
            on_stuck=problems.record_stuck_fault,  # a never-recovering fault holds budget forever
            on_blind=problems.record_probe_blind,
            on_sighted=lambda: problems.resolve_kind(KIND_PROBE_BLIND),
            metrics=metrics,
        )
        orchestrator_router.recovery_probe = probe
        probe.start()

        dispatcher = CommandDispatcher(
            AgentCommandTransport(
                is_local_host=orchestrator_router.is_local_host,
                start_local=execution_service.start,
                get_app_port=orchestrator_router.get_app_port,
                resolve_http_host=orchestrator_router.agent_http_host,
            ),
            metrics=metrics,
        )
        orchestrator_router.command_dispatcher = dispatcher
        class LocalAdmission:
            def check(self, plan):
                if plan.nemesis_type not in definitions:
                    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.fault_engine import FaultRejected
                    raise FaultRejected("Fault is not supported in local process mode")

        engine = FaultEngine(
            guard=failure_guard, inventory=inventory,
            admission=LocalAdmission() if process_specs is not None else None,
            plan_inject=orchestrator_router.chaos_store.plan_inject_target,
            plan_extract=orchestrator_router.chaos_store.plan_extract_target,
            dispatch=lambda cmd: dispatcher.dispatch(cmd, track_history=True),
            recovery_probe=probe,
        )
        orchestrator_router.fault_engine = engine
        # Boundary scheduler, started on demand via /api/scheduler/start.
        orchestrator_router.nemesis_scheduler = BoundaryNemesisScheduler(
            engine=engine,
            enabled_types=list(definitions) if process_specs is not None else None,
            guard=failure_guard,
            inventory=inventory,
            plan_inject=orchestrator_router.chaos_store.plan_inject_target,
            plan_extract=orchestrator_router.chaos_store.plan_extract_target,
            dispatch=lambda cmd: dispatcher.dispatch(cmd, track_history=True),
            recovery_probe=probe,
        )



def _begin_shutdown():
    if orchestrator_router.fault_engine:
        orchestrator_router.fault_engine.begin_shutdown()
    if agent_router.execution_service:
        agent_router.execution_service.begin_shutdown()


def _finish_shutdown():
    errors = []
    if orchestrator_router.fault_engine and not orchestrator_router.fault_engine.quiesce(30):
        errors.append("Fault dispatch did not quiesce")
    if orchestrator_router.nemesis_scheduler:
        orchestrator_router.nemesis_scheduler.stop(drain=False)
    if orchestrator_router.recovery_probe:
        orchestrator_router.recovery_probe.stop()
    # Every agent owns its restore ledger. Wait for actual actions, not merely
    # HTTP acceptance of extract; an agent also drains independently on SIGTERM.
    hosts = [h for h in orchestrator_router.hosts if not orchestrator_router.is_local_host(h)]

    def drain(host):
        try:
            response = requests.post(orchestrator_router.agent_url(host, "/api/agent/drain"), timeout=65)
            response.raise_for_status()
            return host, response.json()
        except Exception as error:
            return host, {"status": "error", "message": str(error)}

    results = {}
    with ThreadPoolExecutor(max_workers=max(1, len(hosts) + 1)) as pool:
        remote = [pool.submit(drain, host) for host in hosts]
        local = pool.submit(agent_router.execution_service.shutdown, 60) if agent_router.execution_service else None
        for future in remote:
            host, result = future.result()
            results[host] = result
        if local:
            results["local"] = local.result()
    if orchestrator_router.healthcheck_reporter:
        orchestrator_router.healthcheck_reporter.stop_healthchecks()
    status = "error" if errors or any(r.get("status") != "ok" for r in results.values()) else "ok"
    report = {"status": status, "errors": errors, "agents": results}
    if status != "ok":
        logger.error("Incomplete Nemesis restore: %s", report)
    return report


def cleanup_app(exception=None):
    if _shutdown_coordinator is None:
        return {"status": "ok"}
    return _shutdown_coordinator.shutdown()


def require_failure_model_or_die(flask_app) -> None:
    """Parse ``cluster.yaml`` before serving, or exit: chaos without a fault-tolerance ceiling is
    worse than no chaos. Called from the ``run`` entry point so importing this module stays free of
    side effects; agents plan nothing and need no model."""
    if get_settings().nemesis_type == "agent":
        return
    try:
        flask_app.config["NEMESIS_TOPOLOGY"] = ClusterTopologyModel(cluster_yaml_path())
    except FailureModelConfigError as e:
        logger.critical("Refusing to start the nemesis orchestrator: %s", e)
        raise SystemExit(2) from e


def create_app():
    settings = get_settings()

    # Configure static folder for orchestrator mode
    static_folder = None
    if settings.nemesis_type != "agent":
        static_folder = settings.static_location
        print(f"Static files configured. Location: {settings.static_location}")
        print(f"Nemesis type: {settings.nemesis_type}")
    else:
        print(f"Static files NOT configured. Nemesis type: {settings.nemesis_type}")

    app = Flask(__name__, static_folder=static_folder, static_url_path="/static")
    app.logger.setLevel(logging.DEBUG)

    atexit.register(cleanup_app)

    @app.route("/health", methods=["GET"])
    def get_health():
        return jsonify({"status": "ok"})

    app.register_blueprint(agent_blueprint)

    if settings.nemesis_type == "agent":
        print("Running in AGENT mode")
    else:
        app.register_blueprint(orchestrator_blueprint)
        print("Running in ORCHESTRATOR mode (with agent endpoints)")

    @app.before_request
    def ensure_initialized():
        if app.config.get("NEMESIS_STOPPING"):
            data = request.get_json(silent=True) or {}
            if request.method == "POST" and request.path != "/api/agent/drain" and not request.path.endswith("/restore") and data.get("action") != "extract":
                return jsonify({"status": "error", "message": "Nemesis is stopping"}), 503
        initialize_app()

    return app


app = create_app()
