from ydb.tests.stability.nemesis.routers.orchestrator_router import blueprint as orchestrator_blueprint
from ydb.tests.stability.nemesis.routers.agent_router import blueprint as agent_blueprint
import ydb.tests.stability.nemesis.routers.orchestrator_router as orchestrator_router
import ydb.tests.stability.nemesis.routers.agent_router as agent_router
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_checker import OrchestratorWardenChecker
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.schedule_loop import OrchestratorNemesisSchedule
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import ChaosOrchestratorStore
from ydb.tests.stability.nemesis.internal.orchestrator.install import get_hosts_from_yaml
from ydb.tests.stability.nemesis.internal.config import AgentSettings
from ydb.tests.stability.nemesis.internal.agent.agent_warden_checker import AgentWardenChecker
from ydb.tests.stability.nemesis.internal import config
from ydb.tests.library.stability.healthcheck.healthcheck_reporter import HealthCheckReporter
from flask import Flask, current_app, jsonify
import atexit
import logging
import sys
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


def initialize_app():
    """Initialize application components (called on first request)."""
    if current_app.config.get("NEMESIS_INITIALIZED"):
        return

    settings = get_settings()

    agent_router.warden_checker = AgentWardenChecker(
        log_directory=AgentSettings().kikimr_logs_directory,
    )

    if settings.nemesis_type != "agent":
        loaded_hosts = get_hosts_from_yaml(settings.yaml_config_location)
        print(f"Loaded hosts: {loaded_hosts}")

        orchestrator_router.hosts = loaded_hosts
        orchestrator_router.healthcheck_reporter = HealthCheckReporter(loaded_hosts, store_results=True)
        orchestrator_router.healthcheck_reporter.start_healthchecks()

        orchestrator_router.chaos_store = ChaosOrchestratorStore()
        orchestrator_router.orchestrator_warden_checker = OrchestratorWardenChecker(
            hosts=loaded_hosts,
            mon_port=orchestrator_router.mon_port,
            fetch_agent_warden_result=orchestrator_router.fetch_agent_warden_result,
            get_monitored_hosts=lambda: orchestrator_router.hosts,
        )
        orchestrator_router.nemesis_schedule = OrchestratorNemesisSchedule(
            chaos_store=orchestrator_router.chaos_store,
            get_hosts=lambda: orchestrator_router.hosts,
            is_local_host=orchestrator_router.is_local_host,
            get_app_port=orchestrator_router.get_app_port,
        )

    current_app.config["NEMESIS_INITIALIZED"] = True


def cleanup_app(exception=None):
    """Cleanup application resources"""
    settings = get_settings()

    if settings.nemesis_type != "agent":
        rep = orchestrator_router.healthcheck_reporter
        if rep:
            rep.stop_healthchecks()

        if orchestrator_router.nemesis_schedule:
            orchestrator_router.nemesis_schedule.shutdown_disable_all()


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
        initialize_app()

    return app


app = create_app()
