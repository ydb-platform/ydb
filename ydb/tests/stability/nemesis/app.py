from functools import lru_cache
import json
import os

from flask import Flask, jsonify

from ydb.tests.library.stability.healthcheck.healthcheck_reporter import HealthCheckReporter
from ydb.tests.stability.nemesis_app.internal import config
from ydb.tests.stability.nemesis_app.internal.install import get_hosts_from_yaml
from ydb.tests.stability.nemesis_app.internal.agent_warden_checker import AgentWardenChecker
from ydb.tests.stability.nemesis_app.internal.orchestrator_warden_checker import OrchestratorWardenChecker


@lru_cache
def get_settings():
    settings = config.Settings()
    print(settings)
    return settings


# Global state for orchestrator mode
hosts = []
healthcheck_reporter = None
nemesis_config = {}
app_initialized = False


def load_nemesis_config():
    global nemesis_config
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'nemesis_config.json')
    try:
        with open(config_path, 'r') as f:
            nemesis_config = json.load(f)
    except FileNotFoundError:
        print(f"Config file not found at {config_path}, using defaults")
        nemesis_config = {}
    except Exception as e:
        print(f"Failed to load config: {e}")
        nemesis_config = {}
    return nemesis_config


def initialize_app():
    """Initialize application components (called on first request)"""
    global hosts, healthcheck_reporter, app_initialized
    if app_initialized:
        return

    settings = get_settings()

    # Initialize agent WardenChecker (always, for both agent and orchestrator modes)
    from ydb.tests.stability.nemesis_app.routers import agent_router
    agent_router.warden_checker = AgentWardenChecker()

    # Orchestrator-specific initialization
    if settings.nemesis_type != 'agent':
        # Load hosts from config (no installation here - that's done via 'install' command)
        hosts = get_hosts_from_yaml(settings.yaml_config_location)
        print(f"Loaded hosts: {hosts}")

        # Load config
        load_nemesis_config()

        # Start healthcheck reporter
        healthcheck_reporter = HealthCheckReporter(hosts, store_results=True)
        healthcheck_reporter.start_healthchecks()

        # Share state with orchestrator router
        from ydb.tests.stability.nemesis_app.routers import orchestrator_router
        orchestrator_router.hosts = hosts
        orchestrator_router.orchestrator_warden_checker = OrchestratorWardenChecker(hosts=hosts, mon_port=orchestrator_router.mon_port)

    app_initialized = True


def cleanup_app(exception=None):
    """Cleanup application resources"""
    global hosts, healthcheck_reporter
    settings = get_settings()

    # Orchestrator-specific cleanup
    if settings.nemesis_type != 'agent':
        if healthcheck_reporter:
            healthcheck_reporter.stop_healthchecks()

        # Stop all scheduled tasks
        from ydb.tests.stability.nemesis_app.routers import orchestrator_router
        for task_info in orchestrator_router.scheduled_tasks.values():
            if 'task' in task_info:
                task_info['enabled'] = False
                if 'thread' in task_info and task_info['thread'].is_alive():
                    # Thread will exit on next iteration due to enabled=False
                    pass


def create_app():
    settings = get_settings()

    # Configure static folder for orchestrator mode
    static_folder = None
    if settings.nemesis_type != 'agent':
        static_folder = settings.static_location
        print(f"Static files configured. Location: {settings.static_location}")
        print(f"Nemesis type: {settings.nemesis_type}")
    else:
        print(f"Static files NOT configured. Nemesis type: {settings.nemesis_type}")

    app = Flask(__name__, static_folder=static_folder, static_url_path='/static')

    # Register cleanup for application shutdown (not per-request teardown)
    import atexit
    atexit.register(cleanup_app)

    # Common health endpoint
    @app.route("/health", methods=["GET"])
    def get_health():
        return jsonify({"status": "ok"})

    # Always include agent router (available in both modes)
    from ydb.tests.stability.nemesis_app.routers.agent_router import blueprint as agent_blueprint
    app.register_blueprint(agent_blueprint)

    # Include routers based on configuration
    if settings.nemesis_type == 'agent':
        # Agent mode: only agent endpoints
        print("Running in AGENT mode")
    else:
        # Orchestrator mode: include orchestrator router
        from ydb.tests.stability.nemesis_app.routers.orchestrator_router import blueprint as orchestrator_blueprint
        app.register_blueprint(orchestrator_blueprint)
        print("Running in ORCHESTRATOR mode (with agent endpoints)")

    # Initialize on first request
    @app.before_request
    def ensure_initialized():
        initialize_app()

    return app


app = create_app()
