from functools import lru_cache

from flask import Flask, jsonify

from ydb.tests.library.stability.healthcheck.healthcheck_reporter import HealthCheckReporter
from ydb.tests.stability.nemesis.internal import config
from ydb.tests.stability.nemesis.internal.install import get_hosts_from_yaml
from ydb.tests.stability.nemesis.internal.agent_warden_checker import AgentWardenChecker
from ydb.tests.stability.nemesis.internal.orchestrator_warden_checker import OrchestratorWardenChecker


@lru_cache
def get_settings():
    settings = config.Settings()
    print(settings)
    return settings


# Global state for orchestrator mode
hosts = []
healthcheck_reporter = None
app_initialized = False


def initialize_app():
    """Initialize application components (called on first request)"""
    global hosts, healthcheck_reporter, app_initialized
    if app_initialized:
        return

    settings = get_settings()

    from ydb.tests.stability.nemesis.routers import agent_router
    agent_router.warden_checker = AgentWardenChecker()

    if settings.nemesis_type != 'agent':
        hosts = get_hosts_from_yaml(settings.yaml_config_location)
        print(f"Loaded hosts: {hosts}")

        # Start healthcheck reporter
        healthcheck_reporter = HealthCheckReporter(hosts, store_results=True)
        healthcheck_reporter.start_healthchecks()

        # Share state with orchestrator router
        from ydb.tests.stability.nemesis.routers import orchestrator_router
        orchestrator_router.hosts = hosts
        orchestrator_router.orchestrator_warden_checker = OrchestratorWardenChecker(hosts=hosts, mon_port=orchestrator_router.mon_port)

    app_initialized = True


def cleanup_app(exception=None):
    """Cleanup application resources"""
    settings = get_settings()

    if settings.nemesis_type != 'agent':
        healthcheck_reporter.stop_healthchecks()

        from ydb.tests.stability.nemesis.routers import orchestrator_router
        for task_info in orchestrator_router.scheduled_tasks.values():
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
    from ydb.tests.stability.nemesis.routers.agent_router import blueprint as agent_blueprint
    app.register_blueprint(agent_blueprint)

    # Include routers based on configuration
    if settings.nemesis_type == 'agent':
        # Agent mode: only agent endpoints
        print("Running in AGENT mode")
    else:
        # Orchestrator mode: include orchestrator router
        from ydb.tests.stability.nemesis.routers.orchestrator_router import blueprint as orchestrator_blueprint
        app.register_blueprint(orchestrator_blueprint)
        print("Running in ORCHESTRATOR mode (with agent endpoints)")

    # Initialize on first request
    @app.before_request
    def ensure_initialized():
        initialize_app()

    return app


app = create_app()
