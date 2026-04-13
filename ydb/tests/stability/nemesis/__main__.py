import argparse
import json
import logging
import sys

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.stability.nemesis.internal.config import Settings, get_orchestrator_settings
from ydb.tests.stability.nemesis.internal.orchestrator.install import get_hosts_from_yaml, install_on_hosts, stop_agent_services
from ydb.tests.tools.nemesis.library import monitor
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_execution import run_orchestrator_liveness_cli_batch


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Nemesis App - Stability testing application",
        allow_abbrev=False
    )

    # Positional command argument
    parser.add_argument(
        'command',
        nargs='?',
        choices=['run', 'stop', 'liveness', 'install'],
        help='Command to run: install (install and run nemesis services), stop (stop nemesis services), liveness (run liveness checks), run (run agent)'
    )

    # Optional settings arguments (override env and defaults)
    parser.add_argument('--nemesis-type', choices=['orchestrator', 'agent'],
                        help='Type of nemesis: orchestrator or agent')
    parser.add_argument('--app-host', help='Host to bind the application to')
    parser.add_argument('--app-port', type=int, help='Port to bind the application to')
    parser.add_argument('--yaml-config-location', help='Path to cluster.yaml config file or to cluster template')
    parser.add_argument('--database-config-location', help='Path to database.yaml config file')
    parser.add_argument('--static-location', help='Path to static files directory')
    parser.add_argument('--mon-port', type=int, default=8765, help='Monitoring port for liveness checks')
    parser.add_argument(
        '--install-root',
        help='Remote install root on cluster hosts (default from NEMESIS_INSTALL_ROOT / Settings.install_root)',
    )
    parser.add_argument(
        '--kikimr-logs-directory',
        help='Kikimr logs path for agent safety wardens (default from KIKIMR_LOGS_DIRECTORY)',
    )

    return parser.parse_args()


def run_liveness_checks(settings: Settings):
    """
    Run liveness checks synchronously and output JSON result to stdout.

    This function is designed to be called as a subprocess with timeout.
    It runs the library wardens directly without Flask overhead.

    Output format (JSON):
    {
        "status": "completed" | "error",
        "checks": [
            {
                "name": "AllTabletsAlive",
                "category": "liveness",
                "status": "ok" | "violation" | "error",
                "violations": [...],
                "error_message": "..." (optional)
            },
            ...
        ],
        "error_message": "..." (optional, only if status is "error")
    }
    """
    # Suppress all logging to stderr to keep stdout clean for JSON
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)

    # Get hosts from config
    hosts = get_hosts_from_yaml(settings.yaml_config_location)

    if not hosts:
        result = {
            "status": "error",
            "checks": [],
            "error_message": "No hosts found in config"
        }
        print(json.dumps(result))
        return

    # Create cluster object
    cluster_yaml = None
    template_yaml = settings.yaml_config_location
    if settings.database_config_location:
        template_yaml = settings.database_config_location
        cluster_yaml = settings.yaml_config_location

    cluster = ExternalKiKiMRCluster(
        cluster_template=template_yaml,
        kikimr_configure_binary_path=None,
        kikimr_path=None,
        yaml_config=cluster_yaml)

    checks: list = []
    try:
        checks = run_orchestrator_liveness_cli_batch(cluster)
        result = {
            "status": "completed",
            "checks": checks,
        }
    except Exception as e:
        result = {
            "status": "error",
            "checks": checks,
            "error_message": str(e),
        }

    # Output JSON to stdout
    print(json.dumps(result))


def main():
    args = parse_args()

    # Build kwargs from argv (only include non-None values)
    argv_kwargs = {}
    if args.nemesis_type is not None:
        argv_kwargs['nemesis_type'] = args.nemesis_type
    if args.app_host is not None:
        argv_kwargs['app_host'] = args.app_host
    if args.app_port is not None:
        argv_kwargs['app_port'] = args.app_port
    if args.yaml_config_location is not None:
        argv_kwargs['yaml_config_location'] = args.yaml_config_location
    if args.database_config_location is not None:
        argv_kwargs['database_config_location'] = args.database_config_location
    if args.static_location is not None:
        argv_kwargs['static_location'] = args.static_location
    if getattr(args, 'install_root', None) is not None:
        argv_kwargs['install_root'] = args.install_root
    if getattr(args, 'kikimr_logs_directory', None) is not None:
        argv_kwargs['kikimr_logs_directory'] = args.kikimr_logs_directory

    settings = get_orchestrator_settings(**argv_kwargs)

    # Check for command-line arguments
    if args.command == "install":
        # Install mode: deploy services and print orchestrator endpoint
        print("Installing nemesis services on cluster...")
        hosts = get_hosts_from_yaml(settings.yaml_config_location)
        settings.hosts = hosts
        print(f"Hosts: {hosts}")

        orchestrator_host = install_on_hosts(hosts, settings)

        print("\n" + "=" * 60)
        print("Installation completed successfully!")
        print(f"Orchestrator endpoint: http://{orchestrator_host}:{settings.app_port}")
        print(f"Orchestrator UI: http://{orchestrator_host}:{settings.app_port}/static/index.html")
        print("=" * 60 + "\n")
        return

    elif args.command == "stop":
        # Stop mode: stop all services on cluster
        print("Stopping nemesis services on cluster...")
        hosts = get_hosts_from_yaml(settings.yaml_config_location)
        print(f"Hosts: {hosts}")

        stop_agent_services(hosts)

        print("\n" + "=" * 60)
        print("All services stopped successfully!")
        print("=" * 60 + "\n")
        return

    elif args.command == "liveness":
        # Liveness mode: run liveness checks and output JSON to stdout
        # This is designed to be called as a subprocess with timeout
        run_liveness_checks(settings)
        return

    elif args.command == "run":
        # Expose /sensors (monlib) on nemesis_mon_port; main API stays on app_port.
        # Second Flask app + thread: no conflict as long as nemesis_mon_port != app_port.
        monitor.setup_page(settings.app_host, settings.nemesis_mon_port)
        # threaded=True is important for concurrent request handling
        from ydb.tests.stability.nemesis.app import app
        app.run(
            host=settings.app_host,
            port=settings.app_port,
            threaded=True,
            debug=False
        )
    else:
        raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
