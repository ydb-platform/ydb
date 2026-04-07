import argparse
import json
import logging
import sys
from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.stability.nemesis.internal.config import get_master_settings
from ydb.tests.stability.nemesis.internal.install import get_hosts_from_yaml, install_on_hosts, stop_agent_services


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
    parser.add_argument('--nemesis-type', choices=['master', 'agent'],
                        help='Type of nemesis: master or agent')
    parser.add_argument('--app-host', help='Host to bind the application to')
    parser.add_argument('--app-port', type=int, help='Port to bind the application to')
    parser.add_argument('--yaml-config-location', help='Path to cluster.yaml config file')
    parser.add_argument('--static-location', help='Path to static files directory')
    parser.add_argument('--mon-port', type=int, default=8765, help='Monitoring port for liveness checks')

    return parser.parse_args()


def run_liveness_checks(settings):
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

    # Import wardens
    from ydb.tests.library.wardens.hive import AllTabletsAliveLivenessWarden, BootQueueSizeWarden
    from ydb.tests.library.wardens.schemeshard import SchemeShardHasNoInFlightTransactions
    from ydb.tests.library.wardens.datashard import TxCompleteLagLivenessWarden

    # Create cluster object
    cluster = ExternalKiKiMRCluster(get_master_settings().yaml_config_location, None, None)

    # Define wardens to run
    warden_configs = [
        ('AllTabletsAlive', lambda: AllTabletsAliveLivenessWarden(cluster)),
        ('BootQueueSize', lambda: BootQueueSizeWarden(cluster)),
        ('SchemeShardNoInFlightTx', lambda: SchemeShardHasNoInFlightTransactions(cluster)),
        ('TxCompleteLag', lambda: TxCompleteLagLivenessWarden(cluster)),
    ]

    checks = []
    try:
        for name, warden_fn in warden_configs:
            try:
                warden = warden_fn()
                violations = warden.list_of_liveness_violations
                status = 'violation' if violations else 'ok'
                checks.append({
                    "name": name,
                    "category": "liveness",
                    "status": status,
                    "violations": violations if violations else []
                })
            except Exception as e:
                checks.append({
                    "name": name,
                    "category": "liveness",
                    "status": "error",
                    "violations": [],
                    "error_message": str(e)
                })

        result = {
            "status": "completed",
            "checks": checks
        }
    except Exception as e:
        result = {
            "status": "error",
            "checks": checks,
            "error_message": str(e)
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
    if args.static_location is not None:
        argv_kwargs['static_location'] = args.static_location

    settings = get_master_settings(**argv_kwargs)

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
        # run the application
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
