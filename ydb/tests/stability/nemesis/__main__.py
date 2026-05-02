import argparse
import json
import logging
import os
import sys
import tempfile
import warnings

# Suppress noisy DeprecationWarnings from vendored Flask / Werkzeug / pkgutil
# (pkgutil.find_loader, ast.Str, etc.) so they don't pollute --help and CLI output.
warnings.filterwarnings("ignore", category=DeprecationWarning)

from library.python import resource  # noqa: E402
from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster  # noqa: E402
from ydb.tests.stability.nemesis.internal.config import Settings, get_orchestrator_settings  # noqa: E402
from ydb.tests.stability.nemesis.internal.orchestrator.install import get_hosts_from_yaml, install_on_hosts, stop_agent_services  # noqa: E402
from ydb.tests.tools.nemesis.library import monitor  # noqa: E402
from ydb.tests.stability.nemesis.internal.orchestrator.orchestrator_warden_execution import run_orchestrator_liveness_cli_batch  # noqa: E402


_STATIC_FILES = [
    'index.html',
    'HostProcessItem.js',
    'HostStatusItem.js',
    'NemesisGroupAccordion.js',
    'ProcessCard.js',
    'ProcessTypeGroup.js',
    'WardenChecksCard.js',
]


def _extract_static() -> str:
    """Extract bundled static files to a temp directory and return its path."""
    tmpdir = tempfile.mkdtemp(prefix='nemesis_static_')
    for filename in _STATIC_FILES:
        data = resource.find(f'nemesis_static/{filename}')
        with open(os.path.join(tmpdir, filename), 'wb') as f:
            f.write(data)
    return tmpdir


_DESCRIPTION = """\
Nemesis — chaos / stability testing tool for YDB clusters.

Deploys an orchestrator + per-host agents that inject faults
(kill nodes, block network, break disks, …) on a schedule and
monitor cluster health.

User commands:
  install
                        Deploy nemesis binary and systemd units to every host listed in
                        the cluster YAML, then start the services.  The first host becomes
                        the orchestrator; the rest become agents.
  stop
                        Stop nemesis-agent systemd services on every cluster host.

Commands used by the test framework (not intended for direct use):
  run
                        Start the Flask application (orchestrator or agent depending on
                        --nemesis-type / NEMESIS_TYPE).  Normally launched by systemd
                        after ``install``.
  liveness
                        Run orchestrator liveness checks once and print a JSON report to
                        stdout.  Designed to be called as a subprocess with a timeout.

Examples:
  # Install and start nemesis on the cluster (two-file config):
  nemesis install \\
      --yaml-config-location /path/to/config.yaml \\
      --database-config-location /path/to/databases.yaml

  # Install with a single combined YAML:
  nemesis install --yaml-config-location /path/to/cluster.yaml

  # Stop all services:
  nemesis stop --yaml-config-location /path/to/config.yaml
"""


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="nemesis",
        description=_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )

    parser.add_argument(
        'command',
        choices=['install', 'stop', 'run', 'liveness'],
        help=(
            "install — deploy and start services on the cluster; "
            "stop — stop services on the cluster; "
            "run — start the Flask app (used by systemd); "
            "liveness — one-shot liveness check (JSON to stdout)"
        ),
    )

    # ---- Cluster configuration ----
    cfg = parser.add_argument_group("cluster configuration")
    cfg.add_argument(
        '--yaml-config-location',
        metavar='PATH',
        help=(
            'Path to the cluster YAML config (contains config.hosts, '
            'config.bridge_config, etc.).  '
            'When only one config file is used, this is the combined '
            'cluster + database template.'
        ),
    )
    cfg.add_argument(
        '--database-config-location',
        metavar='PATH',
        help=(
            'Path to the database template YAML (contains domains, '
            'dynamic_slots, etc.).  Required only when the cluster '
            'layout is split into two files.'
        ),
    )

    # ---- Network / ports ----
    net = parser.add_argument_group("network")
    net.add_argument(
        '--app-host',
        metavar='HOST',
        help='Address to bind the HTTP API to (default: "::", all interfaces).',
    )
    net.add_argument(
        '--app-port',
        type=int,
        metavar='PORT',
        help='Port for the HTTP API (default: 31434).',
    )
    net.add_argument(
        '--mon-port',
        type=int,
        default=8765,
        metavar='PORT',
        help='Monitoring port for /sensors endpoint (default: 8765).',
    )

    # ---- Deployment layout ----
    deploy = parser.add_argument_group("deployment layout (install / run)")
    deploy.add_argument(
        '--nemesis-type',
        choices=['orchestrator', 'agent'],
        help='Role of this process: orchestrator or agent (set automatically by install).',
    )
    deploy.add_argument(
        '--static-location',
        metavar='PATH',
        help='Path to the static files directory served by the orchestrator UI.',
    )
    deploy.add_argument(
        '--install-root',
        metavar='PATH',
        help=(
            'Remote install root on cluster hosts where binaries and configs '
            'are placed (default: /Berkanavt/nemesis).  '
            'Override via NEMESIS_INSTALL_ROOT env var.'
        ),
    )
    deploy.add_argument(
        '--kikimr-logs-directory',
        metavar='PATH',
        help=(
            'Kikimr log directory on agents, used by safety wardens to grep '
            'for error markers (default: /Berkanavt/kikimr/logs/).  '
            'Override via KIKIMR_LOGS_DIRECTORY env var.'
        ),
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

    # Auto-extract static files before settings are loaded.
    # TODO: use it by deafault in all cases 
    if args.command == 'run':
        static_path = args.static_location or os.environ.get('STATIC_LOCATION', 'static')
        if not os.path.isdir(static_path):
            os.environ['STATIC_LOCATION'] = _extract_static()

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
