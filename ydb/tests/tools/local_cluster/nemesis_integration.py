"""
Nemesis chaos testing integration for a local KiKiMR cluster.

Extracts the nemesis binary from bundled resources, writes a nemesis-specific
cluster.yaml, then launches nemesis as a child subprocess.

Note on cluster.yaml format:
  The harness generates a YDB binary config (blob_storage_config, domains_config,
  grpc_config, ...).  Nemesis needs a different topology format: hosts[].name and
  domains[].dynamic_slots.  These two formats are not interchangeable, so we
  write a separate nemesis-specific file from the KiKiMR cluster object.
"""

import logging
import os
import socket
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

import yaml
from library.python import resource

logger = logging.getLogger(__name__)


def _extract_binary(tmpdir: str) -> str:
    data = resource.find('nemesis')
    path = os.path.join(tmpdir, 'nemesis')
    with open(path, 'wb') as f:
        f.write(data)
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)
    return path


def _write_cluster_yaml(cluster, output_path: str) -> None:
    # Nemesis topology format: hosts[].name + domains[].dynamic_slots.
    # Incompatible with the YDB binary config the harness writes, so we generate it separately.
    node = next(iter(cluster.nodes.values()))
    config = {
        'hosts': [{'name': node.host}],
        'domains': [{'name': 'Root', 'dynamic_slots': len(cluster.slots)}],
    }
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)


def _wait_for_server(port: int, timeout: float = 15.0) -> bool:
    health_url = f'http://localhost:{port}/health'
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=1):
                return True
        except (urllib.error.URLError, OSError):
            time.sleep(0.3)
    return False


def start_nemesis(
    cluster,
    working_dir: str,
    nemesis_port: int = 31434,
) -> tuple[subprocess.Popen, str]:
    """
    Start a nemesis orchestrator as a child subprocess for a local YDB cluster.

    Returns:
        (proc, url) — the subprocess handle and the nemesis web UI URL.
    """
    tmpdir = tempfile.mkdtemp(prefix='nemesis_')
    nemesis_bin = _extract_binary(tmpdir)
    logger.info("Extracted nemesis binary: %s", nemesis_bin)

    cluster_yaml_path = os.path.join(working_dir, 'nemesis_cluster.yaml')
    _write_cluster_yaml(cluster, cluster_yaml_path)
    logger.info("Written nemesis cluster.yaml: %s", cluster_yaml_path)

    env = os.environ.copy()
    env['YAML_CONFIG_LOCATION'] = cluster_yaml_path
    env['NEMESIS_TYPE'] = 'orchestrator'
    env['APP_PORT'] = str(nemesis_port)
    env['APP_HOST'] = '0.0.0.0'
    env['LOCAL_MODE'] = 'true'
    env.setdefault('KIKIMR_LOGS_DIRECTORY', working_dir)

    log_path = os.path.join(working_dir, 'nemesis.log')
    log_file = open(log_path, 'w')  # kept open for subprocess lifetime
    proc = subprocess.Popen(
        [nemesis_bin, 'run'],
        env=env,
        stdout=log_file,
        stderr=log_file,
    )
    logger.info("Nemesis subprocess started (pid=%d), logs: %s", proc.pid, log_path)

    if _wait_for_server(nemesis_port):
        logger.info("Nemesis server is ready")
    else:
        logger.warning("Nemesis server did not become ready within 15 seconds")

    hostname = socket.getfqdn()
    return proc, f'http://{hostname}:{nemesis_port}/static/index.html'
