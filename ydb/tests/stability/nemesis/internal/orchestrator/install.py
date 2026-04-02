from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import shlex
import subprocess
import sys

import yaml

from ydb.tests.stability.nemesis.internal.config import Settings, AgentSettings


def _get_app_dir() -> str:
    """Return the directory containing the nemesis binary.

    When the program is executed as ``./nemesis install ...`` the binary
    (or the ``__main__.py`` entry-point) lives next to the ``static/``
    directory that must be uploaded to the orchestrator host.  All local
    paths (binary, static assets, temporary service-unit files) are
    resolved relative to this directory so that the install command works
    regardless of the caller's working directory.
    """
    return os.path.dirname(os.path.abspath(sys.argv[0]))


def _ssh_base():
    return [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-A",
    ]


def _run_external(argv: list, *, log_line: str, host: str | None = None) -> None:
    """
    Run a subprocess quietly: only our log_line on success.
    On failure, print captured stdout/stderr (rsync/ssh noise stays hidden when all is OK).
    """
    prefix = f"[{host}] " if host else ""
    print(f"{prefix}{log_line}")
    r = subprocess.run(argv, capture_output=True, text=True)
    if r.returncode != 0:
        parts = [f"{prefix}FAILED ({r.returncode}): {log_line}"]
        if r.stdout and r.stdout.strip():
            parts.append("--- stdout ---\n" + r.stdout.rstrip())
        if r.stderr and r.stderr.strip():
            parts.append("--- stderr ---\n" + r.stderr.rstrip())
        print("\n".join(parts), file=sys.stderr)
        raise RuntimeError(
            f"{prefix.rstrip()}{log_line} failed with exit code {r.returncode}"
        ) from None


def ensure_remote_install_dirs(host: str, settings: Settings, *, is_orchestrator: bool) -> None:
    """
    Create and verify target paths on the remote host before rsync.
    rsync targets: {install_root}/bin/agent, optionally {install_root}/static/, cluster.yaml under root.
    """
    root = settings.install_root.rstrip("/")
    dirs = [f"{root}/bin"]
    if is_orchestrator:
        dirs.append(f"{root}/static")

    mkdir = "sudo mkdir -p " + " ".join(shlex.quote(d) for d in dirs)
    verify = " && ".join(f"sudo test -d {shlex.quote(d)}" for d in dirs)
    remote_script = f"{mkdir} && {verify}"

    argv = _ssh_base() + [host, remote_script]
    _run_external(
        argv,
        log_line=f"ensure remote dirs ({', '.join(dirs)})",
        host=host,
    )


def _require_local_path(path: str, *, kind: str) -> None:
    if kind == "dir":
        if not os.path.isdir(path):
            raise FileNotFoundError(f"Local path missing or not a directory: {path}")
    else:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Local file missing: {path}")


def upload_binary(host, settings: Settings, is_orchestrator=False, yaml_config_location=None, app_dir=None):
    if app_dir is None:
        app_dir = _get_app_dir()

    nemesis_binary = os.path.join(app_dir, "nemesis")
    _require_local_path(nemesis_binary, kind="file")

    ssh_base = _ssh_base()
    ssh_rsh = " ".join(ssh_base)
    root = settings.install_root.rstrip("/")

    ensure_remote_install_dirs(host, settings, is_orchestrator=is_orchestrator)

    _run_external(
        [
            "rsync",
            "-aqLW",
            "--del",
            "--no-o",
            "--no-g",
            "--rsh={}".format(ssh_rsh),
            "--rsync-path=sudo rsync",
            nemesis_binary,
            f"{host}:{root}/bin/agent",
        ],
        log_line="upload nemesis binary",
        host=host,
    )

    if is_orchestrator:
        # Use settings.static_location if it's an absolute path;
        # otherwise resolve relative to app_dir (default: "static").
        if os.path.isabs(settings.static_location):
            static_dir = settings.static_location
        else:
            static_dir = os.path.join(app_dir, settings.static_location)
        _require_local_path(static_dir, kind="dir")
        _run_external(
            [
                "rsync",
                "-aqLW",
                "--del",
                "--no-o",
                "--no-g",
                "--rsh={}".format(ssh_rsh),
                "--rsync-path=sudo rsync",
                static_dir + "/",
                f"{host}:{root}/static/",
            ],
            log_line="upload static/",
            host=host,
        )

        if yaml_config_location:
            _require_local_path(yaml_config_location, kind="file")
            _run_external(
                [
                    "rsync",
                    "-aqLW",
                    "--no-o",
                    "--no-g",
                    "--rsh={}".format(ssh_rsh),
                    "--rsync-path=sudo rsync",
                    yaml_config_location,
                    f"{host}:{root}/cluster.yaml",
                ],
                log_line="upload cluster.yaml",
                host=host,
            )
    elif yaml_config_location:
        _require_local_path(yaml_config_location, kind="file")
        _run_external(
            [
                "rsync",
                "-aqLW",
                "--no-o",
                "--no-g",
                "--rsh={}".format(ssh_rsh),
                "--rsync-path=sudo rsync",
                yaml_config_location,
                f"{host}:{root}/cluster.yaml",
            ],
            log_line="upload cluster.yaml (agent)",
            host=host,
        )

    unit_file = os.path.join(app_dir, f"nemesis-agent.service.{host}")
    _require_local_path(unit_file, kind="file")
    _run_external(
        [
            "rsync",
            "-aqLW",
            "--no-o",
            "--no-g",
            "--rsh={}".format(ssh_rsh),
            "--rsync-path=sudo rsync",
            unit_file,
            f"{host}:/etc/systemd/system/nemesis-agent.service",
        ],
        log_line="upload systemd unit",
        host=host,
    )

    _run_external(
        ssh_base
        + [
            host,
            "sudo systemctl daemon-reload && sudo systemctl enable nemesis-agent && sudo systemctl restart nemesis-agent",
        ],
        log_line="restart nemesis-agent (systemd)",
        host=host,
    )


def stop_agent_service(host):
    ssh_base = _ssh_base()
    _run_external(
        ssh_base + [host, "sudo systemctl stop nemesis-agent"],
        log_line="stop nemesis-agent (systemd)",
        host=host,
    )


def get_hosts_from_yaml(yaml_path):
    """
    Get list of hosts from cluster.yaml file.

    Args:
        yaml_path: Path to the cluster.yaml file

    Returns:
        List of host names
    """
    with open(yaml_path, "r") as r:
        yaml_config = yaml.safe_load(r.read())
        hosts = [host.get("name") or host.get("host") for host in yaml_config.get("hosts", [])]
        if len(hosts) == 0:
            hosts = [
                host.get("name") or host.get("host")
                for host in yaml_config.get("config", {}).get("hosts", [])
            ]
        return hosts


def install_on_hosts(hosts, settings: Settings):
    """
    Install services on hosts:
    - First host: orchestrator mode
    - Remaining hosts: agent mode
    """
    if not hosts:
        return None

    app_dir = _get_app_dir()
    orchestrator_host = hosts[0]
    agent_hosts = hosts[1:] if len(hosts) > 1 else []

    root = settings.install_root.rstrip("/")

    # Create service file for orchestrator (first host)
    with open(os.path.join(app_dir, f"nemesis-agent.service.{orchestrator_host}"), "w") as f:
        # Use the copied config location on the orchestrator host
        config_location = f"{root}/cluster.yaml" if settings.yaml_config_location else ""
        yaml_config_env = f"Environment=YAML_CONFIG_LOCATION={config_location}\n" if config_location else ""

        f.write(
            f"""[Unit]
Description=Nemesis Orchestrator Service
After=network-online.target
Wants=nemesis-autoconf.service
StartLimitInterval=10
StartLimitBurst=15

[Service]
Restart=always
RestartSec=10
Environment=NEMESIS_USER=robot-nemesis
Environment=NEMESIS_TYPE=orchestrator
Environment=STATIC_LOCATION={root}/static
Environment=APP_HOST=::
Environment=APP_PORT={settings.app_port}
Environment=MON_PORT={settings.mon_port}
Environment=NEMESIS_INSTALL_ROOT={root}
Environment=KIKIMR_LOGS_DIRECTORY={settings.kikimr_logs_directory}
{yaml_config_env}Type=simple
ExecStart={root}/bin/agent run
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=nemesis-orchestrator
SyslogFacility=daemon
SyslogLevel=err
LimitNOFILE=65536
LimitCORE=0
LimitMEMLOCK=32212254720

[Install]
WantedBy=multi-user.target
"""
        )

    # Create service files for agents (remaining hosts)
    for host in agent_hosts:
        agent_settings = AgentSettings.from_orchestrator_args(settings)
        agent_settings.app_host = host

        agent_config_location = f"{root}/cluster.yaml" if settings.yaml_config_location else ""
        agent_yaml_env = (
            f"Environment=YAML_CONFIG_LOCATION={agent_config_location}\n" if agent_config_location else ""
        )

        with open(os.path.join(app_dir, f"nemesis-agent.service.{host}"), "w") as f:
            f.write(
                f"""[Unit]
Description=Nemesis Agent Service
After=network-online.target
Wants=nemesis-autoconf.service
StartLimitInterval=10
StartLimitBurst=15

[Service]
Restart=always
RestartSec=10
Environment=NEMESIS_USER=robot-nemesis
Environment=NEMESIS_TYPE=agent
Environment=APP_HOST=::
Environment=APP_PORT={agent_settings.app_port}
Environment=MON_PORT={agent_settings.mon_port}
Environment=NEMESIS_INSTALL_ROOT={root}
Environment=KIKIMR_LOGS_DIRECTORY={agent_settings.kikimr_logs_directory}
{agent_yaml_env}Type=simple
ExecStart={root}/bin/agent run
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=nemesis-agent
SyslogFacility=daemon
SyslogLevel=err
LimitNOFILE=65536
LimitCORE=0
LimitMEMLOCK=32212254720

[Install]
WantedBy=multi-user.target
"""
            )

    # Upload binaries to all hosts — collect futures so thread exceptions are not swallowed
    futures = []
    with ThreadPoolExecutor(max_workers=max(len(hosts), 1)) as executor:
        futures.append(
            executor.submit(upload_binary, orchestrator_host, settings, True, settings.yaml_config_location, app_dir)
        )
        for host in agent_hosts:
            futures.append(
                executor.submit(upload_binary, host, settings, False, settings.yaml_config_location or None, app_dir)
            )

        for fut in as_completed(futures):
            fut.result()

    for host in agent_hosts + [orchestrator_host]:
        os.remove(os.path.join(app_dir, f"nemesis-agent.service.{host}"))

    return orchestrator_host


def stop_agent_services(hosts):
    if not hosts:
        return
    futures = []
    with ThreadPoolExecutor(max_workers=max(len(hosts), 1)) as executor:
        for host in hosts:
            futures.append(executor.submit(stop_agent_service, host))
        for fut in as_completed(futures):
            fut.result()
