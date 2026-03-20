from concurrent.futures import ThreadPoolExecutor
import os
import subprocess

import yaml
from ydb.tests.stability.nemesis.internal.config import Settings, AgentSettings


def upload_binary(host, is_orchestrator=False, yaml_config_location=None):
    print(f'Uploading binary to {host}')
    ssh_base = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '-A']
    ssh_rsh = " ".join(ssh_base)

    subprocess.check_call(["rsync", "-avqLW", "--del", "--no-o", "--no-g",
                           "--rsh={}".format(ssh_rsh),
                           "--rsync-path=sudo rsync", "--progress", './nemesis', f'{host}:/Berkanavt/nemesis/bin/agent'],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Upload static files and config for orchestrator
    if is_orchestrator:
        print(f'Uploading static files to {host}')
        subprocess.check_call(["rsync", "-avqLW", "--del", "--no-o", "--no-g",
                               "--rsh={}".format(ssh_rsh),
                               "--rsync-path=sudo rsync", "--progress", './static/', f'{host}:/Berkanavt/nemesis/static/'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if yaml_config_location:
            print(f'Uploading cluster config to {host}')
            subprocess.check_call(["rsync", "-avqLW", "--no-o", "--no-g",
                                   "--rsh={}".format(ssh_rsh),
                                   "--rsync-path=sudo rsync", "--progress", yaml_config_location, f'{host}:/Berkanavt/nemesis/cluster.yaml'],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print(f'Uploading service to {host}')
    subprocess.check_call(["rsync", "-avqLW", "--no-o", "--no-g",
                           "--rsh={}".format(ssh_rsh),
                           "--rsync-path=sudo rsync", "--progress", f'./nemesis-agent.service.{host}',
                           f'{host}:/etc/systemd/system/nemesis-agent.service'],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print(f'Restarting service on {host}')
    subprocess.check_call(ssh_base + [host, "sudo systemctl daemon-reload && sudo systemctl enable nemesis-agent && sudo systemctl restart nemesis-agent"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def stop_agent_service(host):
    ssh_base = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', '-A']

    print(f'Stopping service on {host}')
    subprocess.check_call(ssh_base + [host, "sudo systemctl stop nemesis-agent"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get_hosts_from_yaml(yaml_path):
    """
    Get list of hosts from cluster.yaml file.

    Args:
        yaml_path: Path to the cluster.yaml file

    Returns:
        List of host names
    """
    with open(yaml_path, 'r') as r:
        yaml_config = yaml.safe_load(r.read())
        hosts = [host.get('name') or host.get('host') for host in yaml_config.get('hosts', [])]
        if len(hosts) == 0:
            hosts = [host.get('name') or host.get('host') for host in yaml_config.get('config', {}).get('hosts', [])]
        return hosts


def install_on_hosts(hosts, settings: Settings):
    """
    Install services on hosts:
    - First host: orchestrator mode (master)
    - Remaining hosts: agent mode
    """
    if not hosts:
        return

    orchestrator_host = hosts[0]
    agent_hosts = hosts[1:] if len(hosts) > 1 else []

    # Create service file for orchestrator (first host)
    with open(f"nemesis-agent.service.{orchestrator_host}", "w") as f:
        # Use the copied config location on the orchestrator host
        config_location = "/Berkanavt/nemesis/cluster.yaml" if settings.yaml_config_location else ""
        yaml_config_env = f"Environment=YAML_CONFIG_LOCATION={config_location}\n" if config_location else ""

        f.write(f"""[Unit]
Description=Nemesis Orchestrator Service
After=network-online.target
Wants=nemesis-autoconf.service
StartLimitInterval=10
StartLimitBurst=15

[Service]
Restart=always
RestartSec=10
Environment=NEMESIS_USER=robot-nemesis
Environment=NEMESIS_TYPE=master
Environment=STATIC_LOCATION=/Berkanavt/nemesis/static
Environment=APP_HOST=::
Environment=APP_PORT={settings.app_port}
Environment=MON_HOST={settings.mon_port}
{yaml_config_env}Type=simple
ExecStart=/Berkanavt/nemesis/bin/agent run
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
""")

    # Create service files for agents (remaining hosts)
    for host in agent_hosts:
        agent_settings = AgentSettings.from_master_args(settings)
        agent_settings.app_host = host

        with open(f"nemesis-agent.service.{host}", "w") as f:
            f.write(f"""[Unit]
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
Environment=MON_HOST={agent_settings.mon_port}
Type=simple
ExecStart=/Berkanavt/nemesis/bin/agent run
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
""")

    # Upload binaries to all hosts
    with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        # Upload to orchestrator with static files and config
        executor.submit(upload_binary, orchestrator_host, True, settings.yaml_config_location)

        # Upload to agents (without static files and config)
        for host in agent_hosts:
            executor.submit(upload_binary, host, False, None)

    for host in agent_hosts + [orchestrator_host]:
        os.remove(f"nemesis-agent.service.{host}")

    return orchestrator_host


def stop_agent_services(hosts):
    with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        for host in hosts:
            executor.submit(stop_agent_service, host)
