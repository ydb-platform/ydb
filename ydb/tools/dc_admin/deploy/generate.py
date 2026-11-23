"""Render the configuration and systemd units for a single-host mirror-3-dc stand."""

import argparse
import json
import os
from pathlib import Path
import re
import stat

import yaml

BIN = "/opt/ydb/bin"
APP = "/opt/ydb/dc-admin"
DATA = "/var/lib/ydb-dc"
DCS = ("dc1", "dc2", "dc3")


def inventory(paths):
    nodes = {}
    for i, path in enumerate(paths, 1):
        dc = DCS[(i - 1) // 3]
        nodes[i] = {
            "unit": f"ydb_{dc}_node{1 + (i - 1) % 3}",
            "dc": dc,
            "pdisk": path,
            "grpc": 2134 + i,
            "mon": 19200 + i,
            "ic": 19300 + i,
        }
    return nodes


def config(nodes):
    hosts = []
    host_configs = []
    for i, node in nodes.items():
        hosts.append(
            {
                "host": "localhost",
                "port": node["ic"],
                "host_config_id": i,
                "location": {"data_center": node["dc"], "rack": f"rack-{i}"},
            }
        )
        host_configs.append({"host_config_id": i, "drive": [{"type": "NVME", "path": node["pdisk"]}]})
    return {
        "metadata": {"kind": "MainConfig", "cluster": "", "version": 0},
        "config": {
            "erasure": "mirror-3-dc",
            "fail_domain_type": "rack",
            "default_disk_type": "NVME",
            "self_management_config": {"enabled": True},
            "domains_config": {
                name: {"ring": {"nto_select": len(nodes), "ring": [{"node": [n]} for n in nodes]}}
                for name in (
                    "explicit_state_storage_config",
                    "explicit_state_storage_board_config",
                    "explicit_scheme_board_config",
                )
            },
            "actor_system_config": {"use_auto_config": True, "cpu_count": 8},
            "grpc_config": {"host": "[::1]", "public_host": "ip6-localhost"},
            "interconnect_config": {"start_tcp": True, "bind_on_all_addresses": False},
            "hosts": hosts,
            "host_configs": host_configs,
        },
    }


def l3_affinities(count):
    domains = set()
    for cache in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cache/index*/level"):
        if cache.read_text().strip() == "3":
            domain = cache.with_name("shared_cpu_list").read_text().strip()
            domains.add(domain)
    ordered = sorted(domains, key=lambda s: int(re.split(r"[-,]", s)[0]))
    if len(ordered) < count:
        raise ValueError(f"need {count} distinct L3 domains, found {len(ordered)}")
    return [s.replace(",", " ") for s in ordered[:count]]


def unit(description, argv, settings):
    return "\n".join(
        [
            "[Unit]",
            f"Description={description}",
            "After=network.target",
            "",
            "[Service]",
            "User=ydb",
            *settings,
            f"ExecStart={' '.join(argv)}",
            "Restart=on-failure",
            "RestartSec=5",
            "TimeoutStopSec=120",
            "LimitNOFILE=65536",
            "UMask=0077",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
            "",
        ]
    )


def render(out, nodes, dynamic, affinities):
    out.mkdir(parents=True, exist_ok=False)
    units = out / "systemd"
    units.mkdir()
    (out / "config.yaml").write_text(yaml.safe_dump(config(nodes), sort_keys=False))
    (out / "nodes.json").write_text(json.dumps(nodes, indent=2) + "\n")
    for i in range(1, 10 + dynamic):
        directory = f"{DATA}/node-{i}"
        argv = [f"{BIN}/ydbd", "server", f"--config-dir={directory}", "--node-domain=Root", "--mon-address=::1"]
        settings = [f"WorkingDirectory={directory}"]
        if affinities:
            settings.append(f"CPUAffinity={affinities[i - 1]}")
        if i <= 9:
            node = nodes[i]
            name = node["unit"]
            grpc = node["grpc"]
            settings.append(f"ExecStartPre=/usr/bin/test -b {node['pdisk']}")
            argv.append(f"--node={i}")
        else:
            name = "ydb_tpcc_dynamic" if i == 10 else f"ydb_tpcc_dynamic_{i - 9}"
            grpc = 2135 + i
            argv.extend([f"--node-broker=ip6-localhost:{node['grpc']}" for node in nodes.values()])
            argv.extend(["--tenant=/Root/tpcc", f"--data-center={DCS[i - 10]}", "--rack=compute"])
        argv.extend([f"--grpc-port={grpc}", f"--mon-port={19200 + i}", f"--ic-port={19300 + i}"])
        (units / f"{name}.service").write_text(unit(name, argv, settings))

    admin = [
        "/usr/bin/python3",
        f"{APP}/server.py",
        "--ydb",
        f"{BIN}/ydb",
        "--endpoint",
        "grpc://ip6-localhost:2135",
        "--database",
        "/Root",
        "--workload-endpoint",
        "grpc://ip6-localhost:2145",
        "--workload-database",
        "/Root/tpcc",
        "--monitoring-url",
        "http://[::1]:31001/monitoring/",
    ]
    for dc in DCS:
        admin.extend(["--datacenter", dc])
    (units / "ydb_dc_admin.service").write_text(
        unit(
            "YDB DC admin",
            admin,
            [
                f"WorkingDirectory={DATA}/admin",
                "Environment=DC_ADMIN_NODES=/etc/ydb-dc/nodes.json",
                f"Environment=DC_ADMIN_JOURNAL={DATA}/admin/commands.sqlite3",
                "EnvironmentFile=-/etc/ydb-dc/admin.env",
            ],
        )
    )
    ports = [str(node["mon"]) for node in nodes.values()]
    monitor = ["/usr/bin/python3", f"{APP}/monitor_proxy.py", *ports]
    (units / "ydb_dc_monitor.service").write_text(unit("YDB monitoring proxy", monitor, []))
    (units / "ydb_dc_monitor.socket").write_text(
        "[Unit]\nDescription=YDB monitoring listener\n\n"
        "[Socket]\nListenStream=[::1]:31001\n\n[Install]\nWantedBy=sockets.target\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="create this new directory")
    parser.add_argument("--disks", type=Path, help="nine PDisk paths in DC and node order, one per line")
    parser.add_argument("--dynamic-nodes", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--pin-l3", action="store_true", help="assign each YDB process a separate local L3 cache")
    parser.add_argument("--check-devices", action="store_true", help="verify nine distinct block devices on this host")
    args = parser.parse_args()
    paths = (
        args.disks.read_text().splitlines()
        if args.disks
        else [f"/dev/mapper/ydb_{dc}_node{n}_pdisk" for dc in DCS for n in range(1, 4)]
    )
    if len(paths) != 9 or len(set(paths)) != 9 or any(not re.fullmatch(r"/dev/[a-zA-Z0-9_./+-]+", p) for p in paths):
        parser.error("supply nine different absolute /dev paths without whitespace")
    if any(".." in Path(p).parts for p in paths):
        parser.error("device paths must not contain '..'")
    try:
        if args.check_devices:
            devices = [os.stat(p) for p in paths]
            if not all(stat.S_ISBLK(s.st_mode) for s in devices) or len({s.st_rdev for s in devices}) != 9:
                raise ValueError("paths must point to nine distinct block devices")
        affinities = l3_affinities(9 + args.dynamic_nodes) if args.pin_l3 else []
        render(args.output, inventory(paths), args.dynamic_nodes, affinities)
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
