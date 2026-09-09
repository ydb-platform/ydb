#!/usr/bin/env python3
"""Regenerate the checked-in, single-host functional fixture (JSON is valid YAML)."""
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def generate():
    config = {
        "yaml_config_enabled": True,
        "erasure": "block-8-2",
        "fail_domain_type": "rack",
        "default_disk_type": "SSD",
        "self_management_config": {"enabled": True},
        "host_configs": [{"host_config_id": 1, "drive": [{"path": "/data/pdisk.dat", "type": "SSD"}]}],
        "hosts": [
            {"host": f"storage-{n}", "node_id": n, "port": 19001, "host_config_id": 1,
             "location": {"data_center": "local", "rack": str(n), "body": n}}
            for n in range(1, 14)
        ],
        "blob_storage_config": {
            "service_set": {"groups": [{
                "group_id": 0, "group_generation": 1, "erasure_species": "block-8-2",
                "rings": [{"fail_domains": [
                    {"vdisk_locations": [{"node_id": n, "path": "/data/pdisk.dat", "pdisk_category": "SSD"}]}
                    for n in range(1, 13)
                ]}],
            }]},
        },
        "domains_config": {
            "domain": [{"name": "Root", "storage_pool_types": [
                {"kind": f"ssd-block{suffix}", "pool_config": {
                    "box_id": 1, "kind": f"ssd-block{suffix}", "erasure_species": species,
                    "vdisk_kind": "Default", "pdisk_filter": [{"property": [{"type": "SSD"}]}],
                }} for suffix, species in [("42", "block-4-2"), ("82", "block-8-2")]
            ]}],
            "state_storage": [{"ssid": 1, "ring": {"node": list(range(1, 9)), "nto_select": 5}}],
        },
        "channel_profile_config": {"profile": [{"profile_id": 0, "channel": [
            {"erasure_species": "block-8-2", "pdisk_category": 1, "storage_pool_kind": "ssd-block82"}
            for _ in range(3)
        ]}]},
        "actor_system_config": {
            "executor": [
                {"type": "BASIC", "threads": 2, "spin_threshold": 0, "name": "System"},
                {"type": "BASIC", "threads": 2, "spin_threshold": 0, "name": "User"},
                {"type": "BASIC", "threads": 1, "spin_threshold": 0, "name": "Batch"},
                {"type": "IO", "threads": 1, "name": "IO"},
                {"type": "BASIC", "threads": 1, "spin_threshold": 0, "name": "IC"},
            ],
            "scheduler": {"resolution": 1024, "spin_threshold": 0, "progress_threshold": 10000},
            "sys_executor": 0, "user_executor": 1, "batch_executor": 2, "io_executor": 3,
            "service_executor": [{"service_name": "Interconnect", "executor_id": 4}],
        },
        "interconnect_config": {"start_tcp": True},
        "grpc_config": {"port": 2135, "services_enabled": ["legacy"]},
        "monitoring_config": {"monitoring_port": 8765, "monitoring_address": "0.0.0.0"},
        "log_config": {"default_level": 5, "sys_log": False},
        "pqconfig": {"check_acl": False, "require_credentials_in_new_protocol": False},
    }
    main = {"metadata": {"kind": "MainConfig", "cluster": "block82-local", "version": 0}, "config": config}
    services = {}
    for n in range(1, 14):
        service = base_service()
        service.update({"hostname": f"storage-{n}", "command": ["storage", str(n)],
                        "volumes": [f"pdisk-{n}:/data"], "mem_limit": "6g"})
        if n == 1:
            service["ports"] = ["127.0.0.1:8765:8765"]
        services[f"storage-{n}"] = service
    services["bootstrap-storage"] = {
        "image": "${BLOCK82_IMAGE:?Run build-image.sh first}",
        "entrypoint": ["python3", "/fixture/control.py"], "command": ["bootstrap-storage"],
        "network_mode": "service:storage-1", "volumes": ["bootstrap-state-storage:/state"],
        "depends_on": {"storage-1": {"condition": "service_started"}},
        "restart": "on-failure", "logging": log_settings(),
    }
    for suffix, port, mon in [("42", 2136, 8766), ("82", 2137, 8767)]:
        service = base_service()
        service.update({
            "hostname": f"compute{suffix}", "command": ["compute", suffix, str(port)],
            "ports": [f"127.0.0.1:{port}:{port}", f"127.0.0.1:{mon}:8765"], "mem_limit": "6g",
            "healthcheck": {"test": ["CMD", "python3", "/fixture/control.py", "check-row", suffix, str(port)],
                            "interval": "20s", "timeout": "15s", "retries": 60, "start_period": "60s"},
        })
        services[f"compute{suffix}"] = service
        # Share the compute network namespace so localhost discovery is also valid
        # for host clients using the published loopback gRPC ports.
        services[f"bootstrap{suffix}"] = {
            "image": "${BLOCK82_IMAGE:?Run build-image.sh first}",
            "entrypoint": ["python3", "/fixture/control.py"],
            "command": ["bootstrap", suffix, str(port)],
            "network_mode": f"service:compute{suffix}",
            "volumes": [f"bootstrap-state-{suffix}:/state"],
            "depends_on": {f"compute{suffix}": {"condition": "service_started"},
                           "bootstrap-storage": {"condition": "service_completed_successfully"}},
            "restart": "on-failure", "logging": log_settings(),
        }
    compose = {"version": "3.8", "services": services, "volumes": {f"pdisk-{n}": {} for n in range(1, 14)}}
    compose["volumes"].update({f"bootstrap-state-{suffix}": {} for suffix in ("42", "82")})
    compose["volumes"]["bootstrap-state-storage"] = {}
    return {"config.yaml": main, "docker-compose.yaml": compose}


def log_settings():
    return {"driver": "json-file", "options": {"max-size": "20m", "max-file": "3"}}


def base_service():
    return {
        "image": "${BLOCK82_IMAGE:?Run build-image.sh first}", "restart": "unless-stopped",
        "stop_grace_period": "60s", "logging": log_settings(),
        "ulimits": {"nofile": {"soft": 65536, "hard": 65536}},
        "healthcheck": {"test": ["CMD", "python3", "/fixture/control.py", "ping"],
                        "interval": "15s", "timeout": "5s", "retries": 60, "start_period": "60s"},
    }


if __name__ == "__main__":
    for name, value in generate().items():
        (ROOT / name).write_text(json.dumps(value, indent=2) + "\n")
