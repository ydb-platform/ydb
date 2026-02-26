# -*- coding: utf-8 -*-
import logging
import subprocess
import textwrap
import threading
import time
from urllib.parse import urlparse
import requests
import yaml
import ydb

from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.public.api.protos.draft import ydb_dynamic_config_pb2 as dynconfig_proto
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


class WorkloadRegisterNode(WorkloadBase):
    def __init__(self, client, stop):
        super().__init__(client, "", "register_node", stop)
        self.registered = 0
        self.next_port = 0
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Registered: {self.registered}"

    def _get_next_port(self):
        with self.lock:
            port = self.next_port
            self.next_port += 1
            return port

    def _register_node(self, node_port):
        request = ydb_discovery_pb2.NodeRegistrationRequest(
            host="localhost",
            port=node_port,
            resolve_host="localhost",
            address="594f:10c7:ad54:eada:99eb:7b5b:eec2:4490",
            location=ydb_discovery_pb2.NodeLocation(
                data_center="DC",
                module="1",
                rack="2",
                unit="3",
            ),
            path=self.client.database,
        )

        self.client.driver(
            request,
            ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub,
            "NodeRegistration",
            ydb.operation.Operation,
            None,
            (self.client.driver,),
        )

    def _register_node_loop(self):
        while not self.is_stop_requested():
            self._register_node(self._get_next_port())
            with self.lock:
                self.registered += 1

    def get_workload_thread_funcs(self):
        return [self._register_node_loop for x in range(0, 10)]

class BackupValidator:
    SELECTOR_NAME = "system_tablet_backup_workload_validator"

    SYSTEM_TABLETS = {
        "FLAT_HIVE":            [72057594037968897],
        "FLAT_BS_CONTROLLER":   [72057594037932033],
        "FLAT_SCHEMESHARD":     [72057594046678944],
        "CMS":                  [72057594037936128],
        "NODE_BROKER":          [72057594037936129],
        "TENANT_SLOT_BROKER":   [72057594037936130],
        "CONSOLE":              [72057594037936131],
        "FLAT_TX_COORDINATOR":  [72057594046316545, 72057594046316546, 72057594046316547],
        "TX_MEDIATOR":          [72057594046382081, 72057594046382082, 72057594046382083],
        "TX_ALLOCATOR":         [72057594046447617, 72057594046447618, 72057594046447619],
    }

    RECOVERY_TABLETS = {"NODE_BROKER"}

    def __init__(self, endpoint, mon_endpoint, backup_path):
        parsed = urlparse(endpoint)
        self.dynconfig_client = DynConfigClient(parsed.hostname, parsed.port)
        self.mon_endpoint = mon_endpoint
        self.backup_path = backup_path

    @staticmethod
    def _assert_success(response, operation_name):
        status = response.operation.status
        if status != StatusIds.SUCCESS:
            issues = [issue.message for issue in response.operation.issues]
            raise RuntimeError(f"{operation_name} failed with status {status}: {issues}")

    def _retry(self, func, operation_name, retries=10, delay=1):
        for attempt in range(retries):
            response = func()
            if response.operation.status == StatusIds.SUCCESS:
                return response
            if response.operation.status == StatusIds.UNAVAILABLE and attempt < retries - 1:
                time.sleep(delay)
                continue
            self._assert_success(response, operation_name)
        return response

    def _fetch_config(self):
        response = self._retry(self.dynconfig_client.fetch_config, "GetConfig")
        result = dynconfig_proto.GetConfigResult()
        response.operation.result.Unpack(result)

        if result.config and result.config[0]:
            return result.config[0]

        response = self._retry(self.dynconfig_client.fetch_startup_config, "FetchStartupConfig")
        result = dynconfig_proto.FetchStartupConfigResult()
        response.operation.result.Unpack(result)
        full_config = {
            "metadata": {
                "kind": "MainConfig",
                "version": 0,
                "cluster": "",
            },
            "config": yaml.safe_load(result.config),
        }
        return yaml.dump(full_config)

    def _replace_config(self, config_yaml):
        self._retry(lambda: self.dynconfig_client.replace_config(config_yaml), "ReplaceConfig")

    def _make_bootstrap_selector(self, erasure_name):
        channel = lambda i: {
            "channel": i,
            "history": [{"from_generation": 0, "group_id": 0}],
            "channel_erasure_name": erasure_name,
        }
        tablets = []
        for tablet_type, tablet_ids in self.SYSTEM_TABLETS.items():
            for tablet_id in tablet_ids:
                tablet = {
                    "type": tablet_type,
                    "node": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                    "info": {
                        "tablet_id": str(tablet_id),
                        "channels": [channel(0), channel(1), channel(2)],
                    },
                }
                if tablet_type in self.RECOVERY_TABLETS:
                    tablet["boot_type"] = "RECOVERY"
                tablets.append(tablet)
        return {
            "description": self.SELECTOR_NAME,
            "selector": {},
            "config": {
                "bootstrap_config": {
                    "tablet": tablets,
                },
            },
        }

    def _enable_node_broker_recovery(self):
        config_yaml = self._fetch_config()
        full_config = yaml.safe_load(config_yaml)
        erasure_name = full_config["config"].get("static_erasure", "mirror-3-dc")
        full_config.setdefault("selector_config", []).append(
            self._make_bootstrap_selector(erasure_name)
        )
        updated_yaml = yaml.dump(full_config)
        self._replace_config(updated_yaml)

    def _disable_node_broker_recovery(self):
        config_yaml = self._fetch_config()
        full_config = yaml.safe_load(config_yaml)
        full_config["selector_config"] = [
            s for s in full_config.get("selector_config", [])
            if s.get("description") != self.SELECTOR_NAME]
        updated_yaml = yaml.dump(full_config)
        self._replace_config(updated_yaml)

    def _get_tablet_node_id(self, tablet_id):
        url = f"{self.mon_endpoint}/viewer/json/tabletinfo?tablet_id={tablet_id}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        tablet_info = data.get("TabletStateInfo", [])
        if not tablet_info:
            raise RuntimeError(f"No tablet info found for tablet {tablet_id}")
        return tablet_info[0].get("NodeId")

    def _fetch_node_list(self):
        url = f"{self.mon_endpoint}/viewer/json/nodelist"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def _get_node_hostname(self, node_id):
        for node in self.nodes:
            if node.get("Id") == node_id:
                return node.get("Host", "unknown")
        raise RuntimeError(f"Node {node_id} not found in node list")

    def _run_on_host(self, host, cmd):
        if host in ("localhost", "127.0.0.1", "::1"):
            return subprocess.run(
                ["bash", "-c", cmd],
                capture_output=True, text=True, timeout=30,
            )
        return subprocess.run(
            ["ssh", host, cmd],
            capture_output=True, text=True, timeout=30,
        )

    def _discover_backups(self, backup_path):
        tablet_id = self.SYSTEM_TABLETS["NODE_BROKER"][0]
        tablet_backup_path = f"{backup_path}/node_broker/{tablet_id}"
        print(f"Tablet backup path: {tablet_backup_path}")

        hosts = set(node.get("Host") for node in self.nodes if node.get("Host"))
        print(f"Hosts: {hosts}")

        backups = {}
        for host in hosts:
            cmd = (
                f'for d in {tablet_backup_path}/*; do '
                f'[ -d "$d/snapshot" ] && basename "$d"; '
                f'done'
            )
            result = self._run_on_host(host, cmd)

            lines = [line for line in result.stdout.strip().split("\n") if line]
            if lines:
                backups[host] = lines
                print(f"  [{host}] found {len(lines)} backup(s): {lines}")
            else:
                print(f"  [{host}] no backups with snapshots found")

        return backups

    def validate(self):
        print("Starting backup validation...")
        self.nodes = self._fetch_node_list()
        self._enable_node_broker_recovery()

        backups = self._discover_backups(self.backup_path)
        print(f"Discovered {sum(len(backup) for backup in backups.values())} backups")

        tablet_id = self.SYSTEM_TABLETS["NODE_BROKER"][0]
        node_id = self._get_tablet_node_id(tablet_id)
        hostname = self._get_node_hostname(node_id)
        print(f"NodeBroker is running on node {node_id} (hostname: {hostname})")

        self._disable_node_broker_recovery()
        print("Backup validation completed.")


class WorkloadRunner:
    def __init__(self, client, duration, endpoint, mon_endpoint, backup_path):
        self.client = client
        self.duration = duration
        self.endpoint = endpoint
        self.mon_endpoint = mon_endpoint
        self.backup_path = backup_path
        ydb.interceptor.monkey_patch_event_handler()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self):
        stop = threading.Event()
        workloads = [
            WorkloadRegisterNode(self.client, stop),
        ]

        for w in workloads:
            w.start()
        started_at = time.time()
        while time.time() - started_at < self.duration:
            print(f"Elapsed {(int)(time.time() - started_at)} seconds, stat:")
            for w in workloads:
                print(f"\t{w.name}: {w.get_stat()}")
            time.sleep(10)
        stop.set()
        print("Waiting for stop...")
        for w in workloads:
            w.join()
        print("Stopped")

        validator = BackupValidator(self.endpoint, self.mon_endpoint, self.backup_path)
        validator.validate()
