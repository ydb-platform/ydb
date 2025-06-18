# -*- coding: utf-8 -*-
import threading
import time
import ydb
import requests

from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadCacheMiss(WorkloadBase):
    def __init__(self, client, mon_endpoint, stop):
        super().__init__(client, "", "cache_miss", stop)
        self.mon_endpoint = mon_endpoint
        self.cache_misses = 0
        self.nonexistent_node_id = 300000
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Cache misses: {self.cache_misses}"

    def _cache_miss(self):
        url = f"{self.mon_endpoint}/viewer/json/sysinfo?node_id={self.nonexistent_node_id}"
        r = requests.get(url)
        r.raise_for_status()

    def _cache_miss_loop(self):
        while not self.is_stop_requested():
            self._cache_miss()
            with self.lock:
                self.cache_misses += 1

    def get_workload_thread_funcs(self):
        return [self._cache_miss_loop for x in range(0, 10)]


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


class WorkloadRunner:
    def __init__(self, client, mon_endpoint, duration):
        self.client = client
        self.mon_endpoint = mon_endpoint
        self.duration = duration
        ydb.interceptor.monkey_patch_event_handler()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self):
        stop = threading.Event()
        workloads = [
            WorkloadCacheMiss(self.client, self.mon_endpoint, stop),
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
