# -*- coding: utf-8 -*-
import ydb
import time
import threading

from ydb.tests.stress.oltp_workload.workload.type.vector_index import WorkloadVectorIndex
from ydb.tests.stress.oltp_workload.workload.type.insert_delete_all_types import WorkloadInsertDeleteAllTypes
from ydb.tests.stress.oltp_workload.workload.type.select_partition import WorkloadSelectPartition

ydb.interceptor.monkey_patch_event_handler()


class WorkloadRunner:
    def __init__(self, client, path, duration):
        self.client = client
        self.name = path
        self.tables_prefix = "/".join([self.client.database, self.name])
        self.duration = duration
        ydb.interceptor.monkey_patch_event_handler()

    def __enter__(self):
        self._cleanup()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()

    def _cleanup(self):
        print(f"Cleaning up {self.tables_prefix}...")
        deleted = self.client.remove_recursively(self.tables_prefix)
        print(f"Cleaning up {self.tables_prefix}... done, {deleted} tables deleted")

    def run(self):
        stop = threading.Event()
        workloads = [
            WorkloadInsertDeleteAllTypes(self.client, self.name, stop),
            WorkloadVectorIndex(self.client, self.name, stop),
            WorkloadSelectPartition(self.client, self.name, stop)
        ]

        for w in workloads:
            w.start()
        started_at = started_at = time.time()
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
