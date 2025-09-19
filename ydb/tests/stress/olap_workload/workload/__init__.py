# -*- coding: utf-8 -*-
import ydb
import time
import threading

from ydb.tests.stress.olap_workload.workload.type.tables_create_drop import WorkloadTablesCreateDrop
from ydb.tests.stress.olap_workload.workload.type.insert_delete import WorkloadInsertDelete
from ydb.tests.stress.olap_workload.workload.type.transactions import WorkloadTransactions
from ydb.tests.stress.olap_workload.workload.type.rename_tables import WorkloadRenameTables


class WorkloadRunner:
    def __init__(self, client, path, duration, allow_nullables_in_pk):
        self.client = client
        self.name = path
        self.tables_prefix = "/".join([self.client.database, self.name])
        self.duration = duration
        self.allow_nullables_in_pk = allow_nullables_in_pk
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
            WorkloadTablesCreateDrop(self.client, self.name, stop, self.allow_nullables_in_pk),
            WorkloadInsertDelete(self.client, self.name, stop),
            WorkloadTransactions(self.client, self.name, stop),
            WorkloadRenameTables(self.client, self.name, stop, 10),
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
        print("Waiting for stop... stopped")
