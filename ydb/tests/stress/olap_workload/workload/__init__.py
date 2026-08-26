# -*- coding: utf-8 -*-
import ydb
import time
import threading

from ydb.tests.stress.olap_workload.workload.type.tables_create_drop import WorkloadTablesCreateDrop
from ydb.tests.stress.olap_workload.workload.type.insert_delete import WorkloadInsertDelete
from ydb.tests.stress.olap_workload.workload.type.transactions import WorkloadTransactions
from ydb.tests.stress.olap_workload.workload.type.rename_tables import WorkloadRenameTables
from ydb.tests.stress.olap_workload.workload.type.encodings import WorkloadEncodings
from ydb.tests.stress.olap_workload.workload.type.move_data import WorkloadMoveData


class WorkloadRunner:
    def __init__(self, client, path, duration, allow_nullables_in_pk, endpoint=None):
        self.client = client
        self.endpoint = endpoint
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
        # Workloads that restart tablets or decommission storage (move_data,
        # cut_history) can still have restarts landing when the run ends; a plain
        # remove then dies on transient Unavailable ("Connection to tablet was
        # lost"), which failed release-asan three retries in a row.
        deadline = time.time() + 120
        while True:
            try:
                deleted = self.client.remove_recursively(self.tables_prefix)
                break
            except (ydb.issues.Unavailable, ydb.issues.BadSession, ydb.issues.ConnectionError) as e:
                if time.time() >= deadline:
                    raise
                # e.__class__, not type(e): importing workload.type.* binds `type` as an
                # attribute of this package, shadowing the builtin inside __init__.py.
                print(f"Cleaning up {self.tables_prefix}: transient {e.__class__.__name__}, retrying...")
                time.sleep(3)
        print(f"Cleaning up {self.tables_prefix}... done, {deleted} tables deleted")

    def run(self):
        stop = threading.Event()
        workloads = [
            WorkloadTablesCreateDrop(self.client, self.name, stop, self.allow_nullables_in_pk),
            WorkloadInsertDelete(self.client, self.name, stop),
            WorkloadTransactions(self.client, self.name, stop),
            WorkloadRenameTables(self.client, self.name, stop, 10),
            WorkloadEncodings(self.client, self.name, stop),
        ]
        # Pool shrink/grow needs the console endpoint, so it is only enabled when
        # the caller supplied one.
        if self.endpoint:
            workloads.append(WorkloadMoveData(self.client, self.name, stop, self.endpoint, self.client.database))
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
