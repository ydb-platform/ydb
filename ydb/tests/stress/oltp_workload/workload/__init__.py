# -*- coding: utf-8 -*-
import ydb
import time
import threading

from ydb.tests.stress.oltp_workload.workload.type.fulltext_index import WorkloadFulltextIndex
from ydb.tests.stress.oltp_workload.workload.type.vector_index import WorkloadVectorIndex
from ydb.tests.stress.oltp_workload.workload.type.json_index import WorkloadJsonIndex
from ydb.tests.stress.oltp_workload.workload.type.insert_delete_all_types import WorkloadInsertDeleteAllTypes
from ydb.tests.stress.oltp_workload.workload.type.select_partition import WorkloadSelectPartition
from ydb.tests.stress.oltp_workload.workload.type.secondary_index import WorkloadSecondaryIndex
from ydb.tests.stress.oltp_workload.workload.type.bloom_filter_index import WorkloadBloomFilterIndex
from ydb.tests.stress.oltp_workload.workload.type.tli import WorkloadTli
from ydb.tests.stress.oltp_workload.workload.type.combined_indexes import WorkloadCombinedIndexes

ydb.interceptor.monkey_patch_event_handler()


class WorkloadRunner:
    def __init__(self, client, path, duration, seed=None):
        self.client = client
        self.name = path
        self.tables_prefix = "/".join([self.client.database, self.name])
        self.duration = duration
        self.seed = seed
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

    def run(self, enabled_workloads=None, disabled_workloads=None):
        stop = threading.Event()
        workloads = [
            WorkloadInsertDeleteAllTypes(self.client, self.name, stop),
            WorkloadFulltextIndex(self.client, self.name, stop, seed=self.seed),
            WorkloadVectorIndex(self.client, self.name, stop),
            WorkloadJsonIndex(self.client, self.name, stop, seed=self.seed),
            WorkloadSelectPartition(self.client, self.name, stop),
            WorkloadSecondaryIndex(self.client, self.name, stop),
            WorkloadBloomFilterIndex(self.client, self.name, stop),
            WorkloadTli(self.client, self.name, stop),
            WorkloadCombinedIndexes(self.client, self.name, stop, seed=self.seed),
        ]

        if enabled_workloads is not None:
            workloads = [w for w in workloads if w.name in enabled_workloads]
        if disabled_workloads is not None:
            workloads = [w for w in workloads if w.name not in disabled_workloads]

        print(
            f"Starting workloads duration={self.duration}s seed={self.seed!r} "
            f"names={[w.name for w in workloads]}"
        )

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
