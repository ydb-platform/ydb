import ydb
import time
import threading

from ydb.tests.stress.result_set_format.workload.type.schema_inclusion import WorkloadSchemaInclusion
from ydb.tests.stress.result_set_format.workload.type.data_types import WorkloadDataTypes
from ydb.tests.stress.result_set_format.workload.type.compression import WorkloadCompression
from ydb.tests.stress.result_set_format.workload.type.mixed import WorkloadMixed

ydb.interceptor.monkey_patch_event_handler()


class WorkloadRunner:
    def __init__(self, client, path, duration, format):
        self.client = client
        self.name = path
        self.tables_prefix = "/".join([self.client.database, self.name])
        self.duration = duration
        self.format = format

    def __enter__(self):
        self._cleanup()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()

    def _cleanup(self):
        print(f"Cleaning up {self.tables_prefix}...")
        deleted = self.client.remove_recursively(self.tables_prefix)
        if deleted is None:
            print(f"Cleaning up {self.tables_prefix}... done, no tables deleted")
        else:
            print(f"Cleaning up {self.tables_prefix}... done, {deleted} tables deleted")

    def run(self):
        stop = threading.Event()

        workloads = [
            WorkloadDataTypes(self.client, self.name, self.format, stop),
            WorkloadSchemaInclusion(self.client, self.name, self.format, stop),
            WorkloadCompression(self.client, self.name, self.format, stop),
            WorkloadMixed(self.client, self.name, self.format, stop),
        ]

        for w in workloads:
            w.start()

        started_at = time.time()
        while time.time() - started_at < self.duration:
            print(f"Elapsed {int(time.time() - started_at)} seconds, stat:")
            for w in workloads:
                print(f"\t{w.name}: {w.get_stat()}")
            time.sleep(10)

        stop.set()
        print("Waiting for stop...")
        for w in workloads:
            w.join()
        print("Stopped")
