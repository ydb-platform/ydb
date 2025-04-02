# -*- coding: utf-8 -*-
import ydb
import time
import threading

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.datashard.lib.create_table import cleanup_type_name, pk_types, non_pk_types, null_types

ydb.interceptor.monkey_patch_event_handler()

digits = 2  # should be consistent with format string below


class WorkloadInsertDeleteAllTypes(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "insert_delete_all_types", stop)
        self.inserted = 0
        self.table_name = "table"
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Inserted: {self.inserted}"

    def _loop(self):
        table_path = self.get_table_path(self.table_name)

        create_sql = f"""
            CREATE TABLE `{table_path}` (
                pk Uint64,
                {", ".join(["pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in pk_types.keys()])},
                {", ".join(["null_pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in null_types.keys()])},
                {", ".join(["col_" + cleanup_type_name(type_name) + " " + type_name for type_name in non_pk_types.keys()])},
                {", ".join(["null_col_" + cleanup_type_name(type_name) + " " + type_name for type_name in null_types.keys()])},
                PRIMARY KEY(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                {", ".join(["null_pk_" + cleanup_type_name(type_name) for type_name in null_types.keys()])}
                )
            )
        """

        self.client.query(create_sql, True,)
        inflight = 10
        i = 0
        sum = 0
        while not self.is_stop_requested():
            value = i % 100
            insert_sql = f"""
                INSERT INTO `{table_path}` (
                pk,
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                {", ".join(["null_pk_" + cleanup_type_name(type_name) for type_name in null_types.keys()])},
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in non_pk_types.keys()])},
                {", ".join(["null_col_" + cleanup_type_name(type_name) for type_name in null_types.keys()])}
                )
                VALUES
                (
                {i},
                {", ".join([pk_types[type_name].format(value) for type_name in pk_types.keys()])},
                {", ".join(['NULL' for type_name in null_types.keys()])},
                {", ".join([non_pk_types[type_name].format(value) for type_name in non_pk_types.keys()])},
                {", ".join(['NULL' for type_name in null_types.keys()])}
                )
                ;
            """
            self.client.query(insert_sql, False,)
            sum += i

            if (i >= inflight):
                self.client.query(
                    f"""
                    DELETE FROM `{table_path}`
                    WHERE pk == {i - inflight} AND null_pk_Int64 IS NULL
                """,
                    False,
                )
                sum -= (i - inflight)

                actual = self.client.query(
                    f"""
                    SELECT COUNT(*) as cnt, SUM(pk) as sum FROM `{table_path}`
                """,
                    False,
                )[0].rows[0]
                expected = {"cnt": inflight, "sum": sum}
                if actual != expected:
                    raise Exception(
                        f"Incorrect result: expected:{expected}, actual:{actual}")
            i += 1
            with self.lock:
                self.inserted += 1

    def get_workload_thread_funcs(self):
        return [self._loop]


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
        print(
            f"Cleaning up {self.tables_prefix}... done, {deleted} tables deleted")

    def run(self):
        stop = threading.Event()

        workloads = [
            WorkloadInsertDeleteAllTypes(self.client, self.name, stop),
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
