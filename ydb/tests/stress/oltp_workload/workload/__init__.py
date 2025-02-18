# -*- coding: utf-8 -*-
import ydb
import time
import threading

from ydb.tests.stress.common.common import WorkloadBase

ydb.interceptor.monkey_patch_event_handler()

digits = 2  # should be consistent with format string below
pk_types = {
    "Int64": "CAST({} AS Int64)",
    "Uint64": "CAST({} AS Uint64)",
    "Int32": "CAST({} AS Int32)",
    "Uint32": "CAST({} AS Uint32)",
    "Int16": "CAST({} AS Int16)",
    "Uint16": "CAST({} AS Uint16)",
    "Int8": "CAST({} AS Int8)",
    "Uint8": "CAST({} AS Uint8)",
    "Bool": "CAST({} AS Bool)",
    "Decimal(15,0)": "CAST('{}.0' AS Decimal(15,0))",
    "Decimal(22,9)": "CAST('{}.123' AS Decimal(22,9))",
    "Decimal(35,10)": "CAST('{}.123456' AS Decimal(35,10))",
    "DyNumber": "CAST('{}E1' AS DyNumber)",

    "String": "'String {}'",
    "Utf8": "'Uft8 {}'",
    "Uuid": "CAST('{:2}345678-e89b-12d3-a456-556642440000' AS UUID)",

    "Date": "CAST('20{:02}-01-01' AS Date)",
    "Datetime": "CAST('20{:02}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(169624{:02}00000000 AS Timestamp)",
    "Interval": "CAST({} AS Interval)",
    "Date32": "CAST('20{:02}-01-01' AS Date32)",
    "Datetime64": "CAST('20{:02}-10-02T11:00:00Z' AS Datetime64)",
    "Timestamp64": "CAST(169624{:02}00000000 AS Timestamp64)",
    "Interval64": "CAST({} AS Interval64)"
}

non_pk_types = {
    "Float": "CAST('{}.1' AS Float)",
    "Double": "CAST('{}.2' AS Double)",
    "Json": "CAST('{{\"another_key\":{}}}' AS Json)",
    "JsonDocument": "CAST('{{\"another_doc_key\":{}}}' AS JsonDocument)",
    "Yson": "CAST('[{}]' AS Yson)"
}

null_types = {
    "Int64": "CAST({} AS Int64)",
    "Decimal(22,9)": "CAST('{}.123' AS Decimal(22,9))",
    "Decimal(35,10)": "CAST('{}.123456' AS Decimal(35,10))",
    "String": "'{}'",
}

def cleanup_type_name(type_name):
    return type_name.replace('(', '').replace(')', '').replace(',', '')

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
                    raise Exception(f"Incorrect result: expected:{expected}, actual:{actual}")
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
        print(f"Cleaning up {self.tables_prefix}... done, {deleted} tables deleted")

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
