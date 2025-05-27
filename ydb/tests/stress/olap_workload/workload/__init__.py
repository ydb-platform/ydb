# -*- coding: utf-8 -*-
import ydb
import time
import random
import threading
from enum import Enum

from ydb.tests.stress.common.common import WorkloadBase

supported_pk_types = [
    # Bool https://github.com/ydb-platform/ydb/issues/13037
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Uint8",
    "Uint16",
    "Uint32",
    "Uint64",
    "Decimal(22,9)",
    # "DyNumber", https://github.com/ydb-platform/ydb/issues/13048

    "String",
    "Utf8",
    # Uuid", https://github.com/ydb-platform/ydb/issues/13047

    "Date",
    "Datetime",
    "Datetime64",
    "Timestamp",
    # "Interval", https://github.com/ydb-platform/ydb/issues/13050
]

supported_types = supported_pk_types + [
    "Float",
    "Double",
    "Json",
    "JsonDocument",
    "Yson"
]


class WorkloadTablesCreateDrop(WorkloadBase):
    class TableStatus(Enum):
        CREATING = "Creating",
        AVAILABLE = "Available",
        DELITING = "Deleting"

    def __init__(self, client, prefix, stop, allow_nullables_in_pk):
        super().__init__(client, prefix, "create_drop", stop)
        self.allow_nullables_in_pk = allow_nullables_in_pk
        self.created = 0
        self.deleted = 0
        self.tables = {}
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Created: {self.created}, Deleted: {self.deleted}, Exists: {len(self.tables)}"

    def _generate_new_table_n(self):
        while True:
            r = random.randint(1, 40000)
            with self.lock:
                if r not in self.tables:
                    self.tables[r] = WorkloadTablesCreateDrop.TableStatus.CREATING
                    return r

    def _get_table_to_delete(self):
        with self.lock:
            for n, s in self.tables.items():
                if s == WorkloadTablesCreateDrop.TableStatus.AVAILABLE:
                    self.tables[n] = WorkloadTablesCreateDrop.TableStatus.DELITING
                    return n
        return None

    def create_table(self, table):
        path = self.get_table_path(table)
        column_n = random.randint(1, 10000)
        primary_key_column_n = random.randint(1, column_n)
        partition_key_column_n = random.randint(1, primary_key_column_n)
        column_defs = []
        for i in range(column_n):
            if i < primary_key_column_n:
                c = random.choice(supported_pk_types)
                if not self.allow_nullables_in_pk or random.choice([False, True]):
                    c += " NOT NULL"
            else:
                c = random.choice(supported_types)
                if random.choice([False, True]):
                    c += " NOT NULL"
            column_defs.append(c)

        stmt = f"""
                CREATE TABLE `{path}` (
                    {", ".join(["c" + str(i) + " " + column_defs[i] for i in range(column_n)])},
                    PRIMARY KEY({", ".join(["c" + str(i) for i in range(primary_key_column_n)])})
                )
                PARTITION BY HASH({", ".join(["c" + str(i) for i in range(partition_key_column_n)])})
                WITH (
                    STORE = COLUMN
                )
            """
        self.client.query(stmt, True)

    def _create_tables_loop(self):
        while not self.is_stop_requested():
            n = self._generate_new_table_n()
            self.create_table(str(n))
            with self.lock:
                self.tables[n] = WorkloadTablesCreateDrop.TableStatus.AVAILABLE
                self.created += 1

    def _delete_tables_loop(self):
        while not self.is_stop_requested():
            n = self._get_table_to_delete()
            if n is None:
                print("create_drop: No tables to delete")
                time.sleep(10)
                continue
            self.client.drop_table(self.get_table_path(str(n)))
            with self.lock:
                del self.tables[n]
                self.deleted += 1

    def get_workload_thread_funcs(self):
        r = [self._create_tables_loop for x in range(0, 10)]
        r.append(self._delete_tables_loop)
        return r


class WorkloadInsertDelete(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "insert_delete", stop)
        self.inserted = 0
        self.current = 0
        self.table_name = "table"
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Inserted: {self.inserted}, Current: {self.current}"

    def _loop(self):
        table_path = self.get_table_path(self.table_name)
        self.client.query(
            f"""
                CREATE TABLE `{table_path}` (
                id Int64 NOT NULL,
                i64Val Int64,
                PRIMARY KEY(id)
                )
                PARTITION BY HASH(id)
                WITH (
                    STORE = COLUMN
                )
        """,
            True,
        )
        i = 1
        while not self.is_stop_requested():
            self.client.query(
                f"""
                INSERT INTO `{table_path}` (`id`, `i64Val`)
                VALUES
                ({i * 2}, {i * 10}),
                ({i * 2 + 1}, {i * 10 + 1})
            """,
                False,
            )

            self.client.query(
                f"""
                DELETE FROM `{table_path}`
                WHERE i64Val % 2 == 1
            """,
                False,
            )

            actual = self.client.query(
                f"""
                SELECT COUNT(*) as cnt, SUM(i64Val) as vals, SUM(id) as ids FROM `{table_path}`
            """,
                False,
            )[0].rows[0]
            expected = {"cnt": i, "vals": i * (i + 1) * 5, "ids": i * (i + 1)}
            if actual != expected:
                raise Exception(f"Incorrect result: expected:{expected}, actual:{actual}")
            i += 1
            with self.lock:
                self.inserted += 2
                self.current = actual["cnt"]

    def get_workload_thread_funcs(self):
        return [self._loop]


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
