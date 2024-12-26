# -*- coding: utf-8 -*-
import argparse
import ydb
import time
import os
import random
import threading

ydb.interceptor.monkey_patch_event_handler()


class YdbClient:
    def __init__(self, endpoint, database, use_query_service=False):
        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.database = database
        self.use_query_service = use_query_service
        self.session_pool = ydb.QuerySessionPool(self.driver) if use_query_service else ydb.SessionPool(self.driver)

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement, is_ddl):
        if self.use_query_service:
            return self.session_pool.execute_with_retries(statement)
        else:
            if is_ddl:
                return self.session_pool.retry_operation_sync(lambda session: session.execute_scheme(statement))
            else:
                raise "Unsuppported dml"  # TODO implement me

    def drop_table(self, path_to_table):
        if self.use_query_service:
            self.session_pool.execute_with_retries(f"DROP TABLE `{path_to_table}`")
        else:
            self.session_pool.retry_operation_sync(lambda session: session.drop_table(path_to_table))

    def describe(self, path):
        try:
            return self.driver.scheme_client.describe_path(path)
        except ydb.issues.SchemeError as e:
            if "Path not found" in e.message:
                return None
            raise e

    def _remove_recursively(self, path):
        deleted = 0
        d = self.driver.scheme_client.list_directory(path)
        for entry in d.children:
            entry_path = "/".join([path, entry.name])
            if entry.is_directory():
                deleted += self._remove_recursively(entry_path)
            elif entry.is_column_table() or entry.is_table():
                self.drop_table(entry_path)
                deleted += 1
            else:
                raise f"Scheme entry {entry_path} of unexpected type"
        self.driver.scheme_client.remove_directory(path)
        return deleted

    def remove_recursively(self, path):
        d = self.describe(path)
        if d is None:
            return
        if not d.is_directory():
            raise f"{path} has unexpected type"
        return self._remove_recursively(path)


class WorkloadBase:
    def __init__(self, client, tables_prefix, workload_name, stop):
        self.client = client
        self.table_prefix = tables_prefix + '/' + workload_name
        self.name = workload_name
        self.stop = stop
        self.workload_threads = []

    def name(self):
        return self.name

    def get_table_path(self, table_name):
        return "/".join([self.client.database, self.table_prefix, table_name])

    def is_stop_requested(self):
        return self.stop.is_set()

    def start(self):
        funcs = self.get_workload_thread_funcs()

        def wrapper(f):
            try:
                f()
            except Exception as e:
                print(f"FATAL: {e}")
                os._exit(1)

        for f in funcs:
            t = threading.Thread(target=lambda: wrapper(f))
            t.start()
            self.workload_threads.append(t)

    def join(self):
        for t in self.workload_threads:
            t.join()


class WorkloadTablesCreateDrop(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "create_drop", stop)
        self.created = 0
        self.deleted = 0
        self.tables = set()
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Created: {self.created}, Deleted: {self.deleted}, Exists: {len(self.tables)}"

    def _generate_new_table_n(self):
        while True:
            r = random.randint(1, 40000)
            with self.lock:
                if r not in self.tables:
                    return r

    def _get_existing_table_n(self):
        with self.lock:
            if len(self.tables) == 0:
                return None
            return next(iter(self.tables))

    def create_table(self, table):
        path = self.get_table_path(table)
        stmt = f"""
                CREATE TABLE `{path}` (
                id Int64 NOT NULL,
                i64Val Int64,
                PRIMARY KEY(id)
                )
                PARTITION BY HASH(id)
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
                self.tables.add(n)
                self.created += 1

    def _delete_tables_loop(self):
        while not self.is_stop_requested():
            n = self._get_existing_table_n()
            if n is None:
                print("create_drop: No tables to delete")
                time.sleep(10)
                continue
            self.client.drop_table(self.get_table_path(str(n)))
            with self.lock:
                self.tables.remove(n)
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
    def __init__(self, client, name, duration):
        self.client = client
        self.name = name
        self.tables_prefix = "/".join([self.client.database, self.name])
        self.duration = duration

    def __enter__(self):
        self._cleanup()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()

    def _cleanup(self):
        print(f"Cleaning up {self.tables_prefix}...")
        deleted = client.remove_recursively(self.tables_prefix)
        print(f"Cleaning up {self.tables_prefix}... done, {deleted} tables deleted")

    def run(self):
        stop = threading.Event()
        workloads = [
            WorkloadTablesCreateDrop(self.client, self.name, stop),
            # WorkloadInsertDelete(self.client, self.name, stop), TODO fix https://github.com/ydb-platform/ydb/issues/12871
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="olap stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default="Root/test", help="A database to connect")
    parser.add_argument("--path", default="olap_workload", help="A path prefix for tables")
    parser.add_argument("--duration", default=10 ** 9, type=lambda x: int(x), help="A duration of workload in seconds.")
    args = parser.parse_args()
    client = YdbClient(args.endpoint, args.database, True)
    client.wait_connection()
    with WorkloadRunner(client, args.path, args.duration) as runner:
        runner.run()
