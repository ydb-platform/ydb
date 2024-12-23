# -*- coding: utf-8 -*-
import argparse
import ydb
import time
import os
import random
import string
import threading

ydb.interceptor.monkey_patch_event_handler()


def timestamp():
    return int(1000 * time.time())


def table_name_with_timestamp():
    return os.path.join("column_table_" + str(timestamp()))


def random_string(length):
    letters = string.ascii_lowercase
    return bytes(''.join(random.choice(letters) for i in range(length)), encoding='utf8')


def random_type():
    return random.choice([ydb.PrimitiveType.Int64, ydb.PrimitiveType.String])


def random_value(type):
    if isinstance(type, ydb.OptionalType):
        return random_value(type.item)
    if type == ydb.PrimitiveType.Int64:
        return random.randint(0, 1 << 31)
    if type == ydb.PrimitiveType.String:
        return random_string(random.randint(1, 32))


class YdbClient:
    def __init__(self, enpoint, database, use_query_service=False):
        self.driver = ydb.Driver(endpoint=args.endpoint, database=args.database, oauth=None)
        self.database = database
        self.session_pool = ydb.QuerySessionPool(self.driver) if use_query_service else ydb.SessionPool(self.driver)

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def use_query_service(self):
        return isinstance(self.session_pool, ydb.QuerySessionPool)

    def query(self, statement, ddl):
        if self.use_query_service():
            return self.session_pool.execute_with_retries(statement)
        else:
            if ddl:
                return self.session_pool.retry_operation_sync(lambda session: session.execute_scheme(statement))
            else:
                raise "Unsuppported dml"

    def drop_table(self, path_to_table):
        if self.use_query_service():
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
        self.create_workload_threads()
        for t in self.workload_threads:
            t.start()

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
            return next(iter(self.tables))  # random.choice(tables)

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

    def create_workload_threads(self):
        for _ in range(0, 10):
            self.workload_threads.append(threading.Thread(target=self._create_tables_loop))
        self.workload_threads.append(threading.Thread(target=self._delete_tables_loop))


class WorkloadInsertDelete(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "insert_delete", stop)
        self.inserted = 0
        self.current = 0
        self.table_name = "table"
        self.lock = threading.Lock()
        self.workload_thread = None

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
                select count(*) as cnt, sum(i64Val) as vals, sum(id) as ids FROM `{table_path}`
            """,
                False,
            )[0].rows[0]
            expected = {"cnt": i, "vals": i * (i + 1) * 5, "ids": i * (i + 1)}
            if actual != expected:
                raise f"Incorrect result: expected:{expected}, actual:{actual}"
            i += 1
            with self.lock:
                self.inserted += 2
                self.current = actual["cnt"]

    def create_workload_threads(self):
        self.workload_threads = [threading.Thread(target=self._loop)]


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="olap stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='localhost:2136', help="An endpoint to be used")
    parser.add_argument('--database', default='Root/test', help='A database to connect')
    parser.add_argument('--duration', default=10, type=lambda x: int(x), help='A duration of workload in seconds.')
    args = parser.parse_args()
    client = YdbClient(args.endpoint, args.database, True)
    client.wait_connection()
    with WorkloadRunner(client, "olap_workload", args.duration) as runner:
        runner.run()
