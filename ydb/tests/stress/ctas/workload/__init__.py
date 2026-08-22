# -*- coding: utf-8 -*-
import ydb
import time
import threading

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadCtas(WorkloadBase):
    def __init__(self, client, prefix, stop, is_olap):
        super().__init__(client, prefix, "ctas", stop)
        self.tables_count = 0
        self.total_rows = 0
        self.is_olap = is_olap
        self.table_name = "table_olap" if is_olap else "table_oltp"
        self.lock = threading.Lock()

        self.value_bytes = 1024
        self.prev_rows = (1, 0)

    def get_stat(self):
        with self.lock:
            return f"Tables: {self.tables_count}, TotalRows: {self.total_rows}, LastTableRows: {self.prev_rows[0]}"

    def get_create_query(self):
        table_path = self.get_table_path(self.table_name + str(self.tables_count))

        params = None
        if self.is_olap:
            params = "STORE = COLUMN"
        else:
            params = "STORE = ROW"

        if self.tables_count == 0:
            return f"""
                CREATE TABLE `{table_path}` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY(Key)
                ) WITH (
                    {params}
                );
            """

        return f"""
            CREATE TABLE `{table_path}` (
                PRIMARY KEY(Key)
            )
            WITH (
                {params}
            ) AS SELECT
                Unwrap(CAST(1 AS Uint64)) AS Key, "{('0' * self.value_bytes)}" AS Value;
        """

    def get_ctas_query(self):
        table_path = self.get_table_path(self.table_name + str(self.tables_count))
        prev_table_path1 = self.get_table_path(self.table_name + str(self.tables_count - 1))
        prev_table_path2 = self.get_table_path(self.table_name + str(self.tables_count - 2))

        return f"""
            CREATE TABLE `{table_path}` (PRIMARY KEY(Key))
            AS SELECT
                Key AS Key, Value
            FROM `{prev_table_path1}`
            UNION ALL
            SELECT
                Key + Unwrap(CAST({self.prev_rows[0]} AS Uint64)) AS Key, Value
            FROM `{prev_table_path2}`
        """

    def prepare(self):
        create_query = self.get_create_query()
        self.client.query(
            create_query,
            True,
        )
        self.tables_count += 1
        create_query = self.get_create_query()
        self.client.query(
            create_query,
            True,
        )
        self.tables_count += 1

    def _loop(self):
        self.prepare()

        while not self.is_stop_requested():
            ctas_query = self.get_ctas_query()
            self.client.query(
                ctas_query,
                True,
            )

            table_path = self.get_table_path(self.table_name + str(self.tables_count))
            actual = self.client.query(
                f"""
                SELECT COUNT(*) as cnt FROM `{table_path}`
            """,
                False,
            )[0].rows[0]['cnt']
            expected = self.prev_rows[0] + self.prev_rows[1]
            if actual != expected:
                raise Exception(f"Incorrect result: expected:{expected}, actual:{actual}")

            self.prev_rows = (actual, self.prev_rows[0])
            self.total_rows += actual
            self.tables_count += 1

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
            WorkloadCtas(self.client, self.name, stop, True),
            WorkloadCtas(self.client, self.name, stop, False),
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
