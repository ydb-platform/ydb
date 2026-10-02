# -*- coding: utf-8 -*-
import ydb
import threading

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadTransactions(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "transactions", stop)
        self.inserts = 0
        self.table_path = self.get_table_path("table")
        self.lock = threading.Lock()
        self.client.query(
            f"""
                CREATE TABLE `{self.table_path}` (
                id Int64 NOT NULL,
                str Utf8,
                PRIMARY KEY(id)
                )
                PARTITION BY HASH(id)
                WITH (
                    STORE = COLUMN
                )
        """,
            True,
        )
        self.client.query(
            f"""
                INSERT INTO `{self.table_path}` (`id`, `str`)
                VALUES (0, "This table was initially created")
            """,
            False,
            )

    def _loop(self, i):
        while not self.is_stop_requested():
            try:
                print(f"Inserting a row by thread {i}")
                self.client.query(
                    f"""
                        $next = select max(id) from `{self.table_path}`;
                        INSERT INTO `{self.table_path}` (id, str)
                            VALUES(Unwrap($next) + 1, "This row was inserted by thread {i}")
                            """,
                    True,
                    )
                with self.lock:
                    self.inserts += 1
            except ydb.issues.Aborted as e:  # TLI is expected on parallel insertions
                if not e.message.startswith('message: "Transaction locks invalidated'):
                    raise e
            except ydb.issues.PreconditionFailed:  # TODO fix me https://github.com/ydb-platform/ydb/issues/23219
                pass

    def _loop_wrapper(self, i):
        return lambda: self._loop(i)

    def get_stat(self):
        with self.lock:
            return f"Inserted: {self.inserts}"

    def get_workload_thread_funcs(self):
        return [self._loop_wrapper(i) for i in range(5)]
