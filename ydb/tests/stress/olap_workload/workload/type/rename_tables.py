# -*- coding: utf-8 -*-
import ydb
import random
import threading

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadRenameTables(WorkloadBase):
    def __init__(self, client, prefix, stop, count):
        super().__init__(client, prefix, "rename_tables", stop)
        self.tables_count = count
        self.renames = 0
        self.writes = 0
        self.lock = threading.Lock()
        for i in range(self.tables_count):
            table_path = self.get_table_path(f"tbl_{i}")
            self.client.query(
                f"""
                    CREATE TABLE `{table_path}` (
                    id Int64 NOT NULL,
                    str String,
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
                INSERT INTO `{table_path}` (`id`, `str`)
                VALUES (0, "This table was initially created as {table_path}")
            """,
                False,
            )

    def _get_table_name_range(self):
        return self.tables_count * 3

    def _rename_table_loop(self):
        while not self.is_stop_requested():
            rename_from = f"tbl_{random.randint(0, self._get_table_name_range())}"
            rename_to = f"tbl_{random.randint(0, self._get_table_name_range())}"
            print(f"Renaming table {rename_from} to {rename_to}")
            try:
                self.client.query(
                    f"""
                        ALTER TABLE `{self.get_table_path(rename_from)}` RENAME TO `{self.get_table_path(rename_to)}`
                        """,
                    True,
                    )
                with self.lock:
                    self.renames += 1
            except ydb.issues.GenericError as e:
                print(e)
            except ydb.issues.SchemeError as e:
                print(e)
            except ydb.issues.Overloaded as e:
                print(e)

    def _write_loop(self):
        while not self.is_stop_requested():
            table = f"tbl_{random.randint(0, self._get_table_name_range())}"
            print(f"Writing to table {table}")
            try:
                self.client.query(
                    f"""
                        $next = select max(id) from `{self.get_table_path(table)}`;
                        INSERT INTO `{self.get_table_path(table)}` (id, str)
                            VALUES(Unwrap($next) + 1, "This row was written to table {table}")
                            """,
                    True,
                    )
                with self.lock:
                    self.writes += 1
            except ydb.issues.BadRequest as e:
                print(e)
            except ydb.issues.SchemeError as e:
                print(e)
            except ydb.issues.PreconditionFailed as e:
                print(e)

    def get_stat(self):
        with self.lock:
            return f"Renames: {self.renames}, Writes: {self.writes}"

    def get_workload_thread_funcs(self):
        r = [self._rename_table_loop for x in range(0, 3)] + [self._write_loop for x in range(0, 3)]
        return r
