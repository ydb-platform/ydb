# -*- coding: utf-8 -*-
import time
import random
import threading
from enum import Enum

from ydb.tests.datashard.lib.types_of_variables import non_pk_types, pk_types, types_not_supported_yet_in_columnshard
from ydb.tests.stress.common.common import WorkloadBase

supported_pk_types = list(pk_types.keys() - types_not_supported_yet_in_columnshard)

supported_types = list((pk_types.keys() | non_pk_types.keys()) - types_not_supported_yet_in_columnshard)


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
