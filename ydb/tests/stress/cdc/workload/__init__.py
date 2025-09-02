# -*- coding: utf-8 -*-
import random
import threading
import time
import ydb

from enum import IntEnum, auto

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadRW(WorkloadBase):
    KEY_COUNT = 10

    def __init__(self, client, table_path, stop):
        super().__init__(client, "", "rw", stop)
        self.table_path = table_path
        self.queries = 0
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Queries: {self.queries}"

    def _loop(self, upsert_key, select_keys):
        while not self.is_stop_requested():
            select_keys_str = ','.join([str(x) for x in select_keys])
            random_value = random.randint(1, 1000)
            try:
                self.client.query(f"""
                    SELECT * FROM `{self.table_path}` WHERE `key` IN ({select_keys_str});
                    UPSERT INTO `{self.table_path}` (`key`, `value`) VALUES ({upsert_key}, {random_value});
                """, False)
                with self.lock:
                    self.queries += 1
            except ydb.Aborted:
                pass

    @staticmethod
    def _generate_select_keys(upsert_key, count):
        return [x for x in range(count) if x != upsert_key]

    def get_workload_thread_funcs(self):
        count = self.KEY_COUNT
        return [lambda x=x: self._loop(x, self._generate_select_keys(x, count)) for x in range(count)]


class WorkloadAlterTable(WorkloadBase):
    class State(IntEnum):
        ADD_COLUMN = auto()
        DROP_COLUMN = auto()

        @staticmethod
        def next(x):
            if x < WorkloadAlterTable.State.DROP_COLUMN:
                return WorkloadAlterTable.State(x + 1)
            else:
                return WorkloadAlterTable.State.ADD_COLUMN

    def __init__(self, client, table_path, stop):
        super().__init__(client, "", "alter_table", stop)
        self.table_path = table_path
        self.state = self.State.ADD_COLUMN
        self.altered = 0
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Altered: {self.altered} times"

    def _alter_table(self, state):
        if state == self.State.ADD_COLUMN:
            self.client.query(f"ALTER TABLE `{self.table_path}` ADD COLUMN `extra` Int32;", True)
        elif state == self.State.DROP_COLUMN:
            self.client.query(f"ALTER TABLE `{self.table_path}` DROP COLUMN `extra`;", True)

    def _alter_table_loop(self):
        while not self.is_stop_requested():
            self._alter_table(self.state)
            with self.lock:
                self.state = self.State.next(self.state)
                self.altered += 1

    def get_workload_thread_funcs(self):
        return [self._alter_table_loop]


class WorkloadRunner:
    def __init__(self, client, duration):
        self.client = client
        self.duration = duration
        self.table_path = '/'.join([self.client.database, "table", str(random.randint(100, 999))])
        ydb.interceptor.monkey_patch_event_handler()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self):
        self.client.query(f"""
            CREATE TABLE `{self.table_path}` (
                key Int32,
                value Int32,
                PRIMARY KEY (key)
            );
        """, True)
        self.client.query(f"""
            ALTER TABLE `{self.table_path}` ADD CHANGEFEED `updates` WITH (
                MODE = 'UPDATES',
                FORMAT = 'JSON'
            );
        """, True)

        stop = threading.Event()
        workloads = [
            WorkloadRW(self.client, self.table_path, stop),
            WorkloadAlterTable(self.client, self.table_path, stop),
        ]

        for w in workloads:
            w.start()
        started_at = time.time()
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
