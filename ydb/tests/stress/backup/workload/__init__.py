import random
import threading
import time
import ydb
import uuid

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadRW(WorkloadBase):
    KEY_COUNT = 10

    def __init__(self, client, table_path, stop):
        super().__init__(client, "", "rw", stop)
        self.table_path = table_path
        self.queries = 0
        self.lock = threading.Lock()
        self._threads = []

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


class WorkloadRunnerBackup:
    def __init__(self, client, duration, backup_interval):
        self.client = client
        self.duration = duration
        self.backup_interval = backup_interval
        uid = uuid.uuid4().hex
        self.table_path = f"{self.client.database}/table/{uid}"
        self.collection_name = f"backup_col_{uid}"
        ydb.interceptor.monkey_patch_event_handler()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _create_table(self):
        self.client.query(f"""
            CREATE TABLE `{self.table_path}` (
                key Int32,
                value Int32,
                PRIMARY KEY (key)
            );
        """, True)

    def _create_backup_collection(self):
        self.client.query(f"""
            CREATE BACKUP COLLECTION `{self.collection_name}` ( TABLE `{self.table_path}` ) WITH (
                STORAGE = "cluster",
                INCREMENTAL_BACKUP_ENABLED = "true"
            );
        """, True)

    def _do_full_backup(self):
        self.client.query(f"BACKUP `{self.collection_name}`;", True)

    def _do_incremental_backup(self):
        self.client.query(f"BACKUP `{self.collection_name}` INCREMENTAL;", True)

    def run(self):
        self._create_table()
        self._create_backup_collection()
        print(f"Created table {self.table_path} and backup collection {self.collection_name}")

        self._do_full_backup()

        stop = threading.Event()
        rw = WorkloadRW(self.client, self.table_path, stop)

        for f in rw.get_workload_thread_funcs():
            t = threading.Thread(target=f)
            t.daemon = True
            t.start()
            rw._threads.append(t)

        def backup_loop():
            while not stop.is_set():
                time.sleep(self.backup_interval)
                print("Triggering incremental backup...")
                self._do_incremental_backup()

        bthread = threading.Thread(target=backup_loop)
        bthread.daemon = True
        bthread.start()

        started_at = time.time()
        try:
            while time.time() - started_at < self.duration:
                print(f"Elapsed {(int)(time.time() - started_at)} seconds, stat:")
                print(f"\t{rw.name}: {rw.get_stat()}")
                time.sleep(10)
        finally:
            stop.set()
            print("Waiting for workers to stop...")
            for t in rw._threads:
                t.join(timeout=5)
            bthread.join(timeout=5)
            print("Stopped")
