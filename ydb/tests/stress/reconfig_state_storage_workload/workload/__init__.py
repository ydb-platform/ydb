# -*- coding: utf-8 -*-
import ydb
import time
import random
import threading
import requests
import logging
from enum import Enum

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient

logger = logging.getLogger(__name__)


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
        CREATING = "Creating"
        AVAILABLE = "Available"
        DELETING = "Deleting"

    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "create_drop", stop)
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
                    self.tables[n] = WorkloadTablesCreateDrop.TableStatus.DELETING
                    return n
        return None

    def create_table(self, table):
        path = self.get_table_path(table)
        column_n = random.randint(1, 10)
        primary_key_column_n = random.randint(1, column_n)
        column_defs = []
        for i in range(column_n):
            if i < primary_key_column_n:
                c = random.choice(supported_pk_types)
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
            """
        self.client.query(stmt, True)

    def _create_tables_loop(self):
        logger.debug("starting")
        while not self.is_stop_requested():
            n = self._generate_new_table_n()
            self.create_table(str(n))
            with self.lock:
                self.tables[n] = WorkloadTablesCreateDrop.TableStatus.AVAILABLE
                self.created += 1
                logger.debug(f"iteration {self.created}")
        with self.lock:
            logger.debug(f"exiting after {self.created} iterations")

    def _delete_tables_loop(self):
        logger.debug("starting")
        while not self.is_stop_requested():
            n = self._get_table_to_delete()
            if n is None:
                logger.info("create_drop: No tables to delete")
                time.sleep(5)
                continue
            self.client.drop_table(self.get_table_path(str(n)))
            with self.lock:
                del self.tables[n]
                self.deleted += 1
                logger.debug(f"iteration {self.deleted}")
        with self.lock:
            logger.debug(f"exiting after {self.deleted} iterations")

    def get_workload_thread_funcs(self):
        return [self._create_tables_loop] * 3 + [self._delete_tables_loop] * 2


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
        logger.debug("starting")
        table_path = self.get_table_path(self.table_name)
        self.client.query(
            f"""
                CREATE TABLE `{table_path}` (
                id Int64 NOT NULL,
                i64Val Int64,
                PRIMARY KEY(id)
                )
        """,
            True,
        )
        i = 1
        while not self.is_stop_requested():
            logger.debug(f"iteration {i}")
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
        logger.debug(f"exiting after {i} iterations")

    def get_workload_thread_funcs(self):
        return [self._loop]


class WorkloadReconfigStateStorage(WorkloadBase):
    config_name = "StateStorage"
    loop_cnt = 0
    wait_for = 1

    def __init__(self, client, http_endpoint, prefix, stop, config_name):
        super().__init__(client, prefix, "reconfig_statestorage", stop)
        self.ringGroupActorIdOffset = 1
        self.http_endpoint = http_endpoint
        self.lock = threading.Lock()
        self.config_name = config_name

    def get_stat(self):
        with self.lock:
            return f"Reconfig: {self.loop_cnt}"

    def do_request(self, json_req):
        url = f'{self.http_endpoint}/actors/nodewarden?page=distconf'
        return requests.post(url, headers={'content-type': 'application/json'}, json=json_req).json()

    def do_request_config(self):
        res = self.do_request({"GetStateStorageConfig": {}})["StateStorageConfig"]
        logger.info(f"Config response: {res}")
        return res

    def _loop(self):
        logger.debug("starting")
        while not self.is_stop_requested():
            time.sleep(self.wait_for)
            cfg = self.do_request_config()[f"{self.config_name}Config"]
            defaultRingGroup = [cfg["Ring"]] if "Ring" in cfg else cfg["RingGroups"]
            newRingGroup = [
                {"RingGroupActorIdOffset": self.ringGroupActorIdOffset, "NToSelect": 3, "Ring": [{"Node": [4]}, {"Node": [5]}, {"Node": [6]}]},
                {"RingGroupActorIdOffset": self.ringGroupActorIdOffset, "NToSelect": 3, "Ring": [{"Node": [1]}, {"Node": [2]}, {"Node": [3]}]}
                ]
            logger.info(f"From: {defaultRingGroup} To: {newRingGroup}")
            for i in range(len(newRingGroup)):
                newRingGroup[i]["WriteOnly"] = True
            logger.info(self.do_request({"ReconfigStateStorage": {f"{self.config_name}Config": {
                "RingGroups": defaultRingGroup + newRingGroup}}}))
            time.sleep(self.wait_for)
            for i in range(len(newRingGroup)):
                newRingGroup[i]["WriteOnly"] = False
            logger.info(self.do_request({"ReconfigStateStorage": {f"{self.config_name}Config": {
                "RingGroups": defaultRingGroup + newRingGroup}}}))
            time.sleep(self.wait_for)
            for i in range(len(defaultRingGroup)):
                defaultRingGroup[i]["WriteOnly"] = True
            logger.info(self.do_request({"ReconfigStateStorage": {f"{self.config_name}Config": {
                "RingGroups": newRingGroup + defaultRingGroup}}}))
            time.sleep(self.wait_for)
            logger.info(self.do_request({"ReconfigStateStorage": {f"{self.config_name}Config": {
                "RingGroups": newRingGroup}}}))
            time.sleep(self.wait_for)
            curConfig = self.do_request_config()[f"{self.config_name}Config"]
            expectedConfig = {"Ring": newRingGroup[0]} if len(newRingGroup) == 1 else {"RingGroups": newRingGroup}
            if curConfig != expectedConfig:
                raise Exception(f"Incorrect reconfig: actual:{curConfig}, expected:{expectedConfig}")
            self.ringGroupActorIdOffset += 1
            with self.lock:
                logger.info(f"Reconfig {self.loop_cnt} finished")
                self.loop_cnt += 1
                logger.debug(f"iteration {self.loop_cnt}")
        with self.lock:
            logger.debug(f"exiting after {self.loop_cnt} iterations")

    def get_workload_thread_funcs(self):
        return [self._loop]


class WorkloadDiscovery(WorkloadBase):

    def __init__(self, client, grpc_endpoint, prefix, stop):
        super().__init__(client, prefix, "discovery", stop)
        self.grpc_endpoint = grpc_endpoint
        self.lock = threading.Lock()
        self.cnt = 0

    def get_stat(self):
        with self.lock:
            return f"Discovery: {self.cnt}"

    def _loop(self):
        logger.debug("starting")
        driver_config = ydb.DriverConfig(self.grpc_endpoint, self.client.database)
        while not self.is_stop_requested():
            time.sleep(3)
            resolver = ydb.DiscoveryEndpointsResolver(driver_config)
            result = resolver.resolve()
            if result is None:
                logger.info("Discovery empty")
            else:
                logger.info(f"Len = {len(result.endpoints)} Endpoints: {result.endpoints}")
            with self.lock:
                self.cnt += 1
                logger.debug(f"iteration {self.cnt}")
        with self.lock:
            logger.debug(f"exiting after {self.cnt} iterations")

    def get_workload_thread_funcs(self):
        return [self._loop]


class WorkloadRunner:
    config_name = "StateStorage"

    def __init__(self, grpc_endpoint, http_endpoint, database, path, duration, config_name):
        self.client = InstrumentedYdbClient(grpc_endpoint, database, True)
        self.client.wait_connection()
        self.grpc_endpoint = grpc_endpoint
        self.http_endpoint = http_endpoint
        self.name = path
        self.tables_prefix = "/".join([self.client.database, self.name])
        self.duration = duration
        self.config_name = config_name
        ydb.interceptor.monkey_patch_event_handler()

    def __enter__(self):
        self._cleanup()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()
        self.client.close()

    def _cleanup(self):
        logger.info(f"Cleaning up {self.tables_prefix}...")
        deleted = self.client.remove_recursively(self.tables_prefix)
        logger.info(f"Cleaning up {self.tables_prefix}... done, {deleted} tables deleted")

    def run(self):
        stop = threading.Event()

        reconfigWorkload = WorkloadReconfigStateStorage(self.client, self.http_endpoint, self.name, stop, self.config_name)
        workloads = [
            WorkloadTablesCreateDrop(self.client, self.name, stop),
            WorkloadInsertDelete(self.client, self.name, stop),
            reconfigWorkload,
            WorkloadDiscovery(self.client, self.grpc_endpoint, self.name, stop)
        ]
        for w in workloads:
            w.start()
        started_at = time.time()
        while time.time() - started_at < self.duration:
            logger.info(f"Elapsed {(int)(time.time() - started_at)} seconds, stat:")
            for w in workloads:
                logger.info(f"\t{w.name}: {w.get_stat()}")
            time.sleep(5)
        stop.set()
        logger.info("Waiting for stop...")
        failed_to_stop = []
        for w in workloads:
            logger.debug(f"Waiting for {w.name} to stop...")
            w.join(timeout=30)
            if w.is_alive():
                logger.error(f"Workload {w.name} failed to stop within 30 seconds!")
                failed_to_stop.append(w.name)
            else:
                logger.debug(f"{w.name} stopped")

        if failed_to_stop:
            raise Exception(f"The following workloads failed to stop: {failed_to_stop}")
        logger.info("Waiting for stop... stopped")
        return reconfigWorkload.loop_cnt
