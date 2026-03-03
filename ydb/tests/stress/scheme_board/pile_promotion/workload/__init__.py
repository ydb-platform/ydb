# -*- coding: utf-8 -*-
import ydb
import time
import threading
import logging
from enum import Enum
from urllib.parse import urlparse

from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.clients.kikimr_bridge_client import BridgeClient
from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient
import ydb.public.api.protos.draft.ydb_bridge_pb2 as bridge

logger = logging.getLogger(__name__)


def update_cluster_state(client, updates, expected_status=StatusIds.SUCCESS):
    response = client.update_cluster_state(updates)
    logger.debug(f"Update cluster state response: {response}")
    if response.operation.status != expected_status:
        raise Exception(f"Update cluster state failed with status: {response.operation.status}")
    if expected_status == StatusIds.SUCCESS:
        result = bridge.UpdateClusterStateResult()
        response.operation.result.Unpack(result)
        return result
    else:
        return response


def get_cluster_state(client):
    response = client.get_cluster_state()
    if response.operation.status != StatusIds.SUCCESS:
        raise Exception(f"Get cluster state failed with status: {response.operation.status}")
    result = bridge.GetClusterStateResult()
    response.operation.result.Unpack(result)
    logger.debug(f"Get cluster state result: {result}")
    return result


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
            return f"Created: {self.created}, Deleted: {self.deleted}, Existing: {len(self.tables)}"

    def _generate_new_table_n(self):
        with self.lock:
            n = self.created + 1
            while n in self.tables:
                n += 1
            self.tables[n] = WorkloadTablesCreateDrop.TableStatus.CREATING
            return n

    def create_table(self, table):
        path = self.get_table_path(table)
        stmt = f"""
                CREATE TABLE `{path}` (
                    key Int, value Utf8,
                    PRIMARY KEY(key)
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

    def _get_table_to_delete(self):
        with self.lock:
            for n, status in self.tables.items():
                if status == WorkloadTablesCreateDrop.TableStatus.AVAILABLE:
                    self.tables[n] = WorkloadTablesCreateDrop.TableStatus.DELETING
                    return n
            return None

    def _delete_tables_loop(self):
        logger.debug("starting")
        while not self.is_stop_requested():
            n = self._get_table_to_delete()
            if n is None:
                logger.info("create_drop: no tables to delete")
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
        return [self._create_tables_loop] * 3 + [self._delete_tables_loop] * 3


class WorkloadInsertDelete(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "insert_delete", stop)
        self.inserted = 0
        self.actual_rows = 0
        self.table_name = "table"
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Inserted: {self.inserted}, actual rows: {self.actual_rows}"

    def _query_with_retries(self, query):
        # retry undetermined
        retry_settings = ydb.RetrySettings(idempotent=True)
        return self.client.query(query, False, retry_settings=retry_settings)

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
            self._query_with_retries(
                f"""
                    UPSERT INTO `{table_path}` (`id`, `i64Val`)
                    VALUES
                    ({i * 2}, {i * 10}),
                    ({i * 2 + 1}, {i * 10 + 1})
                """,
            )

            self._query_with_retries(
                f"""
                    DELETE FROM `{table_path}`
                    WHERE i64Val % 2 == 1
                """,
            )

            actual = self._query_with_retries(
                f"""
                    SELECT COUNT(*) as cnt, SUM(i64Val) as vals, SUM(id) as ids FROM `{table_path}`
                """,
            )[0].rows[0]
            expected = {"cnt": i, "vals": i * (i + 1) * 5, "ids": i * (i + 1)}
            if actual != expected:
                raise Exception(f"Incorrect result: expected:{expected}, actual:{actual}")
            i += 1
            with self.lock:
                self.inserted += 2
                self.actual_rows = actual["cnt"]
        logger.debug(f"exiting after {i} iterations")

    def get_workload_thread_funcs(self):
        return [self._loop]


class PilePromotionWorkload(WorkloadBase):
    loop_cnt = 0
    wait_for = 9

    def __init__(self, client, grpc_endpoint, http_endpoint, prefix, stop):
        super().__init__(client, prefix, "pile_promotion", stop)
        self.ringGroupActorIdOffset = 1
        self.http_endpoint = http_endpoint
        self.lock = threading.Lock()
        parsed = urlparse(grpc_endpoint)
        host = parsed.hostname
        port = parsed.port
        self.bridge_client = BridgeClient(host, port)
        self.bridge_client.set_auth_token("root@builtin")

    def get_stat(self):
        with self.lock:
            return f"Pile promotions: {self.loop_cnt}"

    def _loop(self):
        logger.debug("starting")
        while not self.is_stop_requested():
            # Get current state
            current_state = get_cluster_state(self.bridge_client)
            logger.info(f"Current cluster state: {current_state}")
            non_primary_pile = ""
            for pile in current_state.pile_states:
                if pile.state != PileState.PRIMARY:
                    non_primary_pile = pile.pile_name
                    break
            logger.info(f"Non-primary pile: {non_primary_pile}")

            # Promote non-primary pile
            updates = [
                PileState(pile_name=non_primary_pile, state=PileState.PROMOTED),
            ]
            update_cluster_state(self.bridge_client, updates)
            logger.info(f"Promoted {non_primary_pile}")
            time.sleep(self.wait_for)

            # Verify the state change
            new_state = get_cluster_state(self.bridge_client)
            logger.info(f"Cluster state after the promotion: {new_state}")
            for pile in new_state.pile_states:
                if pile.pile_name == non_primary_pile:
                    assert (
                        pile.state == PileState.PRIMARY
                    ), f"expected PRIMARY state of the {non_primary_pile}, but got: {pile.state}"

            with self.lock:
                logger.info(f"Pile promotion {self.loop_cnt} finished")
                self.loop_cnt += 1
                logger.debug(f"iteration {self.loop_cnt}")
        with self.lock:
            logger.debug(f"exiting after {self.loop_cnt} iterations")
            self.bridge_client.close()

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
    def __init__(self, grpc_endpoint, http_endpoint, database, path, duration):
        self.client = InstrumentedYdbClient(grpc_endpoint, database, True)
        self.client.wait_connection()
        self.grpc_endpoint = grpc_endpoint
        self.http_endpoint = http_endpoint
        self.name = path
        self.tables_prefix = "/".join([self.client.database, self.name])
        self.duration = duration
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

        promotionWorkload = PilePromotionWorkload(
            self.client,
            self.grpc_endpoint,
            self.http_endpoint,
            self.name,
            stop,
        )
        workloads = [
            WorkloadTablesCreateDrop(self.client, self.name, stop),
            WorkloadInsertDelete(self.client, self.name, stop),
            promotionWorkload,
            WorkloadDiscovery(self.client, self.grpc_endpoint, self.name, stop),
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
        return promotionWorkload.loop_cnt
