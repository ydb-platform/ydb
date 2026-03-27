# -*- coding: utf-8 -*-
import logging
import os
import shutil
import sys
import threading
import time
import uuid
import ydb
from typing import Any, Dict, List, Optional

from ydb.tests.stress.common.common import WorkloadBase

try:
    from ydb.public.api.protos import ydb_export_pb2
    from ydb.public.api.protos import ydb_import_pb2
    from ydb.public.api.grpc import ydb_export_v1_pb2_grpc
    from ydb.public.api.grpc import ydb_import_v1_pb2_grpc
except ImportError:
    from contrib.ydb.public.api.protos import ydb_export_pb2
    from contrib.ydb.public.api.protos import ydb_import_pb2
    from contrib.ydb.public.api.grpc import ydb_export_v1_pb2_grpc
    from contrib.ydb.public.api.grpc import ydb_import_v1_pb2_grpc

from ydb.operation import OperationClient
from ydb import issues as ydb_issues

logger = logging.getLogger(__name__)

_EXPORT_PROGRESSES = {}
_IMPORT_PROGRESSES = {}


def _init_progresses():
    for key, value in ydb_export_pb2.ExportProgress.Progress.items():
        _EXPORT_PROGRESSES[value] = key[len("PROGRESS_"):]
    for key, value in ydb_import_pb2.ImportProgress.Progress.items():
        _IMPORT_PROGRESSES[value] = key[len("PROGRESS_"):]


_init_progresses()


class ExportToFsOperation:
    def __init__(self, rpc_state, response, driver):
        ydb_issues._process_response(response.operation)
        self.id = response.operation.id
        self.ready = response.operation.ready
        self._driver = driver
        metadata = ydb_export_pb2.ExportToFsMetadata()
        response.operation.metadata.Unpack(metadata)
        self.progress = _EXPORT_PROGRESSES.get(metadata.progress, "UNKNOWN")
        self.items_progress = metadata.items_progress


class ImportFromFsOperation:
    def __init__(self, rpc_state, response, driver):
        ydb_issues._process_response(response.operation)
        self.id = response.operation.id
        self.ready = response.operation.ready
        self._driver = driver
        metadata = ydb_import_pb2.ImportFromFsMetadata()
        response.operation.metadata.Unpack(metadata)
        self.progress = _IMPORT_PROGRESSES.get(metadata.progress, "UNKNOWN")
        self.items_progress = metadata.items_progress


class FsExportClient:
    """Thin wrapper for ExportToFs / ImportFromFs gRPC calls."""

    def __init__(self, driver):
        self._driver = driver

    def export_to_fs(self, base_path, items, description="", number_of_retries=3):
        request = ydb_export_pb2.ExportToFsRequest(
            settings=ydb_export_pb2.ExportToFsSettings(
                base_path=base_path,
                number_of_retries=number_of_retries,
            )
        )
        if description:
            request.settings.description = description
        for src, dst in items:
            request.settings.items.add(source_path=src, destination_path=dst)
        return self._driver(
            request,
            ydb_export_v1_pb2_grpc.ExportServiceStub,
            "ExportToFs",
            ExportToFsOperation,
            None,
            (self._driver,),
        )

    def get_export_operation(self, operation_id):
        from ydb import _apis
        request = _apis.ydb_operation.GetOperationRequest(id=operation_id)
        return self._driver(
            request,
            _apis.OperationService.Stub,
            _apis.OperationService.GetOperation,
            ExportToFsOperation,
            None,
            (self._driver,),
        )

    def import_from_fs(self, base_path, items=None, description="", number_of_retries=3, destination_path=""):
        request = ydb_import_pb2.ImportFromFsRequest(
            settings=ydb_import_pb2.ImportFromFsSettings(
                base_path=base_path,
                number_of_retries=number_of_retries,
            )
        )
        if description:
            request.settings.description = description
        if destination_path:
            request.settings.destination_path = destination_path
        if items:
            for src, dst in items:
                request.settings.items.add(source_path=src, destination_path=dst)
        return self._driver(
            request,
            ydb_import_v1_pb2_grpc.ImportServiceStub,
            "ImportFromFs",
            ImportFromFsOperation,
            None,
            (self._driver,),
        )

    def get_import_operation(self, operation_id):
        from ydb import _apis
        request = _apis.ydb_operation.GetOperationRequest(id=operation_id)
        return self._driver(
            request,
            _apis.OperationService.Stub,
            _apis.OperationService.GetOperation,
            ImportFromFsOperation,
            None,
            (self._driver,),
        )


class WorkloadNfsExportImport(WorkloadBase):
    def __init__(self, client, stop, nfs_mount_path):
        super().__init__(client, "", "nfs_export_import", stop)
        self.lock = threading.Lock()
        self.nfs_mount_path = nfs_mount_path
        self.fs_client = FsExportClient(self.client.driver)
        self.op_client = OperationClient(self.client.driver)

        self.export_limit = 10
        self.export_in_progress = []

        self._stats = {
            "export_started": 0,
            "export_done": 0,
            "export_cancelled": 0,
            "export_error": 0,
            "import_started": 0,
            "import_done": 0,
            "import_cancelled": 0,
            "import_error": 0,
        }

    def get_stat(self):
        with self.lock:
            return ", ".join(f"{k}={v}" for k, v in self._stats.items())

    def _inc_stat(self, key):
        with self.lock:
            self._stats[key] += 1

    def _create_tables(self, table_names: List[str]):
        logger.info("[schema] Creating %d tables under prefix %s", len(table_names), table_names[0].rsplit("/", 1)[0] if table_names else "?")
        for name in table_names:
            if self.is_stop_requested():
                return
            self.client.query(
                f"""
                    CREATE TABLE `{name}` (
                        id Uint32 NOT NULL,
                        message Utf8,
                        INDEX idx_message GLOBAL SYNC ON (message),
                        PRIMARY KEY (id)
                    );
                """,
                True
            )
        logger.info("[schema] Created %d tables", len(table_names))

    def _create_topics(self, topic_names: List[str], consumers: Optional[Dict[str, List[str]]] = None):
        logger.info("[schema] Creating %d topics with consumers", len(topic_names))
        for name in topic_names:
            if self.is_stop_requested():
                return
            self.client.query(f"CREATE TOPIC `{name}`;", True)
            if consumers and name in consumers:
                for consumer in consumers[name]:
                    self.client.query(
                        f"ALTER TOPIC `{name}` ADD CONSUMER {consumer};",
                        True
                    )
        logger.info("[schema] Created %d topics", len(topic_names))

    def _insert_into_table(self, table_name: str, rows: List[Dict[str, Any]]):
        for row in rows:
            if self.is_stop_requested():
                return
            id_val = row["id"]
            msg_val = row["message"]
            self.client.query(
                f"INSERT INTO `{table_name}` (id, message) VALUES ({id_val}, '{msg_val}');",
                True
            )

    def _insert_rows(self, tables: List[str]):
        logger.info("[data] Inserting rows into %d tables", len(tables))
        for idx, table in enumerate(tables, 1):
            if self.is_stop_requested():
                return
            rows = [
                {"id": row_id, "message": f"Table {idx} ({table}) row {row_id}"}
                for row_id in range(1, 6)
            ]
            self._insert_into_table(table, rows)
        logger.info("[data] Inserted rows into %d tables", len(tables))

    def _op_forget(self, op_id):
        try:
            self.op_client.forget(op_id)
        except Exception:
            pass

    def _poll_export(self, export_id):
        """Check export status. Returns terminal status string or None if still in progress."""
        try:
            op = self.fs_client.get_export_operation(export_id)
            logger.debug("[export] Poll op=%s ready=%s progress=%s", export_id, op.ready, op.progress)
            if op.ready:
                return op.progress if op.progress != "UNSPECIFIED" else "DONE"
        except ydb_issues.NotFound:
            logger.debug("[export] Poll op=%s: NOT_FOUND (auto-dropped, treating as DONE)", export_id)
            return "DONE"
        except Exception as e:
            logger.warning("[export] Poll op=%s failed: %s", export_id, e)
            return "ERROR"
        return None

    def _poll_import(self, import_id):
        """Check import status. Returns terminal status string or None if still in progress."""
        try:
            op = self.fs_client.get_import_operation(import_id)
            logger.debug("[import] Poll op=%s ready=%s progress=%s", import_id, op.ready, op.progress)
            if op.ready:
                return op.progress if op.progress != "UNSPECIFIED" else "DONE"
        except ydb_issues.NotFound:
            logger.debug("[import] Poll op=%s: NOT_FOUND (auto-dropped, treating as DONE)", import_id)
            return "DONE"
        except Exception as e:
            logger.warning("[import] Poll op=%s failed: %s", import_id, e)
            return "ERROR"
        return None

    def _cleanup_exports(self):
        """Clean up completed exports. No lock held during gRPC calls."""
        current = list(self.export_in_progress)
        completed = []
        for eid in current:
            status = self._poll_export(eid)
            if status is not None:
                completed.append((eid, status))

        if completed:
            with self.lock:
                for eid, status in completed:
                    if eid in self.export_in_progress:
                        self.export_in_progress.remove(eid)
                    if status == "DONE":
                        self._stats["export_done"] += 1
                    elif status == "CANCELLED":
                        self._stats["export_cancelled"] += 1
                    else:
                        self._stats["export_error"] += 1
            for eid, _ in completed:
                self._op_forget(eid)
            logger.info("[cleanup] Cleaned up %d exports: %s",
                        len(completed), ", ".join(f"{eid[:12]}..={s}" for eid, s in completed))

    def _wait_for_export_slot(self):
        if len(self.export_in_progress) >= self.export_limit:
            logger.info("[export] Waiting for export slot (%d/%d in progress)",
                        len(self.export_in_progress), self.export_limit)
        while len(self.export_in_progress) >= self.export_limit:
            if self.is_stop_requested():
                return
            self._cleanup_exports()
            if len(self.export_in_progress) >= self.export_limit:
                time.sleep(0.5)

    def _do_export(self, base_path, source_prefix, run_id):
        self._cleanup_exports()
        self._wait_for_export_slot()
        if self.is_stop_requested():
            return None
        logger.info("[export] Starting ExportToFs run_id=%s base_path=%s prefix=%s",
                    run_id, base_path, source_prefix)
        result = self.fs_client.export_to_fs(
            base_path=base_path,
            items=[(source_prefix, source_prefix)],
            description=f"stress_export_{run_id}",
        )
        with self.lock:
            self._stats["export_started"] += 1
            self.export_in_progress.append(result.id)
        logger.info("[export] ExportToFs started: op=%s progress=%s", result.id, result.progress)
        return result.id

    def _wait_op_done(self, op_id, poll_fn, op_type="op", timeout=120):
        logger.info("[%s] Waiting for op=%s (timeout=%ds)", op_type, op_id, timeout)
        deadline = time.time() + timeout
        poll_count = 0
        while time.time() < deadline:
            if self.is_stop_requested():
                logger.info("[%s] Stop requested while waiting for op=%s", op_type, op_id)
                return None
            status = poll_fn(op_id)
            poll_count += 1
            if status is not None:
                logger.info("[%s] Op=%s finished with status=%s after %d polls", op_type, op_id, status, poll_count)
                self._op_forget(op_id)
                return status
            time.sleep(1)
        logger.warning("[%s] Op=%s timed out after %ds (%d polls)", op_type, op_id, timeout, poll_count)
        return None

    def _export_loop(self):
        """Continuously create data and export to NFS."""
        iteration = 0
        logger.info("[export_loop] Started, nfs_mount_path=%s", self.nfs_mount_path)
        while not self.is_stop_requested():
            iteration += 1
            run_id = f"{uuid.uuid1()}".replace("-", "_")
            prefix = f"export_block_{run_id}"
            base_path = os.path.join(self.nfs_mount_path, f"export_{run_id}")

            logger.info("[export_loop] === Iteration %d, run_id=%s ===", iteration, run_id[:16])

            tables = [f"{prefix}/table{i}" for i in range(1, 10)]
            topics = [f"{prefix}/topic{i}" for i in range(1, 10)]
            consumers = {
                topic: [f"consumerA_{i}", f"consumerB_{i}"]
                for i, topic in enumerate(topics, 1)
            }

            self._create_tables(tables)
            if self.is_stop_requested():
                break
            self._create_topics(topics, consumers)
            if self.is_stop_requested():
                break
            self._insert_rows(tables)
            if self.is_stop_requested():
                break

            try:
                self._do_export(base_path, prefix, run_id)
            except Exception as e:
                logger.error("[export_loop] Export failed: %s", e, exc_info=True)
                self._inc_stat("export_error")

        logger.info("[export_loop] Stopped after %d iterations", iteration)

    def _export_import_loop(self):
        """Export data to NFS, then import it back (round-trip)."""
        iteration = 0
        logger.info("[roundtrip_loop] Started, nfs_mount_path=%s", self.nfs_mount_path)
        while not self.is_stop_requested():
            iteration += 1
            run_id = f"{uuid.uuid1()}".replace("-", "_")
            prefix = f"rt_block_{run_id}"
            base_path = os.path.join(self.nfs_mount_path, f"roundtrip_{run_id}")

            logger.info("[roundtrip_loop] === Iteration %d, run_id=%s ===", iteration, run_id[:16])

            tables = [f"{prefix}/table{i}" for i in range(1, 4)]
            self._create_tables(tables)
            if self.is_stop_requested():
                break
            self._insert_rows(tables)
            if self.is_stop_requested():
                break

            try:
                export_id = self._do_export(base_path, prefix, run_id)
                if export_id is None:
                    break

                status = self._wait_op_done(export_id, self._poll_export, op_type="export")
                if status is None or self.is_stop_requested():
                    break
                with self.lock:
                    if export_id in self.export_in_progress:
                        self.export_in_progress.remove(export_id)
                    if status == "DONE":
                        self._stats["export_done"] += 1
                    else:
                        self._stats["export_error"] += 1

                if status != "DONE":
                    logger.warning("[roundtrip_loop] Export finished with status=%s, skipping import", status)
                    continue

                db = self.client.database.rstrip("/")
                import_items = [(t, f"{db}/imported_{t}") for t in tables]
                logger.info("[roundtrip_loop] Starting ImportFromFs base_path=%s items=%s", base_path, import_items)
                result = self.fs_client.import_from_fs(
                    base_path=base_path,
                    items=import_items,
                )
                self._inc_stat("import_started")
                logger.info("[roundtrip_loop] ImportFromFs started: op=%s progress=%s", result.id, result.progress)

                imp_status = self._wait_op_done(result.id, self._poll_import, op_type="import")
                if imp_status == "DONE":
                    self._inc_stat("import_done")
                    logger.info("[roundtrip_loop] Import DONE for run_id=%s", run_id[:16])
                elif imp_status == "CANCELLED":
                    self._inc_stat("import_cancelled")
                    logger.warning("[roundtrip_loop] Import CANCELLED for run_id=%s", run_id[:16])
                else:
                    self._inc_stat("import_error")
                    logger.error("[roundtrip_loop] Import failed with status=%s for run_id=%s", imp_status, run_id[:16])

            except Exception as e:
                logger.error("[roundtrip_loop] Round-trip failed: %s", e, exc_info=True)
                self._inc_stat("export_error")
            finally:
                try:
                    if os.path.exists(base_path):
                        shutil.rmtree(base_path, ignore_errors=True)
                except Exception:
                    pass

        logger.info("[roundtrip_loop] Stopped after %d iterations", iteration)

    def get_workload_thread_funcs(self):
        return [self._export_loop, self._export_import_loop]


class WorkloadRunner:
    def __init__(self, client, duration):
        self.client = client
        self.duration = duration
        ydb.interceptor.monkey_patch_event_handler()

    @staticmethod
    def _setup_nfs():
        nfs_mount_path = os.getenv("NFS_MOUNT_PATH")
        ld_preload = os.getenv("LD_PRELOAD")
        logger.info("[setup] NFS_MOUNT_PATH=%s", nfs_mount_path)
        logger.info("[setup] LD_PRELOAD=%s", ld_preload)
        if not nfs_mount_path:
            raise RuntimeError("NFS_MOUNT_PATH environment variable is not set")
        os.makedirs(nfs_mount_path, exist_ok=True)
        logger.info("[setup] NFS mount directory ready: %s", nfs_mount_path)
        return nfs_mount_path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self):
        logger.info("[runner] Starting workload, duration=%ds", self.duration)
        stop = threading.Event()
        nfs_mount_path = self._setup_nfs()
        workloads = [
            WorkloadNfsExportImport(self.client, stop, nfs_mount_path)
        ]

        for w in workloads:
            w.start()
            logger.info("[runner] Started workload thread: %s", w.name)

        started_at = time.time()
        while time.time() - started_at < self.duration:
            elapsed = int(time.time() - started_at)
            for w in workloads:
                stat = w.get_stat()
                msg = f"[runner] Elapsed {elapsed}s | {w.name}: {stat}"
                logger.info(msg)
                print(msg, file=sys.stderr)
            time.sleep(10)

        logger.info("[runner] Duration reached, sending stop signal")
        stop.set()

        for w in workloads:
            logger.info("[runner] Waiting for %s to finish (timeout=30s)", w.name)
            w.join(timeout=30)
            if w.is_alive():
                logger.warning("[runner] %s did not stop within 30s", w.name)
            else:
                logger.info("[runner] %s finished", w.name)

        logger.info("[runner] All workloads stopped")
