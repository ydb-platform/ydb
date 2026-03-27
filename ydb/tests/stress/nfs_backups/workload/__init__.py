# -*- coding: utf-8 -*-
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
        self._driver = driver
        metadata = ydb_export_pb2.ExportToFsMetadata()
        response.operation.metadata.Unpack(metadata)
        self.progress = _EXPORT_PROGRESSES.get(metadata.progress, "UNKNOWN")
        self.items_progress = metadata.items_progress


class ImportFromFsOperation:
    def __init__(self, rpc_state, response, driver):
        ydb_issues._process_response(response.operation)
        self.id = response.operation.id
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

    def _create_topics(self, topic_names: List[str], consumers: Optional[Dict[str, List[str]]] = None):
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
        for idx, table in enumerate(tables, 1):
            if self.is_stop_requested():
                return
            rows = [
                {"id": row_id, "message": f"Table {idx} ({table}) row {row_id}"}
                for row_id in range(1, 6)
            ]
            self._insert_into_table(table, rows)

    def _op_forget(self, op_id):
        try:
            self.op_client.forget(op_id)
        except Exception:
            pass

    def _poll_export(self, export_id):
        """Check export status. Returns terminal status string or None if still in progress."""
        try:
            op = self.fs_client.get_export_operation(export_id)
            if op.progress in ("DONE", "CANCELLED", "UNSPECIFIED"):
                return op.progress
        except Exception:
            return "ERROR"
        return None

    def _poll_import(self, import_id):
        """Check import status. Returns terminal status string or None if still in progress."""
        try:
            op = self.fs_client.get_import_operation(import_id)
            if op.progress in ("DONE", "CANCELLED", "UNSPECIFIED"):
                return op.progress
        except Exception:
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

    def _wait_for_export_slot(self):
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
        result = self.fs_client.export_to_fs(
            base_path=base_path,
            items=[(source_prefix, source_prefix)],
            description=f"stress_export_{run_id}",
        )
        with self.lock:
            self._stats["export_started"] += 1
            self.export_in_progress.append(result.id)
        return result.id

    def _wait_op_done(self, op_id, poll_fn, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_stop_requested():
                return None
            status = poll_fn(op_id)
            if status is not None:
                self._op_forget(op_id)
                return status
            time.sleep(1)
        return None

    def _export_loop(self):
        """Continuously create data and export to NFS."""
        while not self.is_stop_requested():
            run_id = f"{uuid.uuid1()}".replace("-", "_")
            prefix = f"export_block_{run_id}"
            base_path = os.path.join(self.nfs_mount_path, f"export_{run_id}")

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
                print(f"Export error: {e}", file=sys.stderr)
                self._inc_stat("export_error")

    def _export_import_loop(self):
        """Export data to NFS, then import it back (round-trip)."""
        while not self.is_stop_requested():
            run_id = f"{uuid.uuid1()}".replace("-", "_")
            prefix = f"rt_block_{run_id}"
            base_path = os.path.join(self.nfs_mount_path, f"roundtrip_{run_id}")

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

                status = self._wait_op_done(export_id, self._poll_export)
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
                    continue

                import_dest = f"imported_{run_id}"
                result = self.fs_client.import_from_fs(
                    base_path=base_path,
                    destination_path=import_dest,
                )
                self._inc_stat("import_started")

                imp_status = self._wait_op_done(result.id, self._poll_import)
                if imp_status == "DONE":
                    self._inc_stat("import_done")
                elif imp_status == "CANCELLED":
                    self._inc_stat("import_cancelled")
                else:
                    self._inc_stat("import_error")

            except Exception as e:
                print(f"Export/Import round-trip error: {e}", file=sys.stderr)
                self._inc_stat("export_error")
            finally:
                try:
                    if os.path.exists(base_path):
                        shutil.rmtree(base_path, ignore_errors=True)
                except Exception:
                    pass

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
        if not nfs_mount_path:
            raise RuntimeError("NFS_MOUNT_PATH environment variable is not set")
        os.makedirs(nfs_mount_path, exist_ok=True)
        return nfs_mount_path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self):
        stop = threading.Event()
        nfs_mount_path = self._setup_nfs()
        workloads = [
            WorkloadNfsExportImport(self.client, stop, nfs_mount_path)
        ]

        for w in workloads:
            w.start()
        started_at = time.time()
        while time.time() - started_at < self.duration:
            print(f"Elapsed {int(time.time() - started_at)} seconds, stat:", file=sys.stderr)
            for w in workloads:
                print(f"\t{w.name}: {w.get_stat()}", file=sys.stderr)
            time.sleep(10)
        stop.set()
        print("Waiting for stop...", file=sys.stderr)
        for w in workloads:
            w.join(timeout=30)
        print("Stopped", file=sys.stderr)
