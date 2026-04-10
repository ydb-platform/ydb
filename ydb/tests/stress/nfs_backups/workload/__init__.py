# -*- coding: utf-8 -*-
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
import uuid
import ydb

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


def _ensure_table_exists(client, table_name):
    """Check if table exists; if not, import it from S3 using env vars."""
    try:
        result = client.query(
            f"SELECT COUNT(*) AS cnt FROM `{table_name}` LIMIT 1;",
            False,
        )
        logger.info("[setup] Table '%s' already exists", table_name)
        return
    except Exception:
        logger.info("[setup] Table '%s' not found, will try to import from S3", table_name)

    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_bucket = os.getenv("S3_BUCKET")
    s3_access_key = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret_key = os.getenv("S3_ACCESS_KEY_SECRET")
    s3_source_prefix = os.getenv("S3_SOURCE_PREFIX", table_name)

    if not all([s3_endpoint, s3_bucket, s3_access_key, s3_secret_key]):
        missing = [
            name for name, val in [
                ("S3_ENDPOINT", s3_endpoint),
                ("S3_BUCKET", s3_bucket),
                ("S3_ACCESS_KEY_ID", s3_access_key),
                ("S3_ACCESS_KEY_SECRET", s3_secret_key),
            ] if not val
        ]
        raise RuntimeError(
            f"Table '{table_name}' does not exist and cannot import from S3: "
            f"missing env vars: {', '.join(missing)}"
        )

    from ydb.import_client import ImportClient, ImportFromS3Settings

    db_path = client.database.rstrip("/")
    dest_path = f"{db_path}/{table_name}"

    settings = (
        ImportFromS3Settings()
        .with_endpoint(s3_endpoint)
        .with_bucket(s3_bucket)
        .with_access_key(s3_access_key)
        .with_secret_key(s3_secret_key)
        .with_number_of_retries(3)
        .with_source_and_destination(s3_source_prefix, dest_path)
    )

    logger.info(
        "[setup] Importing table from S3: endpoint=%s bucket=%s prefix=%s -> %s",
        s3_endpoint, s3_bucket, s3_source_prefix, dest_path,
    )

    import_client = ImportClient(client.driver)
    result = import_client.import_from_s3(settings)
    op_id = result.id
    logger.info("[setup] S3 import started: op=%s progress=%s", op_id, result.progress.name)

    while True:
        op = import_client.get_import_from_s3_operation(op_id)
        progress = op.progress.name
        if progress == "DONE":
            logger.info("[setup] S3 import DONE: op=%s", op_id)
            break
        elif progress == "CANCELLED":
            raise RuntimeError(f"S3 import cancelled: op={op_id}")
        logger.debug("[setup] S3 import in progress: op=%s progress=%s", op_id, progress)
        time.sleep(5)

    try:
        client.query(f"SELECT COUNT(*) AS cnt FROM `{table_name}` LIMIT 1;", False)
        logger.info("[setup] Table '%s' imported successfully", table_name)
    except Exception as e:
        raise RuntimeError(f"Table '{table_name}' not accessible after S3 import: {e}")


class WorkloadNfsExportImport(WorkloadBase):
    TABLE_NAME = "large_test_table"

    def __init__(self, client, stop, nfs_mount_path, fatal_error_event):
        super().__init__(client, "", "nfs_export_import", stop)
        self.lock = threading.Lock()
        self.nfs_mount_path = nfs_mount_path
        self.fatal_error_event = fatal_error_event
        self.fs_client = FsExportClient(self.client.driver)
        self.op_client = OperationClient(self.client.driver)

        self.export_in_progress = None  # (export_id, base_path, run_id) or None
        self.pending_import = None  # (base_path, run_id) or None

        self._stats = {
            "export_started": 0,
            "export_done": 0,
            "export_error": 0,
            "import_started": 0,
            "import_done": 0,
            "import_error": 0,
        }

    def get_stat(self):
        with self.lock:
            return ", ".join(f"{k}={v}" for k, v in self._stats.items())

    def _inc_stat(self, key):
        with self.lock:
            self._stats[key] += 1

    def _signal_fatal_error(self, message):
        logger.error("[FATAL] %s", message)
        self.fatal_error_event.set()

    def _op_forget(self, op_id):
        try:
            self.op_client.forget(op_id)
        except Exception:
            pass

    def _poll_export(self, export_id):
        try:
            op = self.fs_client.get_export_operation(export_id)
            logger.debug("[export] Poll op=%s ready=%s progress=%s", export_id, op.ready, op.progress)
            if op.ready:
                return op.progress if op.progress != "UNSPECIFIED" else "DONE"
        except ydb_issues.NotFound:
            logger.debug("[export] Poll op=%s: NOT_FOUND (treating as DONE)", export_id)
            return "DONE"
        except Exception as e:
            logger.warning("[export] Poll op=%s failed: %s", export_id, e)
            return "ERROR"
        return None

    def _poll_import(self, import_id):
        try:
            op = self.fs_client.get_import_operation(import_id)
            logger.debug("[import] Poll op=%s ready=%s progress=%s", import_id, op.ready, op.progress)
            if op.ready:
                return op.progress if op.progress != "UNSPECIFIED" else "DONE"
        except ydb_issues.NotFound:
            logger.debug("[import] Poll op=%s: NOT_FOUND (treating as DONE)", import_id)
            return "DONE"
        except Exception as e:
            logger.warning("[import] Poll op=%s failed: %s", import_id, e)
            return "ERROR"
        return None

    def _wait_for_export(self) -> bool:
        """Wait for current export to complete. Returns False on error."""
        if self.export_in_progress is None:
            return True

        export_id, base_path, run_id = self.export_in_progress

        while True:
            if self.is_stop_requested() or self.fatal_error_event.is_set():
                return True

            status = self._poll_export(export_id)
            if status is not None:
                self.export_in_progress = None
                self._op_forget(export_id)

                if status == "DONE":
                    self._inc_stat("export_done")
                    logger.info("[export] Export DONE: op=%s, queuing import", export_id[:16])
                    self.pending_import = (base_path, run_id)
                    return True
                else:
                    self._inc_stat("export_error")
                    logger.error("[export] Export FAILED: op=%s status=%s base_path=%s. NOT cleaning up for investigation.",
                                 export_id[:16], status, base_path)
                    self._signal_fatal_error(f"Export failed with status={status}, op={export_id}")
                    return False

            time.sleep(1)

    def _cleanup_fs_path(self, base_path: str):
        """Remove export directory from filesystem."""
        try:
            if os.path.exists(base_path):
                shutil.rmtree(base_path, ignore_errors=True)
                logger.info("[cleanup] Removed export directory: %s", base_path)
        except Exception as e:
            logger.warning("[cleanup] Failed to remove directory %s: %s", base_path, e)

    def _cleanup_imported_table(self, table_name: str):
        """Drop imported table from database."""
        try:
            self.client.query(f"DROP TABLE `{table_name}`;", True)
            logger.info("[cleanup] Dropped imported table: %s", table_name)
        except Exception as e:
            logger.warning("[cleanup] Failed to drop table %s: %s", table_name, e)

    def _process_pending_import(self) -> bool:
        """Process pending import if any. Returns False on error."""
        if self.pending_import is None:
            return True

        base_path, run_id = self.pending_import
        self.pending_import = None

        db_path = self.client.database.rstrip("/")
        import_dest = f"{db_path}/imported_{run_id}"

        try:
            logger.info("[import] Starting ImportFromFs base_path=%s dest=%s", base_path, import_dest)

            result = self.fs_client.import_from_fs(
                base_path=base_path,
                items=[(self.TABLE_NAME, import_dest)],
                description=f"stress_import_{run_id}",
            )
            self._inc_stat("import_started")
            logger.info("[import] ImportFromFs started: op=%s", result.id)

            while True:
                if self.is_stop_requested() or self.fatal_error_event.is_set():
                    return True

                status = self._poll_import(result.id)
                if status is not None:
                    self._op_forget(result.id)
                    if status == "DONE":
                        self._inc_stat("import_done")
                        logger.info("[import] Import DONE: op=%s, cleaning up", result.id[:16])
                        self._cleanup_imported_table(import_dest)
                        self._cleanup_fs_path(base_path)
                    else:
                        self._inc_stat("import_error")
                        logger.error("[import] Import FAILED: op=%s status=%s. NOT cleaning up for investigation.",
                                     result.id[:16], status)
                        self._signal_fatal_error(f"Import failed with status={status}, op={result.id}")
                        return False
                    break
                time.sleep(1)

        except Exception as e:
            self._inc_stat("import_error")
            logger.error("[import] Import EXCEPTION for run_id=%s. NOT cleaning up for investigation. Error: %s",
                         run_id[:16], e)
            self._signal_fatal_error(f"Import exception: {e}")
            return False

        return True

    def _start_export(self, run_id: str) -> bool:
        """Start a new export. Returns False on error."""
        base_path = os.path.join(self.nfs_mount_path, f"export_{run_id}")

        try:
            logger.info("[export] Starting ExportToFs run_id=%s base_path=%s", run_id[:16], base_path)
            result = self.fs_client.export_to_fs(
                base_path=base_path,
                items=[(self.TABLE_NAME, self.TABLE_NAME)],
                description=f"stress_export_{run_id}",
            )
            self._inc_stat("export_started")
            self.export_in_progress = (result.id, base_path, run_id)
            logger.info("[export] ExportToFs started: op=%s progress=%s", result.id, result.progress)
            return True
        except Exception as e:
            self._inc_stat("export_error")
            logger.error("[export] Export EXCEPTION for run_id=%s base_path=%s. NOT cleaning up for investigation. Error: %s",
                         run_id[:16], base_path, e)
            self._signal_fatal_error(f"Export exception: {e}")
            return False

    def _main_loop(self):
        logger.info("[main_loop] Starting export/import cycle, nfs_mount_path=%s", self.nfs_mount_path)

        try:
            _ensure_table_exists(self.client, self.TABLE_NAME)
        except Exception as e:
            self._signal_fatal_error(f"Cannot ensure table exists: {e}")
            return

        iteration = 0

        while not self.is_stop_requested() and not self.fatal_error_event.is_set():
            iteration += 1
            run_id = f"{uuid.uuid1()}".replace("-", "_")
            logger.info("[main_loop] === Iteration %d, run_id=%s ===", iteration, run_id[:16])

            # Start export
            if not self._start_export(run_id):
                return

            # Wait for export to complete
            if not self._wait_for_export():
                return

            # Process import
            if not self._process_pending_import():
                return

        logger.info("[main_loop] Stopped after %d iterations", iteration)

    def get_workload_thread_funcs(self):
        return [self._main_loop]


class WorkloadFullRoundtrip(WorkloadBase):
    NUM_TABLES = 5
    NUM_ROWS = 100
    NUM_TOPICS = 3
    NUM_VIEWS = 3

    def __init__(self, client, stop, nfs_mount_path, fatal_error_event):
        super().__init__(client, "", "nfs_full_roundtrip", stop)
        self.lock = threading.Lock()
        self.nfs_mount_path = nfs_mount_path
        self.fatal_error_event = fatal_error_event
        self.fs_client = FsExportClient(self.client.driver)
        self.op_client = OperationClient(self.client.driver)

        self._stats = {
            "iterations": 0,
            "export_started": 0,
            "export_done": 0,
            "export_error": 0,
            "import_started": 0,
            "import_done": 0,
            "import_error": 0,
        }

    def get_stat(self):
        with self.lock:
            return ", ".join(f"{k}={v}" for k, v in self._stats.items())

    def _inc_stat(self, key):
        with self.lock:
            self._stats[key] += 1

    def _signal_fatal_error(self, message):
        logger.error("[full_rt][FATAL] %s", message)
        self.fatal_error_event.set()

    def _op_forget(self, op_id):
        try:
            self.op_client.forget(op_id)
        except Exception:
            pass

    def _poll_export(self, export_id):
        try:
            op = self.fs_client.get_export_operation(export_id)
            logger.debug("[full_rt][export] Poll op=%s ready=%s progress=%s", export_id, op.ready, op.progress)
            if op.ready:
                return op.progress if op.progress != "UNSPECIFIED" else "DONE"
        except ydb_issues.NotFound:
            logger.debug("[full_rt][export] Poll op=%s: NOT_FOUND (treating as DONE)", export_id)
            return "DONE"
        except Exception as e:
            logger.warning("[full_rt][export] Poll op=%s failed: %s", export_id, e)
            return "ERROR"
        return None

    def _poll_import(self, import_id):
        try:
            op = self.fs_client.get_import_operation(import_id)
            logger.debug("[full_rt][import] Poll op=%s ready=%s progress=%s", import_id, op.ready, op.progress)
            if op.ready:
                return op.progress if op.progress != "UNSPECIFIED" else "DONE"
        except ydb_issues.NotFound:
            logger.debug("[full_rt][import] Poll op=%s: NOT_FOUND (treating as DONE)", import_id)
            return "DONE"
        except Exception as e:
            logger.warning("[full_rt][import] Poll op=%s failed: %s", import_id, e)
            return "ERROR"
        return None

    def _wait_op(self, op_id, poll_fn, op_type):
        while True:
            if self.is_stop_requested() or self.fatal_error_event.is_set():
                return None
            status = poll_fn(op_id)
            if status is not None:
                self._op_forget(op_id)
                return status
            time.sleep(1)

    def _create_schema(self, prefix, table_names, topic_names, view_names):
        for name in table_names:
            if self.is_stop_requested():
                return False
            self.client.query(
                f"""
                    CREATE TABLE `{name}` (
                        id Uint32 NOT NULL,
                        payload Utf8,
                        INDEX idx_payload GLOBAL SYNC ON (payload),
                        PRIMARY KEY (id)
                    );
                """,
                True,
            )
        logger.info("[full_rt] Created %d tables under %s", len(table_names), prefix)

        for name in topic_names:
            if self.is_stop_requested():
                return False
            self.client.query(f"CREATE TOPIC `{name}`;", True)
            self.client.query(f"ALTER TOPIC `{name}` ADD CONSUMER consumer_a;", True)
        logger.info("[full_rt] Created %d topics under %s", len(topic_names), prefix)

        for i, name in enumerate(view_names):
            if self.is_stop_requested():
                return False
            src_table = table_names[i % len(table_names)]
            self.client.query(
                f"CREATE VIEW `{name}` WITH security_invoker = TRUE AS SELECT * FROM `{src_table}`;",
                True,
            )
        logger.info("[full_rt] Created %d views under %s", len(view_names), prefix)

        return True

    def _insert_rows(self, table_names):
        for table in table_names:
            if self.is_stop_requested():
                return False
            for batch_start in range(0, self.NUM_ROWS, 10):
                if self.is_stop_requested():
                    return False
                values = ", ".join(
                    f"({row_id}, 'row_{row_id}_in_{table.rsplit('/', 1)[-1]}')"
                    for row_id in range(batch_start, min(batch_start + 10, self.NUM_ROWS))
                )
                self.client.query(
                    f"INSERT INTO `{table}` (id, payload) VALUES {values};",
                    False,
                )
        logger.info("[full_rt] Inserted %d rows into %d tables", self.NUM_ROWS, len(table_names))
        return True

    def _drop_tables(self, names):
        for name in names:
            try:
                self.client.query(f"DROP TABLE `{name}`;", True)
            except Exception:
                pass

    def _drop_topics(self, names):
        for name in names:
            try:
                self.client.query(f"DROP TOPIC `{name}`;", True)
            except Exception:
                pass

    def _drop_views(self, names):
        for name in names:
            try:
                self.client.query(f"DROP VIEW `{name}`;", True)
            except Exception:
                pass

    def _main_loop(self):
        logger.info("[full_rt] Started, nfs_mount_path=%s", self.nfs_mount_path)

        while not self.is_stop_requested() and not self.fatal_error_event.is_set():
            self._inc_stat("iterations")
            run_id = f"{uuid.uuid1()}".replace("-", "_")
            prefix = f"full_{run_id}"
            base_path = os.path.join(self.nfs_mount_path, f"full_{run_id}")
            db_path = self.client.database.rstrip("/")

            table_names = [f"{prefix}/tbl{i}" for i in range(self.NUM_TABLES)]
            topic_names = [f"{prefix}/topic{i}" for i in range(self.NUM_TOPICS)]
            view_names = [f"{prefix}/view{i}" for i in range(self.NUM_VIEWS)]

            logger.info("[full_rt] === run_id=%s tables=%d topics=%d views=%d ===",
                        run_id[:16], len(table_names), len(topic_names), len(view_names))

            try:
                if not self._create_schema(prefix, table_names, topic_names, view_names):
                    break
                if not self._insert_rows(table_names):
                    break

                # Export
                logger.info("[full_rt] Starting export, base_path=%s", base_path)
                result = self.fs_client.export_to_fs(
                    base_path=base_path,
                    items=[(prefix, prefix)],
                    description=f"full_export_{run_id}",
                )
                self._inc_stat("export_started")
                logger.info("[full_rt] Export started: op=%s", result.id)

                status = self._wait_op(result.id, self._poll_export, "export")
                if status is None:
                    break
                if status != "DONE":
                    self._inc_stat("export_error")
                    self._signal_fatal_error(f"Export failed status={status} op={result.id}")
                    break
                self._inc_stat("export_done")
                logger.info("[full_rt] Export DONE")

                # Import into a different prefix (tables, topics, views)
                import_prefix = f"imp_{run_id}"
                all_source_names = table_names + topic_names + view_names
                import_items = [
                    (f"{prefix}/{name.split('/')[-1]}", f"{db_path}/{import_prefix}/{name.split('/')[-1]}")
                    for name in all_source_names
                ]
                logger.info("[full_rt] Starting import, %d items (tables=%d, topics=%d, views=%d)",
                            len(import_items), len(table_names), len(topic_names), len(view_names))
                imp_result = self.fs_client.import_from_fs(
                    base_path=base_path,
                    items=import_items,
                    description=f"full_import_{run_id}",
                )
                self._inc_stat("import_started")
                logger.info("[full_rt] Import started: op=%s", imp_result.id)

                imp_status = self._wait_op(imp_result.id, self._poll_import, "import")
                if imp_status is None:
                    break
                if imp_status != "DONE":
                    self._inc_stat("import_error")
                    self._signal_fatal_error(f"Import failed status={imp_status} op={imp_result.id}")
                    break
                self._inc_stat("import_done")
                logger.info("[full_rt] Import DONE")

                # Cleanup imported objects (views first, then topics, then tables)
                imported_views = [f"{import_prefix}/{name.split('/')[-1]}" for name in view_names]
                imported_topics = [f"{import_prefix}/{name.split('/')[-1]}" for name in topic_names]
                imported_tables = [f"{import_prefix}/{name.split('/')[-1]}" for name in table_names]
                self._drop_views(imported_views)
                self._drop_topics(imported_topics)
                self._drop_tables(imported_tables)

            except Exception as e:
                logger.error("[full_rt] Iteration failed: %s", e, exc_info=True)
                self._signal_fatal_error(f"Exception: {e}")
                break
            finally:
                self._drop_views(view_names)
                self._drop_topics(topic_names)
                self._drop_tables(table_names)
                try:
                    if os.path.exists(base_path):
                        shutil.rmtree(base_path, ignore_errors=True)
                except Exception:
                    pass

        logger.info("[full_rt] Stopped")

    def get_workload_thread_funcs(self):
        return [self._main_loop]


WORKLOADS = {
    "full_roundtrip": WorkloadFullRoundtrip,
    "single_table": WorkloadNfsExportImport,
}

DEFAULT_WORKLOAD = "full_roundtrip"


class WorkloadRunner:
    def __init__(self, client, duration, workload_names=None):
        self.client = client
        self.duration = duration
        self.workload_names = workload_names or [DEFAULT_WORKLOAD]
        self._temp_nfs_dir = None
        ydb.interceptor.monkey_patch_event_handler()

    def _setup_nfs(self):
        nfs_mount_path = os.getenv("NFS_MOUNT_PATH")
        logger.info("[setup] NFS_MOUNT_PATH=%s", nfs_mount_path)

        if not nfs_mount_path:
            self._temp_nfs_dir = tempfile.mkdtemp(prefix="nfs_stress_")
            nfs_mount_path = self._temp_nfs_dir
            logger.info("[setup] NFS_MOUNT_PATH not set, created temp dir: %s", nfs_mount_path)
        else:
            os.makedirs(nfs_mount_path, exist_ok=True)

        logger.info("[setup] NFS mount directory ready: %s", nfs_mount_path)
        return nfs_mount_path

    def _cleanup_temp_nfs(self):
        if self._temp_nfs_dir and os.path.exists(self._temp_nfs_dir):
            try:
                shutil.rmtree(self._temp_nfs_dir, ignore_errors=True)
                logger.info("[cleanup] Removed temp NFS dir: %s", self._temp_nfs_dir)
            except Exception as e:
                logger.warning("[cleanup] Failed to remove temp NFS dir %s: %s", self._temp_nfs_dir, e)
            self._temp_nfs_dir = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup_temp_nfs()

    def run(self):
        logger.info("[runner] Starting workload, duration=%ds", self.duration)
        stop = threading.Event()
        fatal_error = threading.Event()
        nfs_mount_path = self._setup_nfs()

        workloads = []
        for name in self.workload_names:
            cls = WORKLOADS[name]
            workloads.append(cls(self.client, stop, nfs_mount_path, fatal_error))
            logger.info("[runner] Registered workload: %s (%s)", name, cls.__name__)

        for w in workloads:
            w.start()
            logger.info("[runner] Started workload thread: %s", w.name)

        started_at = time.time()
        while time.time() - started_at < self.duration:
            if fatal_error.is_set():
                logger.error("[runner] Fatal error detected, stopping workload")
                break

            elapsed = int(time.time() - started_at)
            for w in workloads:
                stat = w.get_stat()
                msg = f"[runner] Elapsed {elapsed}s | {w.name}: {stat}"
                logger.info(msg)
                print(msg, file=sys.stderr)
            time.sleep(10)

        logger.info("[runner] Sending stop signal")
        stop.set()

        for w in workloads:
            logger.info("[runner] Waiting for %s to finish (timeout=30s)", w.name)
            w.join()
            if w.is_alive():
                logger.warning("[runner] %s did not stop within 30s", w.name)
            else:
                logger.info("[runner] %s finished", w.name)

        if fatal_error.is_set():
            logger.error("[runner] Workload terminated due to fatal error")
            raise RuntimeError("Workload failed due to export/import error")

        logger.info("[runner] All workloads stopped successfully")
