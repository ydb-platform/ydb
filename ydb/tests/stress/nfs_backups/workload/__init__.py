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

_TRANSIENT_ERRORS = (
    ydb_issues.ConnectionError,
    ydb_issues.Unavailable,
    ydb_issues.Overloaded,
    ydb_issues.Timeout,
    ydb_issues.Undetermined,
    ydb_issues.Aborted,
    ydb_issues.SessionBusy,
)

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


class NfsWorkloadBase(WorkloadBase):
    """Common base for NFS export/import workloads."""

    MAX_RETRIES = 5

    def __init__(self, client, stop, nfs_mount_path, fatal_error_event, workload_name, extra_stats=None):
        super().__init__(client, "", workload_name, stop)
        self.lock = threading.Lock()
        self.nfs_mount_path = nfs_mount_path
        self.fatal_error_event = fatal_error_event
        self.fs_client = FsExportClient(self.client.driver)
        self.op_client = OperationClient(self.client.driver)

        self._stats = {
            "export_started": 0,
            "export_done": 0,
            "export_error": 0,
            "import_started": 0,
            "import_done": 0,
            "import_error": 0,
        }
        if extra_stats:
            self._stats.update(extra_stats)

    @property
    def _log_prefix(self):
        return self.name

    def _should_stop(self):
        return self.is_stop_requested() or self.fatal_error_event.is_set()

    def get_stat(self):
        with self.lock:
            return ", ".join(f"{k}={v}" for k, v in self._stats.items())

    def _inc_stat(self, key):
        with self.lock:
            self._stats[key] += 1

    def _signal_fatal_error(self, message):
        logger.error("[%s][FATAL] %s", self._log_prefix, message)
        self.fatal_error_event.set()

    def _op_forget(self, op_id):
        try:
            self.op_client.forget(op_id)
        except Exception:
            pass

    def _poll_export(self, export_id):
        try:
            op = self.fs_client.get_export_operation(export_id)
            logger.debug("[%s][export] Poll op=%s ready=%s progress=%s",
                         self._log_prefix, export_id, op.ready, op.progress)
            if op.ready:
                return op.progress if op.progress != "UNSPECIFIED" else "DONE"
            return None
        except ydb_issues.NotFound:
            logger.debug("[%s][export] Poll op=%s: NOT_FOUND (treating as DONE)",
                         self._log_prefix, export_id)
            return "DONE"
        except _TRANSIENT_ERRORS as e:
            logger.warning("[%s][export] Poll op=%s transient error (will retry): %s",
                           self._log_prefix, export_id, e)
            return None

    def _poll_import(self, import_id):
        try:
            op = self.fs_client.get_import_operation(import_id)
            logger.debug("[%s][import] Poll op=%s ready=%s progress=%s",
                         self._log_prefix, import_id, op.ready, op.progress)
            if op.ready:
                return op.progress if op.progress != "UNSPECIFIED" else "DONE"
            return None
        except ydb_issues.NotFound:
            logger.debug("[%s][import] Poll op=%s: NOT_FOUND (treating as DONE)",
                         self._log_prefix, import_id)
            return "DONE"
        except _TRANSIENT_ERRORS as e:
            logger.warning("[%s][import] Poll op=%s transient error (will retry): %s",
                           self._log_prefix, import_id, e)
            return None

    def _wait_op(self, op_id, poll_fn):
        """Poll until operation reaches a terminal state. Returns status string or None on stop."""
        while True:
            if self._should_stop():
                return None
            status = poll_fn(op_id)
            if status is not None:
                self._op_forget(op_id)
                return status
            time.sleep(1)

    def _retry(self, action, description):
        last_error = None
        for attempt in range(self.MAX_RETRIES):
            if self._should_stop():
                return None
            try:
                return action()
            except _TRANSIENT_ERRORS as e:
                last_error = e
                logger.warning("[%s] %s transient error (attempt %d/%d): %s: %s",
                               self._log_prefix, description, attempt + 1, self.MAX_RETRIES,
                               type(e).__name__, e)
                time.sleep(2 ** attempt)
        return last_error

    def _cleanup_fs_path(self, base_path: str):
        try:
            if os.path.exists(base_path):
                shutil.rmtree(base_path, ignore_errors=True)
                logger.info("[%s][cleanup] Removed export directory: %s", self._log_prefix, base_path)
        except Exception as e:
            logger.warning("[%s][cleanup] Failed to remove directory %s: %s", self._log_prefix, base_path, e)


class WorkloadNfsExportImport(NfsWorkloadBase):
    TABLE_NAME = "large_test_table"

    def __init__(self, client, stop, nfs_mount_path, fatal_error_event):
        super().__init__(client, stop, nfs_mount_path, fatal_error_event, "nfs_export_import")

        self.export_in_progress = None  # (export_id, base_path, run_id) or None
        self.pending_import = None  # (base_path, run_id) or None

    def _wait_for_export(self) -> bool:
        """Wait for current export to complete. Returns False on error."""
        if self.export_in_progress is None:
            return True

        export_id, base_path, run_id = self.export_in_progress

        status = self._wait_op(export_id, self._poll_export)
        self.export_in_progress = None
        if status is None:
            return True

        if status == "DONE":
            self._inc_stat("export_done")
            logger.info("[%s] Export DONE: op=%s, queuing import", self._log_prefix, export_id[:16])
            self.pending_import = (base_path, run_id)
            return True

        self._inc_stat("export_error")
        logger.error("[%s] Export FAILED: op=%s status=%s base_path=%s. NOT cleaning up for investigation.",
                     self._log_prefix, export_id[:16], status, base_path)
        self._signal_fatal_error(f"Export failed with status={status}, op={export_id}")
        return False

    def _cleanup_imported_table(self, table_name: str):
        try:
            self.client.query(f"DROP TABLE `{table_name}`;", True)
            logger.info("[%s][cleanup] Dropped imported table: %s", self._log_prefix, table_name)
        except Exception as e:
            logger.warning("[%s][cleanup] Failed to drop table %s: %s", self._log_prefix, table_name, e)

    def _process_pending_import(self) -> bool:
        """Process pending import if any. Returns False on error."""
        if self.pending_import is None:
            return True

        base_path, run_id = self.pending_import
        self.pending_import = None

        db_path = self.client.database.rstrip("/")
        import_dest = f"{db_path}/imported_{run_id}"

        result = self._retry(
            lambda: self.fs_client.import_from_fs(
                base_path=base_path,
                items=[(self.TABLE_NAME, import_dest)],
                description=f"stress_import_{run_id}",
            ),
            "Import start",
        )

        if result is None or isinstance(result, Exception):
            if self._should_stop():
                return True
            self._inc_stat("import_error")
            logger.error("[%s] Import start failed after %d attempts for run_id=%s: %s",
                         self._log_prefix, self.MAX_RETRIES, run_id[:16], result)
            self._signal_fatal_error(f"Import start failed after retries for run_id={run_id}: {result}")
            return False

        self._inc_stat("import_started")
        logger.info("[%s] ImportFromFs started: op=%s", self._log_prefix, result.id)

        status = self._wait_op(result.id, self._poll_import)
        if status is None:
            return True

        if status == "DONE":
            self._inc_stat("import_done")
            logger.info("[%s] Import DONE: op=%s, cleaning up", self._log_prefix, result.id[:16])
            self._cleanup_imported_table(import_dest)
            self._cleanup_fs_path(base_path)
        else:
            self._inc_stat("import_error")
            logger.error("[%s] Import FAILED: op=%s status=%s. NOT cleaning up for investigation.",
                         self._log_prefix, result.id[:16], status)
            self._signal_fatal_error(f"Import failed with status={status}, op={result.id}")
            return False

        return True

    def _start_export(self, run_id: str) -> bool:
        """Start a new export. Retries on transient errors. Returns False on fatal error."""
        base_path = os.path.join(self.nfs_mount_path, f"export_{run_id}")

        result = self._retry(
            lambda: self.fs_client.export_to_fs(
                base_path=base_path,
                items=[(self.TABLE_NAME, self.TABLE_NAME)],
                description=f"stress_export_{run_id}",
            ),
            "Export start",
        )

        if result is None or isinstance(result, Exception):
            if self._should_stop():
                return False
            self._inc_stat("export_error")
            logger.error("[%s] Export start failed after %d attempts for run_id=%s: %s",
                         self._log_prefix, self.MAX_RETRIES, run_id[:16], result)
            self._signal_fatal_error(f"Export start failed after retries for run_id={run_id}: {result}")
            return False

        self._inc_stat("export_started")
        self.export_in_progress = (result.id, base_path, run_id)
        logger.info("[%s] ExportToFs started: op=%s progress=%s", self._log_prefix, result.id, result.progress)
        return True

    def _main_loop(self):
        logger.info("[%s] Starting export/import cycle, nfs_mount_path=%s", self._log_prefix, self.nfs_mount_path)

        try:
            _ensure_table_exists(self.client, self.TABLE_NAME)
        except Exception as e:
            self._signal_fatal_error(f"Cannot ensure table exists: {e}")
            return

        iteration = 0

        while not self._should_stop():
            iteration += 1
            run_id = f"{uuid.uuid1()}".replace("-", "_")
            logger.info("[%s] === Iteration %d, run_id=%s ===", self._log_prefix, iteration, run_id[:16])

            if not self._start_export(run_id):
                return
            if not self._wait_for_export():
                return
            if not self._process_pending_import():
                return

        logger.info("[%s] Stopped after %d iterations", self._log_prefix, iteration)

    def get_workload_thread_funcs(self):
        return [self._main_loop]


class WorkloadFullRoundtrip(NfsWorkloadBase):
    NUM_TABLES = 5
    NUM_ROWS = 100
    NUM_TOPICS = 3
    NUM_VIEWS = 3

    def __init__(self, client, stop, nfs_mount_path, fatal_error_event):
        super().__init__(client, stop, nfs_mount_path, fatal_error_event,
                         "nfs_full_roundtrip", extra_stats={"iterations": 0})

    def _create_schema(self, prefix, table_names, topic_names, view_names):
        for name in table_names:
            if self._should_stop():
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
        logger.info("[%s] Created %d tables under %s", self._log_prefix, len(table_names), prefix)

        for name in topic_names:
            if self._should_stop():
                return False
            self.client.query(f"CREATE TOPIC `{name}`;", True)
            self.client.query(f"ALTER TOPIC `{name}` ADD CONSUMER consumer_a;", True)
        logger.info("[%s] Created %d topics under %s", self._log_prefix, len(topic_names), prefix)

        for i, name in enumerate(view_names):
            if self._should_stop():
                return False
            src_table = table_names[i % len(table_names)]
            self.client.query(
                f"CREATE VIEW `{name}` WITH security_invoker = TRUE AS SELECT * FROM `{src_table}`;",
                True,
            )
        logger.info("[%s] Created %d views under %s", self._log_prefix, len(view_names), prefix)

        return True

    def _insert_rows(self, table_names):
        for table in table_names:
            if self._should_stop():
                return False
            for batch_start in range(0, self.NUM_ROWS, 10):
                if self._should_stop():
                    return False
                values = ", ".join(
                    f"({row_id}, 'row_{row_id}_in_{table.rsplit('/', 1)[-1]}')"
                    for row_id in range(batch_start, min(batch_start + 10, self.NUM_ROWS))
                )
                self.client.query(
                    f"INSERT INTO `{table}` (id, payload) VALUES {values};",
                    False,
                )
        logger.info("[%s] Inserted %d rows into %d tables", self._log_prefix, self.NUM_ROWS, len(table_names))
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
        logger.info("[%s] Started, nfs_mount_path=%s", self._log_prefix, self.nfs_mount_path)

        while not self._should_stop():
            self._inc_stat("iterations")
            run_id = f"{uuid.uuid1()}".replace("-", "_")
            prefix = f"full_{run_id}"
            base_path = os.path.join(self.nfs_mount_path, f"full_{run_id}")
            db_path = self.client.database.rstrip("/")

            table_names = [f"{prefix}/tbl{i}" for i in range(self.NUM_TABLES)]
            topic_names = [f"{prefix}/topic{i}" for i in range(self.NUM_TOPICS)]
            view_names = [f"{prefix}/view{i}" for i in range(self.NUM_VIEWS)]

            logger.info("[%s] === run_id=%s tables=%d topics=%d views=%d ===",
                        self._log_prefix, run_id[:16], len(table_names), len(topic_names), len(view_names))

            try:
                if not self._create_schema(prefix, table_names, topic_names, view_names):
                    break
                if not self._insert_rows(table_names):
                    break

                # Export
                logger.info("[%s] Starting export, base_path=%s", self._log_prefix, base_path)
                result = self._retry(
                    lambda: self.fs_client.export_to_fs(
                        base_path=base_path,
                        items=[(prefix, prefix)],
                        description=f"full_export_{run_id}",
                    ),
                    "Export start",
                )
                if result is None or isinstance(result, Exception):
                    if not self._should_stop():
                        self._inc_stat("export_error")
                        self._signal_fatal_error(f"Export start failed after retries run_id={run_id}: {result}")
                    break
                self._inc_stat("export_started")
                logger.info("[%s] Export started: op=%s", self._log_prefix, result.id)

                status = self._wait_op(result.id, self._poll_export)
                if status is None:
                    break
                if status != "DONE":
                    self._inc_stat("export_error")
                    self._signal_fatal_error(f"Export failed status={status} op={result.id}")
                    break
                self._inc_stat("export_done")
                logger.info("[%s] Export DONE", self._log_prefix)

                # Import into a different prefix
                import_prefix = f"imp_{run_id}"
                all_source_names = table_names + topic_names + view_names
                import_items = [
                    (f"{prefix}/{name.split('/')[-1]}", f"{db_path}/{import_prefix}/{name.split('/')[-1]}")
                    for name in all_source_names
                ]
                logger.info("[%s] Starting import, %d items (tables=%d, topics=%d, views=%d)",
                            self._log_prefix, len(import_items), len(table_names), len(topic_names), len(view_names))
                imp_result = self._retry(
                    lambda: self.fs_client.import_from_fs(
                        base_path=base_path,
                        items=import_items,
                        description=f"full_import_{run_id}",
                    ),
                    "Import start",
                )
                if imp_result is None or isinstance(imp_result, Exception):
                    if not self._should_stop():
                        self._inc_stat("import_error")
                        self._signal_fatal_error(f"Import start failed after retries run_id={run_id}: {imp_result}")
                    break
                self._inc_stat("import_started")
                logger.info("[%s] Import started: op=%s", self._log_prefix, imp_result.id)

                imp_status = self._wait_op(imp_result.id, self._poll_import)
                if imp_status is None:
                    break
                if imp_status != "DONE":
                    self._inc_stat("import_error")
                    self._signal_fatal_error(f"Import failed status={imp_status} op={imp_result.id}")
                    break
                self._inc_stat("import_done")
                logger.info("[%s] Import DONE", self._log_prefix)

                # Cleanup imported objects
                imported_views = [f"{import_prefix}/{name.split('/')[-1]}" for name in view_names]
                imported_topics = [f"{import_prefix}/{name.split('/')[-1]}" for name in topic_names]
                imported_tables = [f"{import_prefix}/{name.split('/')[-1]}" for name in table_names]
                self._drop_views(imported_views)
                self._drop_topics(imported_topics)
                self._drop_tables(imported_tables)

            except Exception as e:
                logger.error("[%s] Iteration failed: %s", self._log_prefix, e, exc_info=True)
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

        logger.info("[%s] Stopped", self._log_prefix)

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
