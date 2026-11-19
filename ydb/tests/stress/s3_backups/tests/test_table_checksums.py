from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
from threading import Event
import time
import uuid
import yatest.common

import grpc
import pytest

from ydb.operation import OperationClient
from ydb.tests.library.backup import BackupStorage, checksum_metadata
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestTableChecksums(StressFixture):
    @pytest.fixture(autouse=True)
    def setup(self):
        yield from self.setup_cluster(
            nodes=2,
            use_in_memory_pdisks=True,
            extra_feature_flags={
                "enable_backup_table_checksum": True,
                "enable_checksums_export": True,
                "enable_fs_backups": True,
                "enable_export_auto_dropping": True,
                "enable_snapshots_locking": True,
                "enable_data_shard_snapshot_locking": True,
            },
        )

    @pytest.mark.parametrize("storage", ["s3", "fs"])
    def test_concurrent_exports_imports(self, storage):
        source = f"{self.database}/checksum_source"
        stop = Event()
        artifacts = Path(yatest.common.test_output_path("checksum_backups")) / f"{storage}_{uuid.uuid4().hex}"
        with ydb.SessionPool(self.driver, size=8) as pool:
            pool.retry_operation_sync(lambda session: session.execute_scheme(
                f"CREATE TABLE `{source}` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key)) "
                "WITH (UNIFORM_PARTITIONS = 4);"
            ))
            for start in range(0, 1024, 64):
                values = ",".join(f'({key}ul, "initial_{key}"u)' for key in range(start, start + 64))
                pool.retry_operation_sync(lambda session: session.transaction().execute(
                    f"UPSERT INTO `{source}` (key, value) VALUES {values};", commit_tx=True
                ))
            deadline = time.monotonic() + int(self.base_duration)

            def mutate():
                iteration = 0
                while not stop.is_set() and time.monotonic() < deadline:
                    first = (iteration * 16) % 1024
                    values = ",".join(f'({key}ul, "version_{iteration}"u)' for key in range(first, first + 16))
                    pool.retry_operation_sync(lambda session: session.transaction().execute(
                        f"UPSERT INTO `{source}` (key, value) VALUES {values}; "
                        f"DELETE FROM `{source}` WHERE key >= {first}ul AND key < {first + 4}ul;",
                        commit_tx=True,
                    ))
                    iteration += 1
                return iteration

            def backup_worker(worker):
                directory = artifacts / str(worker)
                directory.mkdir(parents=True)
                completed = 0
                with grpc.insecure_channel(self.endpoint.removeprefix("grpc://")) as channel:
                    backup = BackupStorage(storage, directory, channel, self.database)
                    operations = OperationClient(self.driver)
                    while completed == 0 or (not stop.is_set() and time.monotonic() < deadline):
                        prefix = f"iteration_{completed}"
                        destination = f"{self.database}/restored_{worker}_{completed}"
                        exported = backup.wait(backup.start_export([(source, prefix)]))
                        expected = json.loads(backup.read("metadata.json", prefix))["table_checksum"]
                        assert set(expected) == {"version", "algorithm", "count", "hash"}, expected
                        try:
                            imported = backup.restore(destination, prefix=prefix)
                        except Exception as error:
                            diagnostics = {"prefix": prefix, "expected": expected, "error": str(error)}
                            try:
                                rows = pool.retry_operation_sync(lambda session: session.transaction().execute(
                                    f"SELECT key, value FROM `{destination}` ORDER BY key;", commit_tx=True
                                ))[0].rows
                                diagnostics["restored_rows"] = [(row.key, row.value) for row in rows]
                            except Exception as read_error:
                                diagnostics["read_error"] = str(read_error)
                            (directory / "failure.json").write_text(json.dumps(diagnostics, indent=2))
                            raise
                        result = pool.retry_operation_sync(lambda session: session.transaction().execute(
                            f"CHECKSUM TABLE `{destination}`;", commit_tx=True
                        ))
                        assert checksum_metadata(result[0].rows[0]) == expected
                        pool.retry_operation_sync(lambda session: session.execute_scheme(f"DROP TABLE `{destination}`;"))
                        operations.forget(imported.id)
                        operations.forget(exported.id)
                        completed += 1
                return completed

            with ThreadPoolExecutor(max_workers=4) as executor:
                writer = executor.submit(mutate)
                workers = [executor.submit(backup_worker, worker) for worker in range(3)]
                try:
                    completed = [worker.result() for worker in workers]
                    assert all(count > 0 for count in completed), completed
                finally:
                    stop.set()
                    writer.result()
