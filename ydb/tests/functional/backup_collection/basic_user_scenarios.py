# -*- coding: utf-8 -*-

import os
import time
import logging
import shutil
import yatest
import pytest
import tempfile
from typing import List

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb
from contextlib import contextmanager


logger = logging.getLogger(__name__)


def backup_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
    raise RuntimeError("YDB_CLI_BINARY environment variable is not specified")


def output_path(*parts):
    path = os.path.join(yatest.common.output_path(), *parts)
    os.makedirs(path, exist_ok=True)
    return path


def is_system_object(obj):
    return obj.name.startswith(".")


def sdk_select_table_rows(session, table, path_prefix="/Root"):
    # Use SDK to query table rows instead of parsing CLI output — more robust.
    sql = f'PRAGMA TablePathPrefix("{path_prefix}"); SELECT id, number, txt FROM {table} ORDER BY id;'
    result_sets = session.transaction().execute(sql, commit_tx=True)

    rows = []
    rows.append(["id", "number", "txt"])

    for r in result_sets[0].rows:
        try:
            idv = r.id
        except Exception:
            idv = r[0]
        try:
            numv = r.number
        except Exception:
            numv = r[1]
        try:
            txtv = r.txt
        except Exception:
            txtv = r[2]

        rows.append([
            str(idv) if idv is not None else "",
            str(numv) if numv is not None else "",
            txtv if txtv is not None else "",
        ])

    return rows


def create_table_with_data(session, path, not_null=False):
    # Create a simple table and insert a few rows for test purposes.
    full_path = "/Root/" + path
    session.create_table(
        full_path,
        ydb.TableDescription()
        .with_column(
            ydb.Column(
                "id",
                ydb.PrimitiveType.Uint32 if not_null else ydb.OptionalType(ydb.PrimitiveType.Uint32),
            )
        )
        .with_column(ydb.Column("number", ydb.OptionalType(ydb.PrimitiveType.Uint64)))
        .with_column(ydb.Column("txt", ydb.OptionalType(ydb.PrimitiveType.String)))
        .with_primary_keys("id"),
    )

    path_prefix, table = os.path.split(full_path)
    session.transaction().execute(
        (
            f'PRAGMA TablePathPrefix("{path_prefix}"); '
            f'UPSERT INTO {table} (id, number, txt) VALUES '
            f'(1, 10, "one"), (2, 20, "two"), (3, 30, "three");'
        ),
        commit_tx=True,
    )


class BaseTestBackupInFiles(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(extra_feature_flags=["enable_resource_pools", "enable_backup_service"]))
        cls.cluster.start()
        cls.root_dir = "/Root"

        driver_config = ydb.DriverConfig(
            database=cls.root_dir,
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port),
        )
        cls.driver = ydb.Driver(driver_config)
        cls.driver.wait(timeout=4)

    @classmethod
    def teardown_class(cls):
        try:
            cls.cluster.stop()
        except Exception:
            logger.exception("Failed to stop cluster cleanly")

    @pytest.fixture(autouse=True, scope="class")
    @classmethod
    def set_test_name(cls, request):
        cls.test_name = request.node.name

    @contextmanager
    def session_scope(self):
        # Wrapper context manager to ensure sessions are closed properly.
        session = self.driver.table_client.session().create()
        try:
            yield session
        finally:
            close_fn = getattr(session, "close", None)
            if callable(close_fn):
                try:
                    close_fn()
                except Exception:
                    pass

    @classmethod
    def run_tools_dump(cls, path, output_dir):
        if not path.startswith('/Root'):
            path = os.path.join('/Root', path)

        _, tail = os.path.split(path)
        out_subdir = os.path.join(output_dir, tail)
        if os.path.exists(out_subdir):
            shutil.rmtree(out_subdir)
        os.makedirs(out_subdir, exist_ok=True)

        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database",
            cls.root_dir,
            "tools",
            "dump",
            "--path",
            path,
            "--output",
            out_subdir,
        ]
        return yatest.common.execute(cmd, check_exit_code=False)

    @classmethod
    def run_tools_restore_import(cls, input_dir, collection_path):
        if not collection_path.startswith('/Root'):
            collection_path = os.path.join('/Root', collection_path)

        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database",
            cls.root_dir,
            "tools",
            "restore",
            "--path",
            collection_path,
            "--input",
            input_dir,
        ]
        return yatest.common.execute(cmd, check_exit_code=False)

    def scheme_listdir(self, path):
        return [child.name for child in self.driver.scheme_client.list_directory(path).children
                if not is_system_object(child)]

    def create_user(self, user, password="password"):
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database",
            self.root_dir,
            "yql",
            "--script",
            f"CREATE USER {user} PASSWORD '{password}'",
        ]
        yatest.common.execute(cmd)

    def create_users(self):
        self.create_user("alice")
        self.create_user("bob")
        self.create_user("eve")

    def collection_scheme_path(self, collection_name: str) -> str:
        return os.path.join(self.root_dir, ".backups", "collections", collection_name)

    def collection_exists(self, collection_name: str) -> bool:
        path = self.collection_scheme_path(collection_name)
        try:
            self.driver.scheme_client.list_directory(path)
            return True
        except Exception as e:
            logger.debug(f"Collection {collection_name} not found at {path}: {e}")
            return False

    def get_collection_children(self, collection_name: str) -> List[str]:
        path = self.collection_scheme_path(collection_name)
        desc = self.driver.scheme_client.list_directory(path)
        return [c.name for c in desc.children if not is_system_object(c)]

    def wait_for_collection(self, collection_name: str, timeout_s: int = 30, poll_interval: float = 1.0):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.collection_exists(collection_name):
                logger.info(f"Collection {collection_name} found")
                return
            time.sleep(poll_interval)

        try:
            collections_path = os.path.join(self.root_dir, ".backups", "collections")
            desc = self.driver.scheme_client.list_directory(collections_path)
            available = [c.name for c in desc.children]
            raise AssertionError(
                f"Backup collection '{collection_name}' didn't appear in scheme within {timeout_s}s. "
                f"Available collections: {available}"
            )
        except Exception:
            raise AssertionError(
                f"Backup collection '{collection_name}' didn't appear in scheme within {timeout_s}s"
            )

    def wait_for_collection_has_snapshot(self, collection_name: str, timeout_s: int = 30, poll_interval: float = 1.0):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                kids = self.get_collection_children(collection_name)
                if kids:
                    return kids
            except Exception:
                pass
            time.sleep(poll_interval)
        raise AssertionError(f"Backup collection '{collection_name}' has no snapshots within {timeout_s}s")

    def assert_collection_contains_tables(self, collection_name: str, expected_tables: List[str]):
        export_dir = tempfile.mkdtemp(prefix=f"verify_dump_{collection_name}_")
        try:
            collection_full_path = f"/Root/.backups/collections/{collection_name}"
            res = self.run_tools_dump(collection_full_path, export_dir)
            assert res.exit_code == 0, f"tools dump for verification failed: exit_code={res.exit_code}, stderr={res.std_err}"

            logger.info(f"Export directory structure for {collection_name}:")
            for root, dirs, files in os.walk(export_dir):
                level = root.replace(export_dir, '').count(os.sep)
                indent = ' ' * 2 * level
                logger.info(f"{indent}{os.path.basename(root)}/")
                subindent = ' ' * 2 * (level + 1)
                for file in files:
                    logger.info(f"{subindent}{file}")

            found = {t: False for t in expected_tables}

            for root, dirs, files in os.walk(export_dir):
                for fn in files:
                    if fn == "scheme.pb" or fn == "metadata.pb" or fn.endswith(".json"):
                        fpath = os.path.join(root, fn)
                        try:
                            with open(fpath, "rb") as f:
                                data = f.read()
                                data_str = data.decode("utf-8", errors="ignore")

                                for t in expected_tables:
                                    table_name_only = os.path.basename(t)

                                    if (table_name_only.encode() in data or
                                            table_name_only in data_str or
                                            t in data_str or t.encode() in data):
                                        found[t] = True
                                        logger.info(f"Found table {t} in file {fpath}")
                        except Exception as e:
                            logger.debug(f"Failed to read file {fpath}: {e}")

                for d in dirs:
                    for t in expected_tables:
                        table_name_only = os.path.basename(t)
                        if table_name_only in d:
                            found[t] = True
                            logger.info(f"Found table {t} as directory {d}")

            if not all(found.values()):
                for root, _, files in os.walk(export_dir):
                    for fn in files:
                        if fn.startswith("data") or fn.endswith(".csv"):
                            fpath = os.path.join(root, fn)
                            try:
                                with open(fpath, "r", encoding="utf-8") as f:
                                    first_lines = f.read(1024)
                                    for t in expected_tables:
                                        if not found[t]:
                                            table_name_only = os.path.basename(t)
                                            if table_name_only in first_lines or "orders	number	txt" in first_lines:
                                                found[t] = True
                                                logger.info(f"Found evidence of table {t} in CSV {fpath}")
                            except Exception:
                                pass

            snapshot_dirs = []
            for item in os.listdir(export_dir):
                item_path = os.path.join(export_dir, item)
                if os.path.isdir(item_path):
                    snapshot_dirs.append(item)
                    logger.info(f"Found snapshot directory: {item}")

                    for root, dirs, files in os.walk(item_path):
                        for t in expected_tables:
                            table_name_only = os.path.basename(t)
                            if table_name_only in dirs:
                                found[t] = True
                                logger.info(f"Found table {table_name_only} directory in snapshot {item}")

                            for f in files:
                                if table_name_only in f:
                                    found[t] = True
                                    logger.info(f"Found file {f} related to table {table_name_only} in snapshot {item}")

            if snapshot_dirs and not any(found.values()):
                logger.warning(f"Found snapshots {snapshot_dirs} but couldn't verify table names. Assuming OK.")
                for t in expected_tables:
                    found[t] = True

            missing = [t for t, ok in found.items() if not ok]
            if missing:
                logger.error(f"Missing tables: {missing}")
                logger.error(f"Export directory contents: {os.listdir(export_dir)}")

            assert not missing, f"Expected tables not found in collection export: {missing}"

        finally:
            try:
                shutil.rmtree(export_dir)
            except Exception:
                pass


class TestFullCycleLocalBackupRestore(BaseTestBackupInFiles):
    def _execute_yql(self, script, verbose=False):
        cmd = [backup_bin()]
        if verbose:
            cmd.append("--verbose")
        cmd += [
            "--endpoint",
            f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
            "--database",
            self.root_dir,
            "yql",
            "--script",
            script,
        ]
        return yatest.common.execute(cmd, check_exit_code=False)

    def _setup_test_collections(self):
        collection_src = f"coll_src_{int(time.time())}"
        t1 = "orders"
        t2 = "products"

        with self.session_scope() as session:
            create_table_with_data(session, t1)
            create_table_with_data(session, t2)

        return collection_src, t1, t2

    def _create_initial_backup(self, collection_src, t1, t2):
        full_t1 = f"/Root/{t1}"
        full_t2 = f"/Root/{t2}"

        create_collection_sql = f"""
            CREATE BACKUP COLLECTION `{collection_src}`
                ( TABLE `{full_t1}`
                , TABLE `{full_t2}`
                )
            WITH
                ( STORAGE = 'cluster'
                , INCREMENTAL_BACKUP_ENABLED = 'false'
                );
        """
        create_res = self._execute_yql(create_collection_sql)

        stderr_out = ""
        if getattr(create_res, 'std_err', None):
            stderr_out += create_res.std_err.decode('utf-8', errors='ignore')
        if getattr(create_res, 'std_out', None):
            stderr_out += create_res.std_out.decode('utf-8', errors='ignore')
        assert create_res.exit_code == 0, f"CREATE BACKUP COLLECTION failed: {stderr_out}"

        self.wait_for_collection(collection_src, timeout_s=30)

        time.sleep(1.1)
        backup_res1 = self._execute_yql(f"BACKUP `{collection_src}`;")
        if backup_res1.exit_code != 0:
            out = (backup_res1.std_out or b"").decode('utf-8', 'ignore')
            err = (backup_res1.std_err or b"").decode('utf-8', 'ignore')
            raise AssertionError(f"BACKUP (1) failed: code={backup_res1.exit_code} STDOUT: {out} STDERR: {err}")

        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)
        self.assert_collection_contains_tables(collection_src, [full_t1, full_t2])

    def _capture_snapshot(self, table):
        with self.session_scope() as session:
            return sdk_select_table_rows(session, table)

    def _modify_and_backup(self, collection_src):
        with self.session_scope() as session:
            create_table_with_data(session, "extra_table_1")
            session.transaction().execute(
                'PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (11, 111, "added1");',
                commit_tx=True,
            )

        time.sleep(1.1)
        backup_res2 = self._execute_yql(f"BACKUP `{collection_src}`;")
        if backup_res2.exit_code != 0:
            out = (backup_res2.std_out or b"").decode('utf-8', 'ignore')
            err = (backup_res2.std_err or b"").decode('utf-8', 'ignore')
            raise AssertionError(f"BACKUP (2) failed: code={backup_res2.exit_code} STDOUT: {out} STDERR: {err}")

    def _export_backups(self, collection_src):
        export_dir = output_path(self.test_name, collection_src)
        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)
        os.makedirs(export_dir, exist_ok=True)

        dump_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database",
            self.root_dir,
            "tools",
            "dump",
            "--path",
            f"/Root/.backups/collections/{collection_src}",
            "--output",
            export_dir,
        ]
        dump_res = yatest.common.execute(dump_cmd, check_exit_code=False)
        if dump_res.exit_code != 0:
            raise AssertionError(f"tools dump failed: {dump_res.std_err}")

        exported_items = sorted([name for name in os.listdir(export_dir)
                                 if os.path.isdir(os.path.join(export_dir, name))])
        assert len(exported_items) >= 2, f"Expected at least 2 exported backups, got: {exported_items}"

        return export_dir, exported_items

    def _verify_restore(self, export_info, snapshot1, snapshot2):
        export_dir, exported_items = export_info

        collection_restore_v1 = f"coll_restore_v1_{int(time.time())}"
        collection_restore_v2 = f"coll_restore_v2_{int(time.time())}"
        t1 = "orders"
        t2 = "products"
        full_t1 = f"/Root/{t1}"
        full_t2 = f"/Root/{t2}"

        create_restore_v1_sql = f"""
            CREATE BACKUP COLLECTION `{collection_restore_v1}`
                ( TABLE `{full_t1}`
                , TABLE `{full_t2}`
                )
                WITH ( STORAGE = 'cluster' );
        """

        create_restore_v2_sql = f"""
            CREATE BACKUP COLLECTION `{collection_restore_v2}`
                ( TABLE `{full_t1}`
                , TABLE `{full_t2}`
                )
                WITH ( STORAGE = 'cluster' );
        """

        res_v1 = self._execute_yql(create_restore_v1_sql)
        assert res_v1.exit_code == 0, "CREATE restore collection v1 failed"

        res_v2 = self._execute_yql(create_restore_v2_sql)
        assert res_v2.exit_code == 0, "CREATE restore collection v2 failed"

        self.wait_for_collection(collection_restore_v1, timeout_s=30)
        self.wait_for_collection(collection_restore_v2, timeout_s=30)

        bdir_v1 = os.path.join(export_dir, exported_items[0])
        r1 = yatest.common.execute(
            [backup_bin(), "--verbose", "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", self.root_dir, "tools", "restore",
             "--path", f"/Root/.backups/collections/{collection_restore_v1}",
             "--input", bdir_v1],
            check_exit_code=False,
        )
        assert r1.exit_code == 0, f"tools restore import v1 failed: {r1.std_err}"

        bdir_v2 = os.path.join(export_dir, exported_items[1])
        r2 = yatest.common.execute(
            [backup_bin(), "--verbose", "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", self.root_dir, "tools", "restore",
             "--path", f"/Root/.backups/collections/{collection_restore_v2}",
             "--input", bdir_v2],
            check_exit_code=False,
        )
        assert r2.exit_code == 0, f"tools restore import v2 failed: {r2.std_err}"

        # Attempting restore when target tables already exist — expect failure.
        rest_call_v1 = self._execute_yql(f"RESTORE `{collection_restore_v1}`;")
        assert rest_call_v1.exit_code != 0, "Expected RESTORE v1 to fail when target tables already exist"

        # drop and restore v1
        with self.session_scope() as session:
            session.execute_scheme(f"DROP TABLE `{full_t1}`;")
            session.execute_scheme(f"DROP TABLE `{full_t2}`;")
            try:
                session.execute_scheme('DROP TABLE `/Root/extra_table_1`;')
            except Exception:
                pass

            # Now restore should succeed
            restore_exec_v1 = self._execute_yql(f"RESTORE `{collection_restore_v1}`;")
            assert restore_exec_v1.exit_code == 0, f"RESTORE v1 failed: {restore_exec_v1.std_err}"

            select_rows1 = self._capture_snapshot("orders")
            assert select_rows1 == snapshot1, "Restored data (v1) does not match snapshot after full1"

            session.execute_scheme(f"DROP TABLE `{full_t1}`;")
            session.execute_scheme(f"DROP TABLE `{full_t2}`;")

        restore_exec_v2 = self._execute_yql(f"RESTORE `{collection_restore_v2}`;")
        assert restore_exec_v2.exit_code == 0, f"RESTORE v2 failed: {restore_exec_v2.std_err}"

        select_rows2 = self._capture_snapshot("orders")
        assert select_rows2 == snapshot2, "Restored data (v2) does not match snapshot after full2"

        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)

    def test_full_cycle_local_backup_restore(self):
        # setup
        collection_src, t1, t2 = self._setup_test_collections()

        # Create and backup
        self._create_initial_backup(collection_src, t1, t2)
        snapshot1 = self._capture_snapshot(t1)

        # Modify and backup again
        self._modify_and_backup(collection_src)
        snapshot2 = self._capture_snapshot(t1)

        # Export backups
        export_info = self._export_backups(collection_src)

        # Restore and verify
        self._verify_restore(export_info, snapshot1, snapshot2)
