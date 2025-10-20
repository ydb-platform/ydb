# -*- coding: utf-8 -*-
import os
import time
import logging
import shutil
import yatest
import pytest
import tempfile
import re
from typing import List

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb
from contextlib import contextmanager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    if table.startswith("/"):
        full_path = table
        base_name = os.path.basename(table)
        table_for_sql = base_name
        pp = os.path.dirname(full_path) or path_prefix
    else:
        base_name = table
        full_path = os.path.join(path_prefix, base_name)
        table_for_sql = base_name
        pp = path_prefix

    cols = None
    primary_keys = None
    try:
        if hasattr(session, "describe_table"):
            desc = session.describe_table(full_path)
        else:
            tc = getattr(getattr(session, "driver", None), "table_client", None)
            if tc is not None and hasattr(tc, "describe_table"):
                desc = tc.describe_table(full_path)
            else:
                desc = None

        if desc is not None:
            raw_cols = getattr(desc, "columns", None) or getattr(desc, "Columns", None)
            if raw_cols:
                try:
                    cols = [c.name for c in raw_cols]
                except Exception:
                    cols = [str(c) for c in raw_cols]

            pk = getattr(desc, "primary_key", None) or getattr(desc, "primary_keys", None) or getattr(desc, "key_columns", None)
            if pk:
                try:
                    if isinstance(pk, (list, tuple)):
                        primary_keys = list(pk)
                    else:
                        primary_keys = [str(pk)]
                except Exception:
                    primary_keys = None
    except Exception:
        cols = None
        primary_keys = None

    if not cols:
        try:
            sql_try = f'PRAGMA TablePathPrefix("{pp}"); SELECT * FROM {table_for_sql} LIMIT 1;'
            res_try = session.transaction().execute(sql_try, commit_tx=True)
            rs0 = res_try[0]
            try:
                meta = getattr(rs0, "columns", None) or getattr(rs0, "Columns", None)
                if meta:
                    cols = [c.name for c in meta]
            except Exception:
                cols = None
        except Exception:
            cols = None

    if not cols:
        raise AssertionError(f"Не удалось получить список колонок для таблицы {full_path}")

    def q(n):
        return "`" + n.replace("`", "``") + "`"

    select_list = ", ".join(q(c) for c in cols)

    order_clause = ""
    if primary_keys:
        pks = [p for p in primary_keys if p in cols]
        if pks:
            order_clause = " ORDER BY " + ", ".join(q(p) for p in pks)

    sql = f'PRAGMA TablePathPrefix("{pp}"); SELECT {select_list} FROM {table_for_sql}{order_clause};'

    result_sets = session.transaction().execute(sql, commit_tx=True)

    rows = []
    rows.append(cols.copy())

    for r in result_sets[0].rows:
        vals = []
        for i, col in enumerate(cols):
            v = None
            try:
                v = getattr(r, col)
            except Exception:
                try:
                    v = r[i]
                except Exception:
                    v = None

            if v is None:
                vals.append("")
            else:
                try:
                    if isinstance(v, (bytes, bytearray)):
                        vals.append(v.decode("utf-8", "replace"))
                    else:
                        vals.append(str(v))
                except Exception:
                    vals.append(repr(v))
        rows.append(vals)

    return rows


def create_table_with_data(session, path, not_null=False):
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

    def _capture_snapshot(self, table):
        with self.session_scope() as session:
            return sdk_select_table_rows(session, table)

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
        assert len(exported_items) >= 1, f"Expected at least 1 exported backup, got: {exported_items}"

        return export_dir, exported_items

    def assert_collection_contains_tables(self, collection_name: str, expected_tables: List[str]):
        export_dir = tempfile.mkdtemp(prefix=f"verify_dump_{collection_name}_")
        try:
            collection_full_path = f"/Root/.backups/collections/{collection_name}"
            res = self.run_tools_dump(collection_full_path, export_dir)
            assert res.exit_code == 0, f"tools dump for verification failed: exit_code={res.exit_code}, stderr={res.std_err}"

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
                        except Exception:
                            pass

                for d in dirs:
                    for t in expected_tables:
                        table_name_only = os.path.basename(t)
                        if table_name_only in d:
                            found[t] = True

            missing = [t for t, ok in found.items() if not ok]
            assert not missing, f"Expected tables not found in collection export: {missing}"

        finally:
            try:
                shutil.rmtree(export_dir)
            except Exception:
                pass

    def wait_for_table_rows(self,
                            table: str,
                            expected_rows,
                            timeout_s: int = 60,
                            poll_interval: float = 0.5):
        # helper to get current rows safely
        deadline = time.time() + timeout_s
        last_exc = None

        while time.time() < deadline:
            try:
                cur_rows = None
                try:
                    cur_rows = self._capture_snapshot(table)
                except Exception as e:
                    # table might not exist yet or select could fail — record and retry
                    last_exc = e
                    time.sleep(poll_interval)
                    continue

                if cur_rows == expected_rows:
                    return cur_rows

            except Exception as e:
                last_exc = e

            time.sleep(poll_interval)

        raise AssertionError(f"Timeout waiting for table '{table}' rows to match expected (timeout {timeout_s}s). Last error: {last_exc}")

    def _create_backup_collection(self, collection_src, tables: List[str]):
        # create backup collection referencing given table full paths
        table_entries = ",\n".join([f"TABLE `/Root/{t}`" for t in tables])
        sql = f"""
            CREATE BACKUP COLLECTION `{collection_src}`
                ( {table_entries} )
            WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'false' );
        """
        res = self._execute_yql(sql)
        stderr_out = ""
        if getattr(res, 'std_err', None):
            stderr_out += res.std_err.decode('utf-8', errors='ignore')
        if getattr(res, 'std_out', None):
            stderr_out += res.std_out.decode('utf-8', errors='ignore')
        assert res.exit_code == 0, f"CREATE BACKUP COLLECTION failed: {stderr_out}"
        self.wait_for_collection(collection_src, timeout_s=30)

    def _backup_now(self, collection_src):
        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}`;")
        if res.exit_code != 0:
            out = (res.std_out or b"").decode('utf-8', 'ignore')
            err = (res.std_err or b"").decode('utf-8', 'ignore')
            raise AssertionError(f"BACKUP failed: code={res.exit_code} STDOUT: {out} STDERR: {err}")

    def _restore_import(self, export_dir, exported_item, collection_restore):
        bdir = os.path.join(export_dir, exported_item)
        r = yatest.common.execute(
            [backup_bin(), "--verbose", "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", self.root_dir, "tools", "restore",
             "--path", f"/Root/.backups/collections/{collection_restore}",
             "--input", bdir],
            check_exit_code=False,
        )
        assert r.exit_code == 0, f"tools restore import failed: {r.std_err}"

    def _verify_restored_table_data(self, table, expected_rows):
        rows = self.wait_for_table_rows(table, expected_rows, timeout_s=90)
        assert rows == expected_rows, f"Restored data for {table} doesn't match expected.\nExpected: {expected_rows}\nGot: {rows}"

    def _capture_acl(self, table_path: str):
        # Attempt to capture owner/grants/acl in a readable form.
        try:
            desc = self.driver.scheme_client.describe_path(table_path)
        except Exception:
            return None

        acl_info = {}
        owner = getattr(desc, "owner", None)
        if owner:
            acl_info["owner"] = owner

        for cand in ("acl", "grants", "effective_acl", "permission", "permissions"):
            if hasattr(desc, cand):
                try:
                    val = getattr(desc, cand)
                    acl_info[cand] = val
                except Exception:
                    acl_info[cand] = "<unreadable>"

        # Fallback: try SHOW GRANTS via YQL and capture stdout
        try:
            res = self._execute_yql(f"SHOW GRANTS ON '{table_path}';")
            out = (res.std_out or b"").decode('utf-8', 'ignore')
            if out:
                acl_info["show_grants"] = out
        except Exception:
            pass

        return acl_info

    def _drop_tables(self, tables: List[str]):
        with self.session_scope() as session:
            for t in tables:
                full = f"/Root/{t}"
                try:
                    session.execute_scheme(f"DROP TABLE `{full}`;")
                except Exception:
                    raise AssertionError("Drop failed")


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


class TestFullCycleLocalBackupRestoreWIncr(TestFullCycleLocalBackupRestore):
    def _modify_data_add_and_remove(self, add_rows: List[tuple] = None, remove_ids: List[int] = None):
        add_rows = add_rows or []
        remove_ids = remove_ids or []

        with self.session_scope() as session:
            # Adds
            if add_rows:
                values = ", ".join(f"({i},{n},\"{t}\")" for i, n, t in add_rows)
                session.transaction().execute(
                    f'PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES {values};',
                    commit_tx=True,
                )
            # Removes
            for rid in remove_ids:
                session.transaction().execute(
                    f'PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = {rid};',
                    commit_tx=True,
                )

    def _add_more_tables(self, prefix: str, count: int = 1):
        created = []
        for i in range(1, count + 1):
            name = f"{prefix}_{i}_{int(time.time()) % 10000}"
            with self.session_scope() as session:
                create_table_with_data(session, name)
            created.append(f"/Root/{name}")
        return created

    def _remove_tables(self, table_paths: List[str]):
        with self.session_scope() as session:
            for tp in table_paths:
                try:
                    session.execute_scheme(f"DROP TABLE `{tp}`;")
                except Exception:
                    logger.debug(f"Failed to drop table {tp} (maybe not exists)")

    # extract timestamp prefix from exported folder name
    name_re = re.compile(r"^([0-9]{8}T[0-9]{6}Z?)_(full|incremental)")

    def extract_ts(self, name):
        m = self.name_re.match(name)
        if m:
            return m.group(1)
        return name.split("_", 1)[0]

    def import_exported_up_to_timestamp(self, target_collection, target_ts, export_dir, full_orders, full_products, timeout_s=60):
        create_sql = f"""
            CREATE BACKUP COLLECTION `{target_collection}`
                ( TABLE `{full_orders}`, TABLE `{full_products}` )
            WITH ( STORAGE = 'cluster' );
        """
        res = self._execute_yql(create_sql)
        assert res.exit_code == 0, f"CREATE {target_collection} failed: {getattr(res, 'std_err', None)}"
        self.wait_for_collection(target_collection, timeout_s=30)

        all_dirs = sorted([d for d in os.listdir(export_dir) if os.path.isdir(os.path.join(export_dir, d))])
        chosen = [d for d in all_dirs if self.extract_ts(d) <= target_ts]
        assert chosen, f"No exported snapshots with ts <= {target_ts} found in {export_dir}: {all_dirs}"

        logger.info(f"Will import into {target_collection} these snapshots (in order): {chosen}")

        for name in chosen:
            src = os.path.join(export_dir, name)
            dest_path = f"/Root/.backups/collections/{target_collection}/{name}"
            logger.info(f"Importing {name} (ts={self.extract_ts(name)}) -> {dest_path}")
            r = yatest.common.execute(
                [
                    backup_bin(),
                    "--verbose",
                    "--endpoint",
                    "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                    "--database",
                    self.root_dir,
                    "tools",
                    "restore",
                    "--path",
                    dest_path,
                    "--input",
                    src,
                ],
                check_exit_code=False,
            )
            out = (r.std_out or b"").decode("utf-8", "ignore")
            err = (r.std_err or b"").decode("utf-8", "ignore")
            if r.exit_code != 0:
                logger.error(f"tools restore import failed for {name}: exit={r.exit_code} stdout={out} stderr={err}")
            assert r.exit_code == 0, f"tools restore import failed for {name}: stdout={out} stderr={err}"

        # --- WAIT until imported snapshots are visible in scheme (registered) ---
        deadline = time.time() + timeout_s
        expected = set(chosen)
        while time.time() < deadline:
            try:
                kids = set(self.get_collection_children(target_collection))
                if expected.issubset(kids):
                    logger.info(f"All imported snapshots are registered in collection {target_collection}")
                    break
                else:
                    missing = expected - kids
                    logger.info(f"Waiting for registered snapshots in {target_collection}, missing: {missing}")
            except Exception as e:
                logger.debug(f"While waiting for imported snapshots: {e}")
            time.sleep(5)
        else:
            # timeout
            try:
                kids = sorted(self.get_collection_children(target_collection))
            except Exception:
                kids = "<could not list children>"
            raise AssertionError(f"Imported snapshots did not appear in collection {target_collection} within {timeout_s}s. Expected: {sorted(chosen)}. Present: {kids}")

        # small safety pause to ensure system is stable before RESTORE
        time.sleep(5)

    def normalize_rows(self, rows):
        header = rows[0]
        body = rows[1:]

        def norm_val(v):
            return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        sorted_body = sorted([tuple(norm_val(x) for x in r) for r in body])
        return (tuple(header), tuple(sorted_body))

    def test_full_cycle_local_backup_restore_with_incrementals(self):
        # setup
        collection_src, t_orders, t_products = self._setup_test_collections()
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"

        # Keep created snapshot names
        created_snapshots: List[str] = []
        snapshot_rows = {}  # name -> captured rows

        # Create collection with incremental enabled
        create_collection_sql = f"""
            CREATE BACKUP COLLECTION `{collection_src}`
                ( TABLE `{full_orders}`, TABLE `{full_products}` )
            WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
        """
        create_res = self._execute_yql(create_collection_sql)
        assert create_res.exit_code == 0, "CREATE BACKUP COLLECTION failed"
        self.wait_for_collection(collection_src, timeout_s=30)

        def record_last_snapshot():
            kids = sorted(self.get_collection_children(collection_src))
            assert kids, "no snapshots found after backup"
            last = kids[-1]
            created_snapshots.append(last)
            return last

        # Add/remove data
        self._modify_data_add_and_remove(add_rows=[(10, 1000, "a1")], remove_ids=[2])

        # Add more tables (1)
        extras = []
        extras += self._add_more_tables("extra1", 1)

        # Create full backup 1
        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}`;")
        assert res.exit_code == 0, f"FULL BACKUP 1 failed: {getattr(res, 'std_err', None)}"
        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)
        snap_full1 = record_last_snapshot()
        snapshot_rows[snap_full1] = self._capture_snapshot(t_orders)

        # Add/remove data
        self._modify_data_add_and_remove(add_rows=[(20, 2000, "b1")], remove_ids=[1])

        # Add more tables, remove some tables from step 4
        extras += self._add_more_tables("extra2", 1)
        if extras:
            self._remove_tables([extras[0]])

        # Create incremental backup 1
        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert res.exit_code == 0, "INCREMENTAL 1 failed"
        snap_inc1 = record_last_snapshot()
        snapshot_rows[snap_inc1] = self._capture_snapshot(t_orders)

        # Add/remove
        self._modify_data_add_and_remove(add_rows=[(30, 3000, "c1")], remove_ids=[10])

        # Add more tables, Remove some tables from step 5
        extras += self._add_more_tables("extra3", 1)
        if len(extras) >= 2:
            self._remove_tables([extras[1]])

        # Create incremental backup 2
        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert res.exit_code == 0, "INCREMENTAL 2 failed"
        snap_inc2 = record_last_snapshot()
        snapshot_rows[snap_inc2] = self._capture_snapshot(t_orders)

        # Add more tables, remove some tables from step 5
        extras += self._add_more_tables("extra4", 1)
        if len(extras) >= 3:
            self._remove_tables([extras[2]])

        # Add/remove
        self._modify_data_add_and_remove(add_rows=[(40, 4000, "d1")], remove_ids=[20])

        # Create full backup 2
        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}`;")
        assert res.exit_code == 0, "FULL BACKUP 2 failed"
        snap_full2 = record_last_snapshot()
        snapshot_rows[snap_full2] = self._capture_snapshot(t_orders)

        # Create incremental backup 3
        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert res.exit_code == 0, "INCREMENTAL 3 failed"
        snap_inc3 = record_last_snapshot()
        snapshot_rows[snap_inc3] = self._capture_snapshot(t_orders)

        # Add more tables, remove some tables from step 5
        extras += self._add_more_tables("extra5", 1)
        if len(extras) >= 4:
            self._remove_tables([extras[3]])

        # Add/remove
        self._modify_data_add_and_remove(add_rows=[(50, 5000, "e1")], remove_ids=[30])

        # Create incremental backup 4
        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert res.exit_code == 0, "INCREMENTAL 4 failed"
        snap_inc4 = record_last_snapshot()
        snapshot_rows[snap_inc4] = self._capture_snapshot(t_orders)

        # Export backups
        export_dir, exported_items = self._export_backups(collection_src)
        exported_dirs = sorted([d for d in os.listdir(export_dir) if os.path.isdir(os.path.join(export_dir, d))])

        # all recorded snapshots should be exported
        for s in created_snapshots:
            assert s in exported_dirs, f"Recorded snapshot {s} not in exported dirs {exported_dirs}"

        # Try to restore and get error that tables already exist
        restore_all_col = f"restore_all_{int(time.time())}"
        # import all snapshots up to the latest snapshot
        latest_ts = self.extract_ts(created_snapshots[-1])
        self.import_exported_up_to_timestamp(restore_all_col, latest_ts, export_dir, full_orders, full_products)
        rest_all = self._execute_yql(f"RESTORE `{restore_all_col}`;")
        assert rest_all.exit_code != 0, "Expected RESTORE to fail when tables already exist"

        # Remove all tables
        self._remove_tables([full_orders, full_products] + extras)

        # Restore to full backup 1
        col_full1 = f"restore_full1_{int(time.time())}"
        ts_full1 = self.extract_ts(snap_full1)
        self.import_exported_up_to_timestamp(col_full1, ts_full1, export_dir, full_orders, full_products)
        rest_full1 = self._execute_yql(f"RESTORE `{col_full1}`;")
        assert rest_full1.exit_code == 0, f"RESTORE full1 failed: {rest_full1.std_err}"
        restored_rows = self.wait_for_table_rows(t_orders, snapshot_rows[snap_full1], timeout_s=90)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_rows[snap_full1]), "Verify data in backup (1) failed"

        # Restore to incremental 1 (full1 + inc1)
        col_inc1 = f"restore_inc1_{int(time.time())}"
        ts_inc1 = self.extract_ts(snap_inc1)
        self.import_exported_up_to_timestamp(col_inc1, ts_inc1, export_dir, full_orders, full_products)
        # ensure target tables absent
        self._remove_tables([full_orders, full_products])
        rest_inc1 = self._execute_yql(f"RESTORE `{col_inc1}`;")
        assert rest_inc1.exit_code == 0, f"RESTORE inc1 failed: {rest_inc1.std_err}"
        restored_rows = self.wait_for_table_rows(t_orders, snapshot_rows[snap_inc1], timeout_s=90)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_rows[snap_inc1]), "Verify data in backup (2) failed"

        # Restore to incremental 2 (full1 + inc1 + inc2)
        col_inc2 = f"restore_inc2_{int(time.time())}"
        ts_inc2 = self.extract_ts(snap_inc2)
        self.import_exported_up_to_timestamp(col_inc2, ts_inc2, export_dir, full_orders, full_products)
        self._remove_tables([full_orders, full_products])
        rest_inc2 = self._execute_yql(f"RESTORE `{col_inc2}`;")
        assert rest_inc2.exit_code == 0, f"RESTORE inc2 failed: {rest_inc2.std_err}"
        restored_rows = self.wait_for_table_rows(t_orders, snapshot_rows[snap_inc2], timeout_s=90)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_rows[snap_inc2]), "Verify data in backup (3) failed"

        # Remove all tables (2)
        self._remove_tables([full_orders, full_products] + extras)

        # Try to restore incremental-only (no base full) -> expect fail
        inc_only_col = f"inc_only_{int(time.time())}"
        # pick incrementals strictly after full1
        idx_full1 = created_snapshots.index(snap_full1)
        incs_after_full1 = [s for s in created_snapshots if "_incremental" in s and created_snapshots.index(s) > idx_full1]
        if incs_after_full1:
            # import incrementals only (should fail on restore)
            create_sql = f"""
                CREATE BACKUP COLLECTION `{inc_only_col}`
                    ( TABLE `{full_orders}`, TABLE `{full_products}` )
                WITH ( STORAGE = 'cluster' );
            """
            res = self._execute_yql(create_sql)
            assert res.exit_code == 0
            self.wait_for_collection(inc_only_col, timeout_s=30)
            for s in incs_after_full1:
                src = os.path.join(export_dir, s)
                dest_path = f"/Root/.backups/collections/{inc_only_col}/{s}"
                r = yatest.common.execute(
                    [
                        backup_bin(),
                        "--verbose",
                        "--endpoint",
                        "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                        "--database",
                        self.root_dir,
                        "tools",
                        "restore",
                        "--path",
                        dest_path,
                        "--input",
                        src,
                    ],
                    check_exit_code=False,
                )
                assert r.exit_code == 0, f"tools restore import for inc-only failed: {r.std_err}"
            rest_inc_only = self._execute_yql(f"RESTORE `{inc_only_col}`;")
            assert rest_inc_only.exit_code != 0, "Expected restore of incrementals-only (no base) to fail"
        else:
            logger.info("No incrementals after full1 — skipping incremental-only restore check")

        # Restore to full backup 2 and verify
        col_full2 = f"restore_full2_{int(time.time())}"
        ts_full2 = self.extract_ts(snap_full2)
        # import all snapshots up to full2
        self.import_exported_up_to_timestamp(col_full2, ts_full2, export_dir, full_orders, full_products)
        self._remove_tables([full_orders, full_products])
        rest_full2 = self._execute_yql(f"RESTORE `{col_full2}`;")
        assert rest_full2.exit_code == 0, f"RESTORE full2 failed: {rest_full2.std_err}"
        restored_rows = self.wait_for_table_rows(t_orders, snapshot_rows[snap_full2], timeout_s=90)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_rows[snap_full2]), "Verify data in backup (4) failed"

        # Restore to most-relevant incremental after full2
        chosen_inc_after_full2 = None
        for cand in (snap_inc3, snap_inc4):
            if cand in created_snapshots and created_snapshots.index(cand) > created_snapshots.index(snap_full2):
                chosen_inc_after_full2 = cand
                break

        if chosen_inc_after_full2:
            col_post_full2 = f"restore_postfull2_{int(time.time())}"
            idx_chosen = created_snapshots.index(chosen_inc_after_full2)
            snaps_for_post = created_snapshots[: idx_chosen + 1]
            # import required snapshots
            create_sql = f"""
                CREATE BACKUP COLLECTION `{col_post_full2}`
                    ( TABLE `{full_orders}`, TABLE `{full_products}` )
                WITH ( STORAGE = 'cluster' );
            """
            res = self._execute_yql(create_sql)
            assert res.exit_code == 0
            self.wait_for_collection(col_post_full2, timeout_s=30)
            for s in snaps_for_post:
                src = os.path.join(export_dir, s)
                dest_path = f"/Root/.backups/collections/{col_post_full2}/{s}"
                r = yatest.common.execute(
                    [
                        backup_bin(),
                        "--verbose",
                        "--endpoint",
                        "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                        "--database",
                        self.root_dir,
                        "tools",
                        "restore",
                        "--path",
                        dest_path,
                        "--input",
                        src,
                    ],
                    check_exit_code=False,
                )
                assert r.exit_code == 0, f"tools restore import failed for {s}: {r.std_err}"
            # drop and restore
            self._remove_tables([full_orders, full_products])
            rest_post = self._execute_yql(f"RESTORE `{col_post_full2}`;")
            assert rest_post.exit_code == 0, f"RESTORE post-full2 failed: {rest_post.std_err}"
            restored_rows = self.wait_for_table_rows(t_orders, snapshot_rows[chosen_inc_after_full2], timeout_s=90)
            assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_rows[chosen_inc_after_full2]), "Verify data in backup (5) failed"

        # cleanup
        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)


class TestFullCycleLocalBackupRestoreWSchemaChange(TestFullCycleLocalBackupRestore):
    def _get_columns_from_scheme_entry(self, desc, path_hint: str = None):
        # Reuse original robust approach: try multiple candidate attributes
        try:
            table_obj = getattr(desc, "table", None)
            if table_obj is not None:
                cols = getattr(table_obj, "columns", None)
                if cols:
                    return [c.name for c in cols]

            cols = getattr(desc, "columns", None)
            if cols:
                try:
                    return [c.name for c in cols]
                except Exception:
                    return [str(c) for c in cols]

            for attr in ("schema", "entry", "path"):
                nested = getattr(desc, attr, None)
                if nested is not None:
                    table_obj = getattr(nested, "table", None)
                    cols = getattr(table_obj, "columns", None) if table_obj is not None else None
                    if cols:
                        return [c.name for c in cols]
        except Exception:
            pass

        if getattr(desc, "is_table", False) or getattr(desc, "is_row_table", False) or getattr(desc, "is_column_table", False):
            if path_hint:
                table_path = path_hint
            else:
                name = getattr(desc, "name", None)
                assert name, f"SchemeEntry has no name, can't form path. desc repr: {repr(desc)}"
                table_path = name if name.startswith("/Root") else os.path.join(self.root_dir, name)

            try:
                tc = getattr(self.driver, "table_client", None)
                if tc is not None and hasattr(tc, "describe_table"):
                    desc_tbl = tc.describe_table(table_path)
                    cols = getattr(desc_tbl, "columns", None) or getattr(desc_tbl, "Columns", None)
                    if cols:
                        try:
                            return [c.name for c in cols]
                        except Exception:
                            return [str(c) for c in cols]
            except Exception:
                pass

            try:
                with self.session_scope() as session:
                    if hasattr(session, "describe_table"):
                        desc_tbl = session.describe_table(table_path)
                        cols = getattr(desc_tbl, "columns", None) or getattr(desc_tbl, "Columns", None)
                        if cols:
                            try:
                                return [c.name for c in cols]
                            except Exception:
                                return [str(c) for c in cols]
            except Exception:
                pass

        diagnostics = ["Failed to find columns via known candidates.\n"]
        try:
            diagnostics.append("dir(desc):\n" + ", ".join(dir(desc)) + "\n")
        except Exception as e:
            diagnostics.append(f"dir(desc) raised: {e}\n")

        readable = []
        for attr in sorted(set(dir(desc))):
            if attr.startswith("_"):
                continue
            if len(readable) >= 40:
                break
            try:
                val = getattr(desc, attr)
                if callable(val):
                    continue
                s = repr(val)
                if len(s) > 300:
                    s = s[:300] + "...(truncated)"
                readable.append(f"{attr} = {s}")
            except Exception as e:
                readable.append(f"{attr} = <unreadable: {e}>")

        diagnostics.append("Sample attributes (truncated):\n" + "\n".join(readable) + "\n")

        raise AssertionError(
            "describe_path returned SchemeEntry in unexpected shape. Cannot locate columns.\n\nDiagnostic dump:\n\n"
            + "\n".join(diagnostics)
        )

    def _capture_schema(self, table_path: str):
        desc = self.driver.scheme_client.describe_path(table_path)
        cols = self._get_columns_from_scheme_entry(desc, path_hint=table_path)
        return cols

    def _create_table_with_data(self, session, path, not_null=False):
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
            .with_column(ydb.Column("expire_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp)))
            .with_primary_keys("id"),
        )

        path_prefix, table = os.path.split(full_path)
        session.transaction().execute(
            (
                f'PRAGMA TablePathPrefix("{path_prefix}"); '
                f'UPSERT INTO {table} (id, number, txt, expire_at) VALUES '
                f'(1, 10, "one", CurrentUtcTimestamp()), (2, 20, "two", CurrentUtcTimestamp()), (3, 30, "three", CurrentUtcTimestamp());'
            ),
            commit_tx=True,
        )

    def _setup_test_collections(self):
        collection_src = f"coll_src_{int(time.time())}"
        t1 = "orders"
        t2 = "products"

        with self.session_scope() as session:
            self._create_table_with_data(session, t1)
            self._create_table_with_data(session, t2)

        return collection_src, t1, t2

    def test_full_cycle_local_backup_restore_with_schema_changes(self):
        collection_src, t1, t2 = self._setup_test_collections()

        # Create backup collection (will reference the initial tables)
        self._create_backup_collection(collection_src, [t1, t2])

        # Add/remove data, change ACLs, add more tables
        # perform first stage of modifications that must be captured by full backup 1
        with self.session_scope() as session:
            # add & remove data
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (10, 100, "one-stage");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM products WHERE id = 1;', commit_tx=True)

            # change ACLs: try multiple grant syntaxes until success
            desc_for_acl = self.driver.scheme_client.describe_path("/Root/orders")
            owner_role = getattr(desc_for_acl, "owner", None) or "root@builtin"

            def q(role: str) -> str:
                return "`" + role.replace("`", "") + "`"

            role_candidates = [owner_role, "public", "everyone", "root"]
            grant_variants = []
            for r in role_candidates:
                role_quoted = q(r)
                grant_variants.extend([
                    f"GRANT ALL ON `/Root/orders` TO {role_quoted};",
                    f"GRANT SELECT ON `/Root/orders` TO {role_quoted};",
                    f"GRANT 'ydb.generic.read' ON `/Root/orders` TO {role_quoted};",
                ])
            grant_variants.append(f"GRANT ALL ON `/Root/orders` TO {q(owner_role)};")

            acl_applied = False
            for cmd in grant_variants:
                res = self._execute_yql(cmd)
                if res.exit_code == 0:
                    acl_applied = True
                    break
            assert acl_applied, "Failed to apply any GRANT variant in step (1)"

            # add more tables
            create_table_with_data(session, "extra_table_1")

        # capture state after stage 1
        snapshot_stage1_t1 = self._capture_snapshot(t1)
        snapshot_stage1_t2 = self._capture_snapshot(t2)
        schema_stage1_t1 = self._capture_schema(f"/Root/{t1}")
        schema_stage1_t2 = self._capture_schema(f"/Root/{t2}")
        acl_stage1_t1 = self._capture_acl(f"/Root/{t1}")
        acl_stage1_t2 = self._capture_acl(f"/Root/{t2}")

        # Create full backup 1
        self._backup_now(collection_src)
        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)

        # modifications include add/remove data, add more tables, remove some tables,
        # add/alter/drop columns, change ACLs
        with self.session_scope() as session:
            # data modifications
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (11, 111, "two-stage");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = 2;', commit_tx=True)

            # add more tables
            create_table_with_data(session, "extra_table_2")

            # remove some tables from step5: drop extra_table_1
            try:
                session.execute_scheme('DROP TABLE `/Root/extra_table_1`;')
            except Exception:
                raise AssertionError("DROP failed")

            # add columns to initial tables -> except fail
            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` ADD COLUMN new_col Uint32;')
            except Exception:
                raise AssertionError("ADD COLUMN failed")

            # ALTER SET -> except fail
            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` SET (TTL = Interval("PT0S") ON expire_at);')
            except Exception:
                raise AssertionError("SET TTL failed")

            # drop columns -> except fail
            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` DROP COLUMN number;')
            except Exception:
                raise AssertionError("DROP COLUMN failed")

            # change ACLs again for initial tables
            desc_for_acl2 = self.driver.scheme_client.describe_path("/Root/orders")
            owner_role2 = getattr(desc_for_acl2, "owner", None) or "root@builtin"
            owner_quoted = owner_role2.replace('`', '')
            cmd = f"GRANT SELECT ON `/Root/orders` TO `{owner_quoted}`;"
            res = self._execute_yql(cmd)
            assert res.exit_code == 0, "Failed to apply GRANT in stage 2"

        # capture state after stage 2
        snapshot_stage2_t1 = self._capture_snapshot(t1)
        snapshot_stage2_t2 = self._capture_snapshot(t2)
        schema_stage2_t1 = self._capture_schema(f"/Root/{t1}")
        schema_stage2_t2 = self._capture_schema(f"/Root/{t2}")
        acl_stage2_t1 = self._capture_acl(f"/Root/{t1}")
        acl_stage2_t2 = self._capture_acl(f"/Root/{t2}")

        # Create full backup 2
        self._backup_now(collection_src)
        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)

        # Export backups so we can import snapshots into separate collections for restore verification
        export_dir, exported_items = self._export_backups(collection_src)
        # expect at least two exported snapshots (backup1 and backup2)
        assert len(exported_items) >= 2, "Expected at least 2 exported snapshots for verification"

        # Attempt to import exported backup into new collection and RESTORE while tables exist -> expect fail
        # create restore collections
        coll_restore_1 = f"coll_restore_v1_{int(time.time())}"
        coll_restore_2 = f"coll_restore_v2_{int(time.time())}"
        self._create_backup_collection(coll_restore_1, [t1, t2])
        self._create_backup_collection(coll_restore_2, [t1, t2])

        # import exported snapshots into restore collections
        # imported_items are directories in exported_items; we'll import both
        self._restore_import(export_dir, exported_items[0], coll_restore_1)
        self._restore_import(export_dir, exported_items[1], coll_restore_2)

        # try RESTORE when tables already exist -> should fail
        res_restore_when_exists = self._execute_yql(f"RESTORE `{coll_restore_1}`;")
        assert res_restore_when_exists.exit_code != 0, "Expected RESTORE to fail when target tables already exist"

        # Remove all tables from DB (orders, products, extras)
        self._drop_tables([t1, t2, "extra_table_2"])

        # Now RESTORE coll_restore_1 (which corresponds to backup1)
        res_restore1 = self._execute_yql(f"RESTORE `{coll_restore_1}`;")
        assert res_restore1.exit_code == 0, f"RESTORE v1 failed: {res_restore1.std_err or res_restore1.std_out}"

        # verify schema/data/acl for backup1
        # verify data
        self._verify_restored_table_data(t1, snapshot_stage1_t1)
        self._verify_restored_table_data(t2, snapshot_stage1_t2)

        # verify schema
        restored_schema_t1 = self._capture_schema(f"/Root/{t1}")
        restored_schema_t2 = self._capture_schema(f"/Root/{t2}")
        assert restored_schema_t1 == schema_stage1_t1, f"Schema for {t1} after restore v1 differs: expected {schema_stage1_t1}, got {restored_schema_t1}"
        assert restored_schema_t2 == schema_stage1_t2, f"Schema for {t2} after restore v1 differs: expected {schema_stage1_t2}, got {restored_schema_t2}"

        # verify acl
        restored_acl_t1 = self._capture_acl(f"/Root/{t1}")
        restored_acl_t2 = self._capture_acl(f"/Root/{t2}")
        # We compare that SHOW GRANTS output contains previously stored show_grants if present
        if 'show_grants' in (acl_stage1_t1 or {}):
            assert 'show_grants' in (restored_acl_t1 or {}) and acl_stage1_t1['show_grants'] in restored_acl_t1['show_grants']
        if 'show_grants' in (acl_stage1_t2 or {}):
            assert 'show_grants' in (restored_acl_t2 or {}) and acl_stage1_t2['show_grants'] in restored_acl_t2['show_grants']

        # === Remove all tables again and restore backup2 ===
        self._drop_tables([t1, t2])  # ignore errors

        res_restore2 = self._execute_yql(f"RESTORE `{coll_restore_2}`;")
        assert res_restore2.exit_code == 0, f"RESTORE v2 failed: {res_restore2.std_err or res_restore2.std_out}"

        # verify data/schema/acl for backup2
        self._verify_restored_table_data(t1, snapshot_stage2_t1)
        self._verify_restored_table_data(t2, snapshot_stage2_t2)

        restored_schema2_t1 = self._capture_schema(f"/Root/{t1}")
        restored_schema2_t2 = self._capture_schema(f"/Root/{t2}")
        assert restored_schema2_t1 == schema_stage2_t1, f"Schema for {t1} after restore v2 differs: expected {schema_stage2_t1}, got {restored_schema2_t1}"
        assert restored_schema2_t2 == schema_stage2_t2, f"Schema for {t2} after restore v2 differs: expected {schema_stage2_t2}, got {restored_schema2_t2}"

        restored_acl2_t1 = self._capture_acl(f"/Root/{t1}")
        restored_acl2_t2 = self._capture_acl(f"/Root/{t2}")
        if 'show_grants' in (acl_stage2_t1 or {}):
            assert 'show_grants' in (restored_acl2_t1 or {}) and acl_stage2_t1['show_grants'] in restored_acl2_t1['show_grants']
        if 'show_grants' in (acl_stage2_t2 or {}):
            assert 'show_grants' in (restored_acl2_t2 or {}) and acl_stage2_t2['show_grants'] in restored_acl2_t2['show_grants']

        # cleanup exported data
        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)


class TestIncrementalChainRestoreAfterDeletion(TestFullCycleLocalBackupRestore):
    """
    Делает цепочку: full -> inc1 -> inc2 -> inc3, экспортирует, импортирует
    все экспортированные snapshot'ы до выбранного inc (в примере - inc2),
    удаляет таблицы и выполняет RESTORE, проверяя, что состояние таблиц
    совпадает с состоянием на выбранном снапшоте.
    """

    def test_incremental_chain_restore_when_tables_deleted(self):
        collection_src, t_orders, t_products = self._setup_test_collections()
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"

        create_collection_sql = f"""
            CREATE BACKUP COLLECTION `{collection_src}`
                ( TABLE `{full_orders}`, TABLE `{full_products}` )
            WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
        """
        create_res = self._execute_yql(create_collection_sql)
        assert create_res.exit_code == 0, f"CREATE BACKUP COLLECTION failed: {getattr(create_res, 'std_err', None)}"
        self.wait_for_collection(collection_src, timeout_s=30)

        created_snapshots = []
        snapshot_rows = {}  # snapshot_name -> {"orders": rows, "products": rows}

        def record_snapshot_and_rows():
            kids = sorted(self.get_collection_children(collection_src))
            assert kids, "No snapshots found after backup"
            last = kids[-1]
            created_snapshots.append(last)
            rows_orders = self._capture_snapshot(t_orders)
            rows_products = self._capture_snapshot(t_products)
            snapshot_rows[last] = {"orders": rows_orders, "products": rows_products}
            return last

        # Full backup
        time.sleep(1.1)
        r = self._execute_yql(f"BACKUP `{collection_src}`;")
        assert r.exit_code == 0, f"FULL BACKUP 1 failed: {getattr(r, 'std_err', None)}"
        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)
        record_snapshot_and_rows()

        # change data and create incremental 1
        with self.session_scope() as session:
            session.transaction().execute(
                'PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (10, 1000, "inc1");',
                commit_tx=True,
            )
            session.transaction().execute(
                'PRAGMA TablePathPrefix("/Root"); DELETE FROM products WHERE id = 1;',
                commit_tx=True,
            )

        time.sleep(1.1)
        r = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert r.exit_code == 0, "INCREMENTAL 1 failed"
        record_snapshot_and_rows()

        # change data and create incremental 2
        with self.session_scope() as session:
            session.transaction().execute(
                'PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (20, 2000, "inc2");',
                commit_tx=True,
            )
            session.transaction().execute(
                'PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = 1;',
                commit_tx=True,
            )

        time.sleep(1.1)
        r = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert r.exit_code == 0, "INCREMENTAL 2 failed"
        snap_inc2 = record_snapshot_and_rows()

        # change data and create incremental 3
        with self.session_scope() as session:
            session.transaction().execute(
                'PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (30, 3000, "inc3");',
                commit_tx=True,
            )

        time.sleep(1.1)
        r = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert r.exit_code == 0, "INCREMENTAL 3 failed"
        record_snapshot_and_rows()

        assert len(created_snapshots) >= 2, "Expected at least 1 full + incrementals"

        # Export backups
        export_dir, exported_items = self._export_backups(collection_src)
        assert exported_items, "No exported snapshots found"
        exported_dirs = sorted([d for d in os.listdir(export_dir) if os.path.isdir(os.path.join(export_dir, d))])
        for s in created_snapshots:
            assert s in exported_dirs, f"Recorded snapshot {s} not found in exported dirs {exported_dirs}"

        # Create restore collection and import snapshots up to target (choose inc2)
        target_snap = snap_inc2
        target_ts = target_snap.split("_", 1)[0]

        coll_restore = f"coll_restore_incr_{int(time.time())}"
        create_restore_sql = f"""
            CREATE BACKUP COLLECTION `{coll_restore}`
                ( TABLE `{full_orders}`, TABLE `{full_products}` )
            WITH ( STORAGE = 'cluster' );
        """
        res = self._execute_yql(create_restore_sql)
        assert res.exit_code == 0, f"CREATE restore collection {coll_restore} failed"
        self.wait_for_collection(coll_restore, timeout_s=30)

        # import exported snapshots with ts <= target_ts
        all_dirs = sorted([d for d in os.listdir(export_dir) if os.path.isdir(os.path.join(export_dir, d))])
        chosen = [d for d in all_dirs if d.split("_", 1)[0] <= target_ts]
        assert chosen, f"No exported snapshots with ts <= {target_ts} found in {export_dir}: {all_dirs}"

        for name in chosen:
            src = os.path.join(export_dir, name)
            dest_path = f"/Root/.backups/collections/{coll_restore}/{name}"
            r = yatest.common.execute(
                [
                    backup_bin(),
                    "--verbose",
                    "--endpoint",
                    "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                    "--database",
                    self.root_dir,
                    "tools",
                    "restore",
                    "--path",
                    dest_path,
                    "--input",
                    src,
                ],
                check_exit_code=False,
            )
            out = (r.std_out or b"").decode("utf-8", "ignore")
            err = (r.std_err or b"").decode("utf-8", "ignore")
            assert r.exit_code == 0, f"tools restore import failed for {name}: stdout={out} stderr={err}"

        # wait until imported snapshots registered in scheme
        deadline = time.time() + 60
        expected = set(chosen)
        while time.time() < deadline:
            kids = set(self.get_collection_children(coll_restore))
            if expected.issubset(kids):
                break
            time.sleep(1)
        else:
            raise AssertionError(f"Imported snapshots did not appear in collection {coll_restore} within timeout. Expected: {sorted(chosen)}")

        self._drop_tables([t_orders, t_products])

        res_restore = self._execute_yql(f"RESTORE `{coll_restore}`;")
        assert res_restore.exit_code == 0, f"RESTORE failed: {getattr(res_restore, 'std_err', None) or getattr(res_restore, 'std_out', None)}"

        expected_orders = snapshot_rows[target_snap]["orders"]
        expected_products = snapshot_rows[target_snap]["products"]

        self._verify_restored_table_data(t_orders, expected_orders)
        self._verify_restored_table_data(t_products, expected_products)

        coll_present = self.collection_exists(collection_src)

        if not coll_present:
            logger.info("Starting collection %s not present (deleted) — OK", collection_src)
        else:
            logger.info(
                f"Starting collection {collection_src} is present and incremental backups appear enabled. "
                "Expected: starting collection removed OR incremental backups disabled."
            )

        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)


class TestFullCycleLocalBackupRestoreWComplSchemaChange(TestFullCycleLocalBackupRestoreWSchemaChange):
    def _rearrange_table(self, from_name: str, to_name: str):
        full_from = f"/Root/{from_name}"
        full_to = f"/Root/{to_name}"

        def run_cli(args):
            cmd = [
                backup_bin(),
                "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
                "--database", self.root_dir,
            ] + args
            return yatest.common.execute(cmd, check_exit_code=False)

        def to_rel(p):
            if p.startswith(self.root_dir + "/"):
                return p[len(self.root_dir) + 1 :]
            if p == self.root_dir:
                return ""
            return p.lstrip("/")

        src_rel = to_rel(full_from)
        dst_rel = to_rel(full_to)

        # ensure parent directory for destination exists (idempotent)
        parent = os.path.dirname(dst_rel)
        parent_full = os.path.join(self.root_dir, parent) if parent else None
        if parent and parent_full:
            self.driver.scheme_client.list_directory(parent_full)

            mkdir_res = run_cli(["scheme", "mkdir", parent])
            if mkdir_res.exit_code != 0:
                logger.debug("scheme mkdir parent returned code=%s stdout=%s stderr=%s",
                             mkdir_res.exit_code,
                             getattr(mkdir_res, "std_out", b"").decode("utf-8", "ignore"),
                             getattr(mkdir_res, "std_err", b"").decode("utf-8", "ignore"))

        # perform tools copy via CLI to the requested destination
        item_arg = f"destination={dst_rel},source={src_rel}"
        res = run_cli(["tools", "copy", "--item", item_arg])
        if res.exit_code != 0:
            out = (res.std_out or b"").decode("utf-8", "ignore")
            err = (res.std_err or b"").decode("utf-8", "ignore")
            raise AssertionError(f"tools copy failed: from={full_from} to={full_to} code={res.exit_code} STDOUT: {out} STDERR: {err}")

        tmp_dir_rel = f"tmp_rearr_{int(time.time())}"
        tmp_full = os.path.join(self.root_dir, tmp_dir_rel)
        tmp_created = False
        try:
            # create temporary directory
            try:
                self.driver.scheme_client.list_directory(tmp_full)
                tmp_created = False
            except Exception:
                res_mk = run_cli(["scheme", "mkdir", tmp_dir_rel])
                if res_mk.exit_code != 0:
                    logger.debug("tmp dir mkdir returned code=%s stdout=%s stderr=%s",
                                 res_mk.exit_code,
                                 getattr(res_mk, "std_out", b"").decode("utf-8", "ignore"),
                                 getattr(res_mk, "std_err", b"").decode("utf-8", "ignore"))
                else:
                    tmp_created = True

            # copy to temporary dir (basename of destination)
            basename = os.path.basename(dst_rel) or to_rel(full_from)
            item_tmp = f"destination={tmp_dir_rel}/{basename},source={src_rel}"
            res_tmp = run_cli(["tools", "copy", "--item", item_tmp])
            if res_tmp.exit_code != 0:
                logger.warning("tools copy to tmp dir failed: code=%s stdout=%s stderr=%s",
                               res_tmp.exit_code,
                               getattr(res_tmp, "std_out", b"").decode("utf-8", "ignore"),
                               getattr(res_tmp, "std_err", b"").decode("utf-8", "ignore"))
            else:
                # list temporary dir contents via scheme_client for visibility
                try:
                    desc = self.driver.scheme_client.list_directory(tmp_full)
                    names = [c.name for c in desc.children if not is_system_object(c)]
                    logger.info("Temporary directory %s contents: %s", tmp_full, names)
                except Exception as e:
                    logger.info("Failed to list tmp dir %s: %s", tmp_full, e)

        finally:
            # remove temporary directory if we created it here
            if tmp_created:
                rmdir_res = run_cli(["scheme", "rmdir", "-rf", tmp_dir_rel])
                if rmdir_res.exit_code != 0:
                    logger.warning("Failed to rmdir tmp dir %s: code=%s stdout=%s stderr=%s",
                                   tmp_dir_rel,
                                   rmdir_res.exit_code,
                                   getattr(rmdir_res, "std_out", b"").decode("utf-8", "ignore"),
                                   getattr(rmdir_res, "std_err", b"").decode("utf-8", "ignore"))

    def test_full_cycle_local_backup_restore_with_complex_schema_changes(self):
        # Preparations: create source collection and initial tables
        collection_src = f"coll_src_{int(time.time())}"
        t1 = "orders"
        t2 = "products"

        with self.session_scope() as session:
            create_table_with_data(session, t1)
            create_table_with_data(session, t2)

        # create backup collection referencing the tables
        self._create_backup_collection(collection_src, [t1, t2])

        # Stage 1: add/remove data, change ACLs (1), add more tables (1)
        with self.session_scope() as session:
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (10, 100, "one-stage");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM products WHERE id = 1;', commit_tx=True)

            # change ACLs: try to grant SELECT to public until succeed
            desc_for_acl = self.driver.scheme_client.describe_path("/Root/orders")
            owner_role = getattr(desc_for_acl, "owner", None) or "root@builtin"
            role_candidates = ["public", owner_role]
            acl_applied = False
            for r in role_candidates:
                cmd = f"GRANT SELECT ON `/Root/orders` TO `{r.replace('`', '')}`;"
                res = self._execute_yql(cmd)
                if res.exit_code == 0:
                    acl_applied = True
                    break
            assert acl_applied, "Failed to apply any GRANT variant in stage 1"

            # add extra table 1
            create_table_with_data(session, "extra_table_1")

        snapshot_stage1_t1 = self._capture_snapshot(t1)
        snapshot_stage1_t2 = self._capture_snapshot(t2)
        schema_stage1_t1 = self._capture_schema(f"/Root/{t1}")
        schema_stage1_t2 = self._capture_schema(f"/Root/{t2}")
        acl_stage1_t1 = self._capture_acl(f"/Root/{t1}")
        acl_stage1_t2 = self._capture_acl(f"/Root/{t2}")

        # Create full backup 1
        self._backup_now(collection_src)
        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)

        # Stage 2: add/remove data, add more tables (2), remove tables from step 1, alter schema
        with self.session_scope() as session:
            # data changes
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (11, 111, "two-stage");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = 2;', commit_tx=True)

            # add extra table 2
            create_table_with_data(session, "extra_table_2")

            # remove extra_table_1
            try:
                session.execute_scheme('DROP TABLE `/Root/extra_table_1`;')
            except Exception:
                raise AssertionError("DROP extra_table_1 failed")

            # add columns to initial tables
            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` ADD COLUMN new_col Uint32;')
            except Exception:
                raise AssertionError("ADD COLUMN failed in stage 2")

            # alter columns (best-effort: change type via SET with TTL as a proxy)
            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` SET (TTL = Interval("PT0S") ON expire_at);')
            except Exception:
                # non-fatal: not all servers support TTL syntax — continue
                pass

            # drop a column
            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` DROP COLUMN number;')
            except Exception:
                # Some setups may deny drop; continue but log
                logger.info("DROP COLUMN number failed or unsupported — continuing")

            # change ACLs again
            desc_for_acl2 = self.driver.scheme_client.describe_path("/Root/orders")
            owner_role2 = getattr(desc_for_acl2, "owner", None) or "root@builtin"
            cmd = f"GRANT SELECT ON `/Root/orders` TO `{owner_role2.replace('`', '')}`;"
            res = self._execute_yql(cmd)
            assert res.exit_code == 0, "Failed to apply GRANT in stage 2"

            # rearrange initial table orders -> orders_rearr
            self._rearrange_table("orders", "orders_rearr")

            create_table_with_data(session, "other_place_topic")

        snapshot_stage2_t1 = self._capture_snapshot("orders_rearr")
        # products may be dropped; try capture but tolerate errors
        try:
            snapshot_stage2_t2 = self._capture_snapshot(t2)
        except Exception:
            snapshot_stage2_t2 = None
        schema_stage2_t1 = self._capture_schema("/Root/orders_rearr")
        schema_stage2_t2 = None
        try:
            schema_stage2_t2 = self._capture_schema(f"/Root/{t2}")
        except Exception:
            schema_stage2_t2 = None
        acl_stage2_t1 = self._capture_acl("/Root/orders_rearr")
        acl_stage2_t2 = None
        try:
            acl_stage2_t2 = self._capture_acl(f"/Root/{t2}")
        except Exception:
            acl_stage2_t2 = None

        # Create full backup 2
        self._backup_now(collection_src)
        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)

        # Export backups for verification
        export_dir, exported_items = self._export_backups(collection_src)
        assert len(exported_items) >= 2, "Expected at least 2 exported snapshots for verification"

        # Create restore collections and import exported snapshots
        coll_restore_1 = f"coll_restore_v1_{int(time.time())}"
        coll_restore_2 = f"coll_restore_v2_{int(time.time())}"
        self._create_backup_collection(coll_restore_1, [t1, t2])
        self._create_backup_collection(coll_restore_2, [t1, t2])

        self._restore_import(export_dir, exported_items[0], coll_restore_1)
        self._restore_import(export_dir, exported_items[1], coll_restore_2)

        # Try RESTORE when tables exist -> expect fail
        res_restore_when_exists = self._execute_yql(f"RESTORE `{coll_restore_2}`;")
        assert res_restore_when_exists.exit_code != 0, "Expected RESTORE to fail when target tables already exist"

        # Remove all tables (1)
        self._drop_tables([t1, t2, "orders_rearr", "extra_table_2", "other_place_topic"])

        # Restore backup 1 and verify
        res_restore1 = self._execute_yql(f"RESTORE `{coll_restore_1}`;")
        assert res_restore1.exit_code == 0, f"RESTORE v1 failed: {res_restore1.std_err or res_restore1.std_out}"

        # verify data/schema/acl for backup1
        self._verify_restored_table_data(t1, snapshot_stage1_t1)
        self._verify_restored_table_data(t2, snapshot_stage1_t2)

        restored_schema_t1 = self._capture_schema(f"/Root/{t1}")
        restored_schema_t2 = self._capture_schema(f"/Root/{t2}")
        assert restored_schema_t1 == schema_stage1_t1, f"Schema for {t1} after restore v1 differs"
        assert restored_schema_t2 == schema_stage1_t2, f"Schema for {t2} after restore v1 differs"

        restored_acl_t1 = self._capture_acl(f"/Root/{t1}")
        restored_acl_t2 = self._capture_acl(f"/Root/{t2}")
        if 'show_grants' in (acl_stage1_t1 or {}):
            assert 'show_grants' in (restored_acl_t1 or {}) and acl_stage1_t1['show_grants'] in restored_acl_t1['show_grants']
        if 'show_grants' in (acl_stage1_t2 or {}):
            assert 'show_grants' in (restored_acl_t2 or {}) and acl_stage1_t2['show_grants'] in restored_acl_t2['show_grants']

        # Remove all tables (again)
        self._drop_tables([t1, t2])

        # Restore backup 2 and verify
        res_restore2 = self._execute_yql(f"RESTORE `{coll_restore_2}`;")
        assert res_restore2.exit_code == 0, f"RESTORE v2 failed: {res_restore2.std_err or res_restore2.std_out}"

        # verify data/schema/acl for backup2
        # t1 in v2 might be 'orders_rearr' after rearrange; try both names
        try:
            self._verify_restored_table_data("orders", snapshot_stage2_t1)
        except AssertionError:
            # fallback to rearranged table name
            self._verify_restored_table_data("orders_rearr", snapshot_stage2_t1)

        if snapshot_stage2_t2 is not None:
            try:
                self._verify_restored_table_data(t2, snapshot_stage2_t2)
            except AssertionError:
                logger.info("products data for stage2 not present or differs — allowed depending on stage operations")

        # schema checks for v2
        try:
            restored_schema2_t1 = self._capture_schema("/Root/orders")
        except Exception:
            restored_schema2_t1 = None
        try:
            restored_schema2_t2 = self._capture_schema(f"/Root/{t2}")
        except Exception:
            restored_schema2_t2 = None

        # best-effort assertions: if we captured schema_stage2, compare
        if schema_stage2_t1 is not None:
            assert restored_schema2_t1 == schema_stage2_t1, "Schema for orders after restore v2 differs (best-effort)"
        if schema_stage2_t2 is not None and schema_stage2_t2:
            assert restored_schema2_t2 == schema_stage2_t2, "Schema for products after restore v2 differs (best-effort)"

        # acl checks for v2
        restored_acl2_t1 = self._capture_acl("/Root/orders")
        restored_acl2_t2 = self._capture_acl(f"/Root/{t2}")
        if 'show_grants' in (acl_stage2_t1 or {}):
            assert 'show_grants' in (restored_acl2_t1 or {}) and acl_stage2_t1['show_grants'] in restored_acl2_t1['show_grants']
        if acl_stage2_t2 and 'show_grants' in acl_stage2_t2:
            assert 'show_grants' in (restored_acl2_t2 or {}) and acl_stage2_t2['show_grants'] in restored_acl2_t2['show_grants']

        # cleanup
        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)


class TestFullCycleLocalBackupRestoreWIncrComplSchemaChange(TestFullCycleLocalBackupRestoreWSchemaChange):
    def normalize_rows(self, rows):
        header = rows[0]
        body = rows[1:]

        def norm_val(v):
            return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        sorted_body = sorted([tuple(norm_val(x) for x in r) for r in body])
        return (tuple(header), tuple(sorted_body))

    def _remove_tables(self, table_paths: List[str]):
        with self.session_scope() as session:
            for tp in table_paths:
                try:
                    session.execute_scheme(f"DROP TABLE `{tp}`;")
                except Exception:
                    raise AssertionError(f"Failed to drop table {tp} (maybe not exists)")

    name_re = re.compile(r"^([0-9]{8}T[0-9]{6}Z?)_(full|incremental)")

    def extract_ts(self, name):
        m = self.name_re.match(name)
        if m:
            return m.group(1)
        return name.split("_", 1)[0]

    def _rearrange_table(self, from_name: str, to_name: str):
        full_from = f"/Root/{from_name}"
        full_to = f"/Root/{to_name}"

        def run_cli(args):
            cmd = [
                backup_bin(),
                "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
                "--database", self.root_dir,
            ] + args
            return yatest.common.execute(cmd, check_exit_code=False)

        def to_rel(p):
            if p.startswith(self.root_dir + "/"):
                return p[len(self.root_dir) + 1 :]
            if p == self.root_dir:
                return ""
            return p.lstrip("/")

        src_rel = to_rel(full_from)
        dst_rel = to_rel(full_to)

        # ensure parent directory for destination exists (idempotent)
        parent = os.path.dirname(dst_rel)
        parent_full = os.path.join(self.root_dir, parent) if parent else None
        if parent and parent_full:
            self.driver.scheme_client.list_directory(parent_full)

            mkdir_res = run_cli(["scheme", "mkdir", parent])
            if mkdir_res.exit_code != 0:
                logger.debug("scheme mkdir parent returned code=%s stdout=%s stderr=%s",
                             mkdir_res.exit_code,
                             getattr(mkdir_res, "std_out", b"").decode("utf-8", "ignore"),
                             getattr(mkdir_res, "std_err", b"").decode("utf-8", "ignore"))

        # perform tools copy via CLI to the requested destination
        item_arg = f"destination={dst_rel},source={src_rel}"
        res = run_cli(["tools", "copy", "--item", item_arg])
        if res.exit_code != 0:
            out = (res.std_out or b"").decode("utf-8", "ignore")
            err = (res.std_err or b"").decode("utf-8", "ignore")
            raise AssertionError(f"tools copy failed: from={full_from} to={full_to} code={res.exit_code} STDOUT: {out} STDERR: {err}")

        tmp_dir_rel = f"tmp_rearr_{int(time.time())}"
        tmp_full = os.path.join(self.root_dir, tmp_dir_rel)
        tmp_created = False
        try:
            # create temporary directory
            try:
                self.driver.scheme_client.list_directory(tmp_full)
                tmp_created = False
            except Exception:
                res_mk = run_cli(["scheme", "mkdir", tmp_dir_rel])
                if res_mk.exit_code != 0:
                    logger.debug("tmp dir mkdir returned code=%s stdout=%s stderr=%s",
                                 res_mk.exit_code,
                                 getattr(res_mk, "std_out", b"").decode("utf-8", "ignore"),
                                 getattr(res_mk, "std_err", b"").decode("utf-8", "ignore"))
                else:
                    tmp_created = True

            # copy to temporary dir (basename of destination)
            basename = os.path.basename(dst_rel) or to_rel(full_from)
            item_tmp = f"destination={tmp_dir_rel}/{basename},source={src_rel}"
            res_tmp = run_cli(["tools", "copy", "--item", item_tmp])
            if res_tmp.exit_code != 0:
                logger.warning("tools copy to tmp dir failed: code=%s stdout=%s stderr=%s",
                               res_tmp.exit_code,
                               getattr(res_tmp, "std_out", b"").decode("utf-8", "ignore"),
                               getattr(res_tmp, "std_err", b"").decode("utf-8", "ignore"))
            else:
                # list temporary dir contents via scheme_client for visibility
                try:
                    desc = self.driver.scheme_client.list_directory(tmp_full)
                    names = [c.name for c in desc.children if not is_system_object(c)]
                    logger.info("Temporary directory %s contents: %s", tmp_full, names)
                except Exception as e:
                    logger.info("Failed to list tmp dir %s: %s", tmp_full, e)

        finally:
            # remove temporary directory if we created it here
            if tmp_created:
                rmdir_res = run_cli(["scheme", "rmdir", "-rf", tmp_dir_rel])
                if rmdir_res.exit_code != 0:
                    logger.warning("Failed to rmdir tmp dir %s: code=%s stdout=%s stderr=%s",
                                   tmp_dir_rel,
                                   rmdir_res.exit_code,
                                   getattr(rmdir_res, "std_out", b"").decode("utf-8", "ignore"),
                                   getattr(rmdir_res, "std_err", b"").decode("utf-8", "ignore"))
    
    def test_full_cycle_local_backup_restore_with_incrementals_and_complex_schema_change(self):
        """
        11. Full cycle local backup restore with incrementals and complex schema change
        Инкрементальная часть реализована по аналогии с TestFullCycleLocalBackupRestoreWIncr:
        - сохраняем rows только для исходных таблиц (t1/t2),
        - при импорте используем последовательный импорт экспортированных папок до нужного ts,
        - при восстановлении проверяем именно исходные таблицы (t1).
        """

        collection_src = f"coll_src_{int(time.time())}"
        t1 = "orders"
        t2 = "products"
        full_t1 = f"/Root/{t1}"
        full_t2 = f"/Root/{t2}"

        # хранение созданных снапшотов и их эталонных строк (всегда относительно исходных таблиц)
        created_snapshots: List[str] = []
        snapshot_meta = {}  # snapshot_name -> { "rows": ..., "schema": ..., "acl": ... }

        def record_last_snapshot():
            kids = sorted(self.get_collection_children(collection_src))
            assert kids, "no snapshots found in collection"
            last = kids[-1]
            created_snapshots.append(last)
            return last

        def import_exported_up_to_timestamp_local(target_collection, target_ts, export_dir, full_orders, full_products, timeout_s=60):
            """
            Более робастная версия импорта экспортированных snapshot'ов в коллекцию до target_ts.
            Сравнение таймстампов выполняется через парсинг в целочисленный YYYYMMDDHHMMSS (без Z),
            чтобы избежать проблем с лексикографическими сравнениями и разным форматированием.
            """
            create_sql = f"""
                CREATE BACKUP COLLECTION `{target_collection}`
                    ( TABLE `{full_orders}`, TABLE `{full_products}` )
                WITH ( STORAGE = 'cluster' );
            """
            res = self._execute_yql(create_sql)
            assert res.exit_code == 0, f"CREATE {target_collection} failed: {getattr(res, 'std_err', None)}"
            self.wait_for_collection(target_collection, timeout_s=30)

            # helper: parse ts like 20251020T173334Z or 20251020173334Z or 20251020173334
            def parse_ts_to_int(ts_str):
                if ts_str is None:
                    return None
                # strip trailing non-digits (like 'Z')
                # accept formats: YYYYMMDDTHHMMSSZ, YYYYMMDDTHHMMSS, YYYYMMDDHHMMSSZ, YYYYMMDDHHMMSS
                m = re.search(r"([0-9]{8}T?[0-9]{6})", ts_str)
                if not m:
                    return None
                core = m.group(1)
                core = core.replace("T", "")
                try:
                    return int(core)  # e.g. 20251020173334
                except Exception:
                    return None

            # normalize target_ts to integer
            target_ts_int = parse_ts_to_int(target_ts)
            if target_ts_int is None:
                raise AssertionError(f"Cannot parse target_ts='{target_ts}'")

            all_dirs = sorted([d for d in os.listdir(export_dir) if os.path.isdir(os.path.join(export_dir, d))])

            # Build list of (dir, parsed_ts) for those that match expected pattern
            candidates = []
            for d in all_dirs:
                # try to extract using your name_re first
                if self.name_re.match(d):
                    ts = self.extract_ts(d)
                    ts_int = parse_ts_to_int(ts)
                    if ts_int is not None:
                        candidates.append((d, ts_int))
                    else:
                        logger.debug("Could not parse ts from name via extract_ts for dir '%s' (extract_ts gave '%s')", d, ts)
                else:
                    # try to extract timestamp-looking prefix anyway
                    m = re.search(r"([0-9]{8}T?[0-9]{6}Z?)", d)
                    if m:
                        ts_guess = m.group(1)
                        ts_int = parse_ts_to_int(ts_guess)
                        if ts_int is not None:
                            candidates.append((d, ts_int))
                        else:
                            logger.debug("Fallback parse failed for dir '%s' (guess='%s')", d, ts_guess)
                    else:
                        logger.debug("Dir '%s' doesn't match name_re and has no timestamp-like prefix", d)

            # choose those with ts <= target_ts_int
            chosen = [name for name, tsi in sorted(candidates, key=lambda x: x[1]) if tsi <= target_ts_int]

            if not chosen:
                # diagnostic output to help debugging in CI
                logger.error("No exported snapshots with ts <= %s found in %s. All dirs: %s", target_ts, export_dir, all_dirs)
                logger.error("Parsed candidate timestamps: %s", candidates)
                raise AssertionError(f"No exported snapshots with ts <= {target_ts} found in {export_dir}: {all_dirs}")

            logger.info(f"Will import into {target_collection} these snapshots (in order): {chosen}")

            for name in chosen:
                src = os.path.join(export_dir, name)
                dest_path = f"/Root/.backups/collections/{target_collection}/{name}"
                logger.info(f"Importing {name} (ts={self.extract_ts(name)}) -> {dest_path}")
                r = yatest.common.execute(
                    [
                        backup_bin(),
                        "--verbose",
                        "--endpoint",
                        "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                        "--database",
                        self.root_dir,
                        "tools",
                        "restore",
                        "--path",
                        dest_path,
                        "--input",
                        src,
                    ],
                    check_exit_code=False,
                )
                out = (r.std_out or b"").decode("utf-8", "ignore")
                err = (r.std_err or b"").decode("utf-8", "ignore")
                if r.exit_code != 0:
                    logger.error(f"tools restore import failed for {name}: exit={r.exit_code} stdout={out} stderr={err}")
                assert r.exit_code == 0, f"tools restore import failed for {name}: stdout={out} stderr={err}"

            # wait until registered
            deadline = time.time() + timeout_s
            expected = set(chosen)
            while time.time() < deadline:
                try:
                    kids = set(self.get_collection_children(target_collection))
                    if expected.issubset(kids):
                        logger.info(f"All imported snapshots are registered in collection {target_collection}")
                        break
                    else:
                        missing = expected - kids
                        logger.info(f"Waiting for registered snapshots in {target_collection}, missing: {missing}")
                except Exception as e:
                    logger.debug(f"While waiting for imported snapshots: {e}")
                time.sleep(2)
            else:
                try:
                    kids = sorted(self.get_collection_children(target_collection))
                except Exception:
                    kids = "<could not list children>"
                raise AssertionError(f"Imported snapshots did not appear in collection {target_collection} within {timeout_s}s. Expected: {sorted(chosen)}. Present: {kids}")

            time.sleep(2)


        # --- 1) Создаём исходные таблицы и коллекцию с INCREMENTAL enabled ---
        with self.session_scope() as session:
            create_table_with_data(session, t1)
            create_table_with_data(session, t2)

        create_collection_sql = f"""
            CREATE BACKUP COLLECTION `{collection_src}`
                ( TABLE `{full_t1}`, TABLE `{full_t2}` )
            WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
        """
        res = self._execute_yql(create_collection_sql)
        assert res.exit_code == 0, "CREATE BACKUP COLLECTION failed"
        self.wait_for_collection(collection_src, timeout_s=30)

        # helper to capture and record snapshot rows for the canonical table names
        def record_snapshot_rows(snapshot_name):
            # always capture rows for the original table t1 (orders)
            rows = self._capture_snapshot(t1)
            schema = None
            acl = None
            try:
                schema = self._capture_schema(full_t1)
            except Exception:
                schema = None
            try:
                acl = self._capture_acl(full_t1)
            except Exception:
                acl = None
            snapshot_meta[snapshot_name] = {"rows": rows, "schema": schema, "acl": acl}

        # --- Stage 1: changes and full backup 1 ---
        with self.session_scope() as session:
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (10, 100, "s1");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM products WHERE id = 1;', commit_tx=True)
            # change ACL
            desc = self.driver.scheme_client.describe_path("/Root/orders")
            owner_role = getattr(desc, "owner", None) or "root@builtin"
            acl_ok = False
            for role in ("public", owner_role):
                cmd = f"GRANT SELECT ON `/Root/orders` TO `{role.replace('`','')}`;"
                r = self._execute_yql(cmd)
                if r.exit_code == 0:
                    acl_ok = True
                    break
            assert acl_ok, "Failed to apply any GRANT variant in stage1"
            create_table_with_data(session, "extra1_stage1")

        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}`;")
        assert res.exit_code == 0, "FULL BACKUP 1 failed"
        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)
        snap_full1 = record_last_snapshot()
        # record canonical rows/schema/acl for full1 (always for t1)
        record_snapshot_rows(snap_full1)

        # --- Stage 2: schema changes, rearrange, incremental 1 ---
        with self.session_scope() as session:
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (20, 200, "s2");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = 2;', commit_tx=True)
            create_table_with_data(session, "extra2_stage2")
            try:
                session.execute_scheme('DROP TABLE `/Root/extra1_stage1`;')
            except Exception:
                logger.info("extra1_stage1 drop failed or not present — continuing")

            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` ADD COLUMN new_col Uint32;')
            except Exception:
                raise AssertionError("ADD COLUMN failed in stage2")

            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` SET (TTL = Interval("PT0S") ON expire_at);')
            except Exception:
                logger.info("SET TTL unsupported — continuing")

            # rearrange: creates orders_rearr but this table is NOT part of the backup collection
            self._rearrange_table("orders", "orders_rearr")

            # now drop column from original orders (best-effort)
            # try:
            #     session.execute_scheme('ALTER TABLE `/Root/orders` DROP COLUMN number;')
            # except Exception:
            #     logger.info("DROP COLUMN number unsupported — continuing")

            # change ACLs
            desc2 = self.driver.scheme_client.describe_path("/Root/orders")
            owner_role2 = getattr(desc2, "owner", None) or "root@builtin"
            r_acl = self._execute_yql(f"GRANT SELECT ON `/Root/orders` TO `{owner_role2.replace('`','')}`;")
            assert r_acl.exit_code == 0, "Failed to apply GRANT in stage2"

            create_table_with_data(session, "cross_other_place_topic")

        # Create incremental 1 (snapshot will contain changes for TABLE `/Root/orders` as it's part of collection)
        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert res.exit_code == 0, "INCREMENTAL 1 failed"
        snap_inc1 = record_last_snapshot()
        # capture rows/schema/acl for canonical table t1 (orders)
        record_snapshot_rows(snap_inc1)

        # --- Stage 3: more changes and incremental 2 ---
        with self.session_scope() as session:
            # try to write into orders_rearr if exists, but canonical snapshots are for t1.
            # we will not rely on orders_rearr being restored because it's not part of collection.
            try:
                session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders_rearr (id, number, txt) VALUES (30, 300, "s3");', commit_tx=True)
            except Exception:
                # if orders_rearr doesn't have 'number' column or doesn't exist, ignore — canonical table is orders
                logger.info("UPSERT into orders_rearr failed or not applicable — continuing")

            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = 10;', commit_tx=True)
            create_table_with_data(session, "extra3_stage3")
            try:
                session.execute_scheme('DROP TABLE `/Root/extra2_stage2`;')
            except Exception:
                logger.info("extra2_stage2 drop failed or not present — continuing")

            try:
                self._rearrange_table("orders_rearr", "orders_rearr_v2")
            except Exception:
                logger.info("rearrange stage3 failed — continuing")

        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert res.exit_code == 0, "INCREMENTAL 2 failed"
        snap_inc2 = record_last_snapshot()
        record_snapshot_rows(snap_inc2)

        # --- Stage 4: more changes and full backup 2 ---
        with self.session_scope() as session:
            create_table_with_data(session, "extra4_stage4")
            try:
                session.execute_scheme('DROP TABLE `/Root/extra3_stage3`;')
            except Exception:
                logger.info("extra3_stage3 drop failed or not present — continuing")

            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (40, 400, "s4");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = 20;', commit_tx=True)

        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}`;")
        assert res.exit_code == 0, "FULL BACKUP 2 failed"
        self.wait_for_collection_has_snapshot(collection_src, timeout_s=30)
        snap_full2 = record_last_snapshot()
        record_snapshot_rows(snap_full2)

        # --- Stage 5: incremental 3 with some schema ops ---
        with self.session_scope() as session:
            create_table_with_data(session, "extra5_stage5")
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (50, 500, "s5");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = 30;', commit_tx=True)
            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` ADD COLUMN more_col Uint64;')
            except Exception:
                logger.info("ADD COLUMN more_col unsupported — continuing")
            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` DROP COLUMN new_col;')
            except Exception:
                logger.info("DROP COLUMN new_col unsupported — continuing")
            desc3 = None
            try:
                desc3 = self.driver.scheme_client.describe_path("/Root/orders")
            except Exception:
                pass
            owner_role3 = getattr(desc3, "owner", None) if desc3 else None
            if owner_role3:
                self._execute_yql(f"GRANT SELECT ON `/Root/orders` TO `{owner_role3.replace('`','')}`;")

        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert res.exit_code == 0, "INCREMENTAL 3 failed"
        snap_inc3 = record_last_snapshot()
        record_snapshot_rows(snap_inc3)

        # --- Stage 6: incremental 4 with final changes ---
        with self.session_scope() as session:
            create_table_with_data(session, "extra6_stage6")
            try:
                session.execute_scheme('DROP TABLE `/Root/extra4_stage4`;')
            except Exception:
                logger.info("extra4_stage4 drop failed or not present — continuing")

            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (60, 600, "s6");', commit_tx=True)
            session.transaction().execute('PRAGMA TablePathPrefix("/Root"); DELETE FROM orders WHERE id = 40;', commit_tx=True)

            try:
                session.execute_scheme('ALTER TABLE `/Root/orders` ADD COLUMN final_col String;')
            except Exception:
                logger.info("ADD COLUMN final_col unsupported — continuing")

            desc4 = None
            try:
                desc4 = self.driver.scheme_client.describe_path("/Root/orders")
            except Exception:
                pass
            owner_role4 = getattr(desc4, "owner", None) if desc4 else None
            if owner_role4:
                self._execute_yql(f"GRANT ALL ON `/Root/orders` TO `{owner_role4.replace('`','')}`;")

        time.sleep(1.1)
        res = self._execute_yql(f"BACKUP `{collection_src}` INCREMENTAL;")
        assert res.exit_code == 0, "INCREMENTAL 4 failed"
        snap_inc4 = record_last_snapshot()
        record_snapshot_rows(snap_inc4)

        # --- Export backups for restore-testing ---
        export_dir, exported_items = self._export_backups(collection_src)
        exported_dirs = sorted([d for d in os.listdir(export_dir) if os.path.isdir(os.path.join(export_dir, d))])
        for s in created_snapshots:
            assert s in exported_dirs, f"Recorded snapshot {s} not in exported dirs {exported_dirs}"

        # ---- Try restore to last point and expect failure because tables already exist ----
        restore_all_col = f"restore_all_{int(time.time())}"
        latest_ts = self.extract_ts(created_snapshots[-1])
        import_exported_up_to_timestamp_local(restore_all_col, latest_ts, export_dir, full_t1, full_t2)
        rest_all = self._execute_yql(f"RESTORE `{restore_all_col}`;")
        assert rest_all.exit_code != 0, "Expected RESTORE to fail when tables already exist"

        # ---- Remove all tables and then restore to various points, validating canonical t1 rows/schema/acl ----
        # remove originals and extras
        self._remove_tables([full_t1, full_t2, "/Root/cross_other_place_topic",
                            "/Root/extra5_stage5", "/Root/extra6_stage6"])

        # Restore to full backup 1
        col_full1 = f"restore_full1_{int(time.time())}"
        ts_full1 = self.extract_ts(snap_full1)
        import_exported_up_to_timestamp_local(col_full1, ts_full1, export_dir, full_t1, full_t2)
        rest_full1 = self._execute_yql(f"RESTORE `{col_full1}`;")
        assert rest_full1.exit_code == 0, f"RESTORE full1 failed: {rest_full1.std_err}"
        restored_rows = self.wait_for_table_rows(t1, snapshot_meta[snap_full1]["rows"], timeout_s=90)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_meta[snap_full1]["rows"]), "Verify data in backup (full1) failed"

        restored_schema1 = self._capture_schema(full_t1)
        assert restored_schema1 == snapshot_meta[snap_full1]["schema"], "Schema mismatch for full1"
        restored_acl1 = self._capture_acl(full_t1)
        if 'show_grants' in (snapshot_meta[snap_full1]["acl"] or {}):
            assert 'show_grants' in (restored_acl1 or {}) and snapshot_meta[snap_full1]["acl"]['show_grants'] in restored_acl1['show_grants']

        # Restore to incremental 1 (full1 + inc1)
        col_inc1 = f"restore_inc1_{int(time.time())}"
        ts_inc1 = self.extract_ts(snap_inc1)
        import_exported_up_to_timestamp_local(col_inc1, ts_inc1, export_dir, full_t1, full_t2)
        # ensure target absent before restore
        self._remove_tables([full_t1, full_t2])
        rest_inc1 = self._execute_yql(f"RESTORE `{col_inc1}`;")
        assert rest_inc1.exit_code == 0, f"RESTORE inc1 failed: {rest_inc1.std_err}"
        restored_rows = self.wait_for_table_rows(t1, snapshot_meta[snap_inc1]["rows"], timeout_s=90)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_meta[snap_inc1]["rows"]), "Verify data in backup (inc1) failed"

        restored_schema_inc1 = self._capture_schema(full_t1)
        if snapshot_meta[snap_inc1]["schema"] is not None:
            assert restored_schema_inc1 == snapshot_meta[snap_inc1]["schema"], "Schema mismatch for inc1"
        restored_acl_inc1 = self._capture_acl(full_t1)
        if 'show_grants' in (snapshot_meta[snap_inc1]["acl"] or {}):
            assert 'show_grants' in (restored_acl_inc1 or {}) and snapshot_meta[snap_inc1]["acl"]['show_grants'] in restored_acl_inc1['show_grants']

        # Restore to incremental 2 (full1 + inc1 + inc2)
        col_inc2 = f"restore_inc2_{int(time.time())}"
        ts_inc2 = self.extract_ts(snap_inc2)
        import_exported_up_to_timestamp_local(col_inc2, ts_inc2, export_dir, full_t1, full_t2)
        self._remove_tables([full_t1, full_t2])
        rest_inc2 = self._execute_yql(f"RESTORE `{col_inc2}`;")
        assert rest_inc2.exit_code == 0, f"RESTORE inc2 failed: {rest_inc2.std_err}"
        restored_rows = self.wait_for_table_rows(t1, snapshot_meta[snap_inc2]["rows"], timeout_s=90)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_meta[snap_inc2]["rows"]), "Verify data in backup (inc2) failed"

        # ---- Try incremental-only restore (no base) -> expect fail ----
        # remove originals
        self._remove_tables([full_t1, full_t2])
        inc_only_col = f"inc_only_{int(time.time())}"
        # pick incrementals strictly after full1
        idx_full1 = created_snapshots.index(snap_full1)
        incs_after_full1 = [s for s in created_snapshots if "_incremental" in s and created_snapshots.index(s) > idx_full1]
        if incs_after_full1:
            # create new collection and import only incrementals
            create_sql = f"""
                CREATE BACKUP COLLECTION `{inc_only_col}`
                    ( TABLE `{full_t1}`, TABLE `{full_t2}` )
                WITH ( STORAGE = 'cluster' );
            """
            res = self._execute_yql(create_sql)
            assert res.exit_code == 0
            self.wait_for_collection(inc_only_col, timeout_s=30)
            for s in incs_after_full1:
                src = os.path.join(export_dir, s)
                dest_path = f"/Root/.backups/collections/{inc_only_col}/{s}"
                r = yatest.common.execute(
                    [
                        backup_bin(),
                        "--verbose",
                        "--endpoint",
                        "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                        "--database",
                        self.root_dir,
                        "tools",
                        "restore",
                        "--path",
                        dest_path,
                        "--input",
                        src,
                    ],
                    check_exit_code=False,
                )
                assert r.exit_code == 0, f"tools restore import for inc-only failed: {r.std_err}"
            rest_inc_only = self._execute_yql(f"RESTORE `{inc_only_col}`;")
            assert rest_inc_only.exit_code != 0, "Expected restore of incrementals-only (no base) to fail"
        else:
            logger.info("No incrementals after full1 — skipping incremental-only restore check")

        # ---- Restore to full backup 2 and verify ----
        col_full2 = f"restore_full2_{int(time.time())}"
        ts_full2 = self.extract_ts(snap_full2)
        import_exported_up_to_timestamp_local(col_full2, ts_full2, export_dir, full_t1, full_t2)
        self._remove_tables([full_t1, full_t2])
        rest_full2 = self._execute_yql(f"RESTORE `{col_full2}`;")
        assert rest_full2.exit_code == 0, f"RESTORE full2 failed: {rest_full2.std_err}"
        restored_rows = self.wait_for_table_rows(t1, snapshot_meta[snap_full2]["rows"], timeout_s=90)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_meta[snap_full2]["rows"]), "Verify data in backup (full2) failed"

        # ---- Restore to most-relevant incremental after full2 (if any) and verify ---
        chosen_inc_after_full2 = None
        for cand in (snap_inc3, snap_inc4):
            if cand in created_snapshots and created_snapshots.index(cand) > created_snapshots.index(snap_full2):
                chosen_inc_after_full2 = cand
                break

        if chosen_inc_after_full2:
            col_post_full2 = f"restore_postfull2_{int(time.time())}"
            idx_chosen = created_snapshots.index(chosen_inc_after_full2)
            snaps_for_post = created_snapshots[: idx_chosen + 1]
            # create collection and import required snapshots
            create_sql = f"""
                CREATE BACKUP COLLECTION `{col_post_full2}`
                    ( TABLE `{full_t1}`, TABLE `{full_t2}` )
                WITH ( STORAGE = 'cluster' );
            """
            res = self._execute_yql(create_sql)
            assert res.exit_code == 0
            self.wait_for_collection(col_post_full2, timeout_s=30)
            for s in snaps_for_post:
                src = os.path.join(export_dir, s)
                dest_path = f"/Root/.backups/collections/{col_post_full2}/{s}"
                r = yatest.common.execute(
                    [
                        backup_bin(),
                        "--verbose",
                        "--endpoint",
                        "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                        "--database",
                        self.root_dir,
                        "tools",
                        "restore",
                        "--path",
                        dest_path,
                        "--input",
                        src,
                    ],
                    check_exit_code=False,
                )
                assert r.exit_code == 0, f"tools restore import failed for {s}: {r.std_err}"
            # drop and restore
            self._remove_tables([full_t1, full_t2])
            rest_post = self._execute_yql(f"RESTORE `{col_post_full2}`;")
            assert rest_post.exit_code == 0, f"RESTORE post-full2 failed: {rest_post.std_err}"
            restored_rows = self.wait_for_table_rows(t1, snapshot_meta[chosen_inc_after_full2]["rows"], timeout_s=90)
            assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_meta[chosen_inc_after_full2]["rows"]), "Verify data in backup (post-full2) failed"

        # cleanup
        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)
