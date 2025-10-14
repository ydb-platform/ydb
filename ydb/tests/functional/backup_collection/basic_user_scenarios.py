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
            assert kids, "No snapshots found after backup"
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
        restored_rows = self._capture_snapshot(t_orders)
        time.sleep(1.1)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_rows[snap_full1]), "Verify data in backup (1) failed"

        # Restore to incremental 1 (full1 + inc1)
        col_inc1 = f"restore_inc1_{int(time.time())}"
        time.sleep(1.1)
        ts_inc1 = self.extract_ts(snap_inc1)
        self.import_exported_up_to_timestamp(col_inc1, ts_inc1, export_dir, full_orders, full_products)
        # ensure target tables absent
        self._remove_tables([full_orders, full_products])
        time.sleep(1.1)
        rest_inc1 = self._execute_yql(f"RESTORE `{col_inc1}`;")
        time.sleep(1.1)
        assert rest_inc1.exit_code == 0, f"RESTORE inc1 failed: {rest_inc1.std_err}"
        restored_rows = self._capture_snapshot(t_orders)
        assert self.normalize_rows(restored_rows) == self.normalize_rows(snapshot_rows[snap_inc1]), "Verify data in backup (2) failed"

        time.sleep(1.1)

        # Restore to incremental 2 (full1 + inc1 + inc2)
        col_inc2 = f"restore_inc2_{int(time.time())}"
        ts_inc2 = self.extract_ts(snap_inc2)
        self.import_exported_up_to_timestamp(col_inc2, ts_inc2, export_dir, full_orders, full_products)
        time.sleep(1.1)
        self._remove_tables([full_orders, full_products])
        time.sleep(1.1)
        rest_inc2 = self._execute_yql(f"RESTORE `{col_inc2}`;")
        time.sleep(1.1)
        assert rest_inc2.exit_code == 0, f"RESTORE inc2 failed: {rest_inc2.std_err}"
        restored_rows = self._capture_snapshot(t_orders)
        time.sleep(1.1)
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
        restored_rows = self._capture_snapshot(t_orders)
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
            restored_rows = self._capture_snapshot(t_orders)
            expected_rows = snapshot_rows[chosen_inc_after_full2]
            assert self.normalize_rows(restored_rows) == self.normalize_rows(expected_rows), "Verify data in backup (5) failed"

        # cleanup
        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)
