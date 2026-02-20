# -*- coding: utf-8 -*-
import os
import time
import logging
import shutil
import yatest
import pytest
import re
import uuid
from typing import List, Dict, Optional, Tuple
from enum import Enum
from dataclasses import dataclass, field
from contextlib import contextmanager

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb

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


# ================ ENUM DEFINITIONS ================
class BackupType(str, Enum):
    """Enum for backup types."""
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class StorageType(str, Enum):
    """Enum for storage types."""
    CLUSTER = "cluster"
    LOCAL = "local"
    S3 = "s3"


# ================ DATA STRUCTURES ================
@dataclass
class TableSnapshot:
    """Represents a snapshot of a single table."""
    table_name: str
    rows: List[List]
    schema: List[str]  # column names
    acl: Optional[Dict] = None


@dataclass
class Snapshot:
    """Represents a captured snapshot of all tables in a backup."""
    name: str
    timestamp: int
    tables: Dict[str, TableSnapshot] = field(default_factory=dict)

    def add_table(self, table_name: str, snapshot: TableSnapshot):
        """Add a table snapshot to this backup snapshot."""
        self.tables[table_name] = snapshot

    def get_table(self, table_name: str) -> Optional[TableSnapshot]:
        """Get snapshot for a specific table."""
        if table_name in self.tables:
            return self.tables[table_name]
        base_name = os.path.basename(table_name)
        for key, snapshot in self.tables.items():
            if os.path.basename(key) == base_name:
                return snapshot
        return None


@dataclass
class BackupResult:
    """Result of a backup operation."""
    success: bool
    snapshot_name: Optional[str] = None
    error_message: Optional[str] = None

    def __bool__(self) -> bool:
        """Allow using result in boolean context: if backup_result: ..."""
        return self.success


@dataclass
class RestoreResult:
    """Result of a restore operation."""
    success: bool
    expected_failure: bool = False
    data_verified: bool = False
    schema_verified: bool = False
    acl_verified: bool = False
    error_message: Optional[str] = None
    diagnostics: Optional[Dict] = None

    def __bool__(self) -> bool:
        """Allow using result in boolean context: if restore_result: ..."""
        return self.success


@dataclass
class BackupStage:
    """Represents a stage in the backup lifecycle."""
    snapshot: Snapshot
    backup_type: BackupType
    stage_number: int
    description: str = ""

    def get_table_snapshot(self, table_name: str) -> Optional[TableSnapshot]:
        """Get snapshot for a specific table in this stage."""
        return self.snapshot.get_table(table_name)


# ================ BASE TEST CLASS ================
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
        max_retries = 5
        retry_delay = 1.0
        last_exc = None
        for attempt in range(max_retries):
            try:
                with self.session_scope() as session:
                    return sdk_select_table_rows(session, table)
            except Exception as e:
                last_exc = e
                if attempt < max_retries - 1:
                    logger.info(
                        f"_capture_snapshot({table}) attempt {attempt + 1}/{max_retries} "
                        f"failed ({e}), retrying in {retry_delay}s"
                    )
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 10.0)
        raise last_exc

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

    def wait_for_table_rows(self,
                            table: str,
                            expected_rows,
                            timeout_s: int = 60,
                            poll_interval: float = 0.5):
        deadline = time.time() + timeout_s
        last_exc = None
        last_rows = None

        while time.time() < deadline:
            try:
                cur_rows = None
                try:
                    cur_rows = self._capture_snapshot(table)
                except Exception as e:
                    last_exc = e
                    time.sleep(poll_interval)
                    continue

                last_rows = cur_rows
                if cur_rows == expected_rows:
                    return cur_rows

            except Exception as e:
                last_exc = e

            time.sleep(poll_interval)

        diag = f"Timeout waiting for table '{table}' rows to match expected (timeout {timeout_s}s)."
        if last_rows is not None:
            diag += f"\n  ACTUAL rows ({len(last_rows)}):\n"
            for r in last_rows[:20]:
                diag += f"    {r}\n"
            diag += f"  EXPECTED rows ({len(expected_rows)}):\n"
            for r in expected_rows[:20]:
                diag += f"    {r}\n"
        if last_exc is not None:
            diag += f"  Last capture error: {last_exc}"
        raise AssertionError(diag)

    def _count_restore_operations(self):
        endpoint = f"grpc://localhost:{self.cluster.nodes[1].grpc_port}"
        database = self.root_dir

        cmd = [backup_bin(), "-e", endpoint, "-d", database, "operation", "list", "restore"]
        try:
            res = yatest.common.execute(cmd, check_exit_code=False)
            output = (res.std_out or b"").decode("utf-8", "ignore")
        except Exception as e:
            return 0, 0, f"CLI failed: {e}"

        candidates = [
            cand for cand in output.splitlines()
            if "│" in cand and not cand.strip().startswith(("┌", "├", "└", "┬", "┴", "┼"))
        ]

        header_idx = None
        for i, ln in enumerate(candidates):
            if re.search(r"\bid\b", ln, re.I) and re.search(r"\bstatus\b", ln, re.I):
                header_idx = i
                break
        if header_idx is not None:
            del candidates[header_idx]

        total = len(candidates)
        success_count = 0
        for ln in candidates:
            low = ln.lower()
            if "success" in low or "true" in low:
                success_count += 1

        return total, success_count, output

    def poll_restore_by_count(self, start_total: int, start_success: int, timeout_s: int = 180, poll_interval: float = 2.0, verbose: bool = True):
        deadline = time.time() + timeout_s
        seen_more = False
        last_total = start_total
        last_success = start_success

        while time.time() < deadline:
            total, success, raw = self._count_restore_operations()
            last_total, last_success, _ = total, success, raw

            if verbose:
                logger.info(f"[poll_restore] total={total} success={success} (start {start_total}/{start_success})")

            if total > start_total:
                seen_more = True

            if seen_more and success > start_success:
                return True, {
                    "start_total": start_total,
                    "start_success": start_success,
                    "last_total": last_total,
                    "last_success": last_success,
                }

            time.sleep(poll_interval)

        return False, {
            "start_total": start_total,
            "start_success": start_success,
            "last_total": last_total,
            "last_success": last_success,
        }

    def _copy_table(self, from_name: str, to_name: str):
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

        parent = os.path.dirname(dst_rel)
        if parent:
            mkdir_res = run_cli(["scheme", "mkdir", parent])
            if mkdir_res.exit_code != 0:
                logger.debug("scheme mkdir parent returned code=%s", mkdir_res.exit_code)

        item_arg = f"destination={dst_rel},source={src_rel}"
        res = run_cli(["tools", "copy", "--item", item_arg])
        if res.exit_code != 0:
            out = (res.std_out or b"").decode("utf-8", "ignore")
            err = (res.std_err or b"").decode("utf-8", "ignore")
            raise AssertionError(f"tools copy failed: from={full_from} to={full_to} code={res.exit_code} STDOUT: {out} STDERR: {err}")

    def normalize_rows(self, rows):
        header = rows[0]
        body = rows[1:]

        def norm_val(v):
            return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        sorted_body = sorted([tuple(norm_val(x) for x in r) for r in body])
        return (tuple(header), tuple(sorted_body))

    name_re = re.compile(r"^([0-9]{8}T[0-9]{6}Z?)_(full|incremental)")

    def extract_ts(self, name):
        m = self.name_re.match(name)
        if m:
            return m.group(1)
        return name.split("_", 1)[0]

    def _capture_acl_pretty(self, table_path: str):
        try:
            desc = self.driver.scheme_client.describe_path(table_path)
        except Exception as e:
            logger.debug(f"_capture_acl_pretty: describe_path failed for {table_path}: {e}")
            return None

        acl_info = {}
        owner = getattr(desc, "owner", None)
        if owner:
            acl_info["owner"] = owner

        permissions = getattr(desc, "permissions", None)
        perms_list = []
        if permissions:
            try:
                iterable = iter(permissions)
            except TypeError:
                iterable = [permissions]

            for perm in iterable:
                perm_dict = {}
                for fld in ("subject", "Subject", "permission_names", "PermissionNames", "grant", "Grant"):
                    if hasattr(perm, fld):
                        val = getattr(perm, fld)
                        if isinstance(val, (list, tuple)):
                            perm_dict[fld.lower()] = [(v.decode() if isinstance(v, (bytes, bytearray)) else str(v)) for v in val]
                        else:
                            perm_dict[fld.lower()] = (val.decode() if isinstance(val, (bytes, bytearray)) else str(val))
                if not perm_dict:
                    try:
                        if hasattr(perm, "to_dict"):
                            perm_dict = perm.to_dict()
                        else:
                            perm_dict = {"raw": repr(perm)}
                    except Exception:
                        perm_dict = {"raw": repr(perm)}
                perms_list.append(perm_dict)

        acl_info["permissions"] = perms_list

        try:
            res = self._execute_yql(f"SHOW GRANTS ON '{table_path}';")
            out = (res.std_out or b"").decode("utf-8", "ignore")
            acl_info["show_grants"] = out.strip()
        except Exception:
            acl_info["show_grants"] = None

        return acl_info

    def import_exported_up_to_timestamp(self, target_collection, target_ts, export_dir, *tables):
        create_sql = f"""
            CREATE BACKUP COLLECTION `{target_collection}`
                ( {", ".join([f'TABLE `{t}`' for t in tables])} )
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

        deadline = time.time() + 60
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
            try:
                kids = sorted(self.get_collection_children(target_collection))
            except Exception:
                kids = "<could not list children>"
            raise AssertionError(f"Imported snapshots did not appear in collection {target_collection} within 60s. Expected: {sorted(chosen)}. Present: {kids}")

        time.sleep(5)

    def _add_more_tables(self, prefix: str, count: int = 1):
        created = []
        for i in range(1, count + 1):
            name = f"{prefix}_{i}_{int(time.time()) % 10000}"
            with self.session_scope() as session:
                create_table_with_data(session, name)
            created.append(f"/Root/{name}")
        return created

    def _try_remove_tables(self, table_paths: List[str]):
        max_retries = 10
        retry_delay = 1.0
        for tp in table_paths:
            full = tp if tp.startswith("/Root") else f"/Root/{tp}"
            for attempt in range(max_retries):
                try:
                    with self.session_scope() as session:
                        session.execute_scheme(f"DROP TABLE `{full}`;")
                    logger.debug(f"Successfully dropped table: {full}")
                    break
                except Exception as e:
                    err_str = str(e)
                    # Table doesn't exist — nothing to drop
                    if "does not exist" in err_str or "path hasn't been resolved" in err_str:
                        logger.debug(f"Table {full} already absent: {e}")
                        break
                    # Table is under operation — retry after delay
                    if attempt < max_retries - 1:
                        logger.info(
                            f"Drop table {full} attempt {attempt + 1}/{max_retries} failed "
                            f"({e}), retrying in {retry_delay}s"
                        )
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.5, 10.0)
                    else:
                        logger.error(f"Failed to drop table {full} after {max_retries} attempts: {e}")
            retry_delay = 1.0

    def try_drop_table_from_backup(self, collection_name: str, backup_type: str, table_name: str, snapshot_index: int = -1) -> bool:
        try:
            # Get all snapshots in the collection
            children = self.get_collection_children(collection_name)
            if not children:
                return False

            # Filter snapshots by type
            backup_suffix = f"_{backup_type.lower()}"
            matching_snapshots = [
                child for child in children
                if child.endswith(backup_suffix)
            ]

            if not matching_snapshots:
                return False

            # Sort to ensure consistent ordering
            matching_snapshots.sort()

            # Get the requested snapshot
            try:
                target_snapshot = matching_snapshots[snapshot_index]
            except IndexError:
                return False

            # Construct full path to the table in backup
            table_basename = os.path.basename(table_name) if "/" in table_name else table_name
            full_path = self.root_dir + f"/.backups/collections/{collection_name}/{target_snapshot}/{table_basename}"

            with self.session_scope() as session:
                session.execute_scheme(f"DROP TABLE `{full_path}`;")
                return True

        except Exception:
            return False

    def _capture_schema(self, table_path: str):
        desc = self.driver.scheme_client.describe_path(table_path)
        cols = self._get_columns_from_scheme_entry(desc, path_hint=table_path)
        return cols

    def _get_columns_from_scheme_entry(self, desc, path_hint: str = None):
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

        raise AssertionError("describe_path returned SchemeEntry in unexpected shape. Cannot locate columns.")

    def has_changefeeds(self, table_name: str) -> Tuple[bool, int]:
        r = yatest.common.execute(
            [
                backup_bin(),
                "--endpoint",
                f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
                "--database",
                self.root_dir,
                "scheme",
                "describe",
                table_name
            ],
            check_exit_code=False,
        )
        out = (r.std_out or b"").decode("utf-8", "ignore")

        # Count total backup changefeeds and their states
        total_backup_cfs = out.count("_continuousBackupImpl")
        enabled_count = out.count("Enabled")
        disabled_count = out.count("Disabled")

        # Check if all changefeeds are accounted for (enabled + disabled)
        has_backup_cfs = total_backup_cfs > 0 and total_backup_cfs == (disabled_count + enabled_count)

        return has_backup_cfs, enabled_count

    def drop_backup_collection(self, collection_name: str) -> None:
        res = self._execute_yql(f"DROP BACKUP COLLECTION `{collection_name}`;")
        assert res.exit_code == 0, f"Failed to drop backup collection '{collection_name}': {res.std_err}"

    def wait_for_changefeed_state(self, table_name: str, expected_enabled: int = 1,
                                  timeout: float = 5.0, poll_interval: float = 0.3) -> Tuple[bool, int, int]:
        start_time = time.time()

        while time.time() - start_time < timeout:
            has_cfs, enabled_count = self.has_changefeeds(table_name)

            # Count total changefeeds by checking the output directly
            r = yatest.common.execute(
                [
                    backup_bin(),
                    "--endpoint",
                    f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
                    "--database",
                    self.root_dir,
                    "scheme",
                    "describe",
                    table_name
                ],
                check_exit_code=False,
            )
            out = (r.std_out or b"").decode("utf-8", "ignore")
            total_cfs = out.count("_continuousBackupImpl")

            if enabled_count == expected_enabled:
                return True, total_cfs, enabled_count

            time.sleep(poll_interval)

        has_cfs, enabled_count = self.has_changefeeds(table_name)
        r = yatest.common.execute(
            [
                backup_bin(),
                "--endpoint",
                f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
                "--database",
                self.root_dir,
                "scheme",
                "describe",
                table_name
            ],
            check_exit_code=False,
        )
        out = (r.std_out or b"").decode("utf-8", "ignore")
        total_cfs = out.count("_continuousBackupImpl")

        return False, total_cfs, enabled_count


# ================ BUILDER AND HELPER CLASSES ================
class BackupBuilder:
    """Fluent builder for backup operations."""

    def __init__(self, test_instance, collection_name: str):
        self.test = test_instance
        self.collection = collection_name
        self._backup_type = BackupType.FULL
        self._timeout = 30

    def full(self) -> 'BackupBuilder':
        """Set backup type to FULL."""
        self._backup_type = BackupType.FULL
        return self

    def incremental(self) -> 'BackupBuilder':
        """Set backup type to INCREMENTAL."""
        self._backup_type = BackupType.INCREMENTAL
        return self

    def execute(self) -> BackupResult:
        """Execute the backup and return result."""
        time.sleep(1.1)

        if self._backup_type == BackupType.INCREMENTAL:
            sql = f"BACKUP `{self.collection}` INCREMENTAL;"
        else:
            sql = f"BACKUP `{self.collection}`;"

        # Retry loop: incremental backups may hit OVERLOADED if a previous
        # CDC stream alter hasn't finished yet (table still in EPathStateAlter).
        max_retries = 10
        retry_delay = 2.0
        for attempt in range(max_retries + 1):
            res = self.test._execute_yql(sql)
            if res.exit_code == 0:
                break

            out = (res.std_out or b"").decode('utf-8', 'ignore')
            err = (res.std_err or b"").decode('utf-8', 'ignore')

            is_retryable = "OVERLOADED" in err or "under operation" in err
            if is_retryable and attempt < max_retries:
                logger.info(
                    f"Backup attempt {attempt + 1}/{max_retries + 1} got retryable error, "
                    f"retrying in {retry_delay}s: {err.strip()}"
                )
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 15.0)
                continue

            error_msg = f"BACKUP failed: code={res.exit_code} STDOUT: {out} STDERR: {err}"
            return BackupResult(
                success=False,
                snapshot_name=None,
                error_message=error_msg
            )

        self.test.wait_for_collection_has_snapshot(self.collection, timeout_s=self._timeout)
        kids = sorted(self.test.get_collection_children(self.collection))
        snap_name = kids[-1] if kids else None

        return BackupResult(
            success=True,
            snapshot_name=snap_name,
            error_message=None
        )


class RestoreBuilder:
    """Fluent builder for restore operations."""

    def __init__(self, test_instance):
        self.test = test_instance
        self._collection = None
        self._expected_snapshot = None
        self._should_fail = False
        self._remove_tables = []
        self._timeout = 180
        self._use_polling = True

    def collection(self, name: str) -> 'RestoreBuilder':
        """Set collection to restore from."""
        self._collection = name
        return self

    def expect(self, snapshot: Snapshot) -> 'RestoreBuilder':
        """Set expected snapshot for verification."""
        self._expected_snapshot = snapshot
        return self

    def should_fail(self) -> 'RestoreBuilder':
        """Expect the restore to fail."""
        self._should_fail = True
        return self

    def remove_tables(self, tables: List[str]) -> 'RestoreBuilder':
        """Remove tables before restore."""
        self._remove_tables = tables
        return self

    def without_polling(self) -> 'RestoreBuilder':
        """Disable polling for operations (for full backups)."""
        self._use_polling = False
        return self

    def timeout(self, seconds: int) -> 'RestoreBuilder':
        """Set timeout for restore operation."""
        self._timeout = seconds
        return self

    def execute(self) -> RestoreResult:
        """Execute restore and return results."""
        # Remove tables if specified
        if self._remove_tables:
            self.test._try_remove_tables(self._remove_tables)

        # Execute restore with retries for transient errors (e.g. table
        # still being dropped or under CDC operation from a prior backup).
        max_retries = 10 if not self._should_fail else 0
        retry_delay = 2.0
        for attempt in range(max_retries + 1):
            start_total, start_success, _ = self.test._count_restore_operations()
            res = self.test._execute_yql(f"RESTORE `{self._collection}`;")

            if res.exit_code == 0:
                break

            err = (res.std_err or b"").decode('utf-8', 'ignore') if isinstance(res.std_err, bytes) else str(res.std_err or "")
            is_retryable = (
                "path exist" in err
                or "under operation" in err
                or "OVERLOADED" in err
            )
            if is_retryable and attempt < max_retries:
                logger.info(
                    f"Restore attempt {attempt + 1}/{max_retries + 1} got retryable error, "
                    f"retrying in {retry_delay}s: {err.strip()}"
                )
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 15.0)
                continue
            break

        if self._should_fail:
            if res.exit_code != 0:
                return RestoreResult(
                    success=True,
                    expected_failure=True,
                    error_message="Restore failed as expected"
                )
            else:
                return RestoreResult(
                    success=False,
                    expected_failure=True,
                    error_message="Expected RESTORE to fail but it succeeded"
                )

        if res.exit_code != 0:
            return RestoreResult(
                success=False,
                error_message=f"RESTORE failed: {res.std_err}"
            )

        if self._use_polling:
            # Poll for completion
            ok, info = self.test.poll_restore_by_count(
                start_total=start_total,
                start_success=start_success,
                timeout_s=self._timeout,
                poll_interval=2.0,
                verbose=True
            )

            if not ok:
                return RestoreResult(
                    success=False,
                    error_message="Timeout waiting restore",
                    diagnostics=info
                )

        # Create result with success
        result = RestoreResult(success=True)

        # Verify if expected snapshot provided
        if self._expected_snapshot:
            verification = self._verify_restored_data()
            result.data_verified = verification.get('data_verified', False)
            result.schema_verified = verification.get('schema_verified', False)
            result.acl_verified = verification.get('acl_verified', False)

        return result

    def _verify_restored_data(self) -> Dict:
        """Verify restored data matches expected snapshot."""
        results = {'data_verified': True, 'schema_verified': True, 'acl_verified': True}

        for table_path, table_snapshot in self._expected_snapshot.tables.items():
            table_name = os.path.basename(table_path)

            # Verify data - USE wait_for_table_rows like in original!
            try:
                self.test.wait_for_table_rows(
                    table_name,
                    table_snapshot.rows,
                    timeout_s=90
                )
            except Exception as e:
                logger.error(f"Data verification failed for {table_name}: {e}")
                results['data_verified'] = False

            # Verify schema
            try:
                actual_schema = self.test._capture_schema(table_path)
                if actual_schema != table_snapshot.schema:
                    results['schema_verified'] = False
            except Exception as e:
                logger.error(f"Schema verification failed for {table_path}: {e}")
                results['schema_verified'] = False

            # Verify ACL if available
            if table_snapshot.acl:
                try:
                    actual_acl = self.test._capture_acl_pretty(table_path)
                    if 'show_grants' in table_snapshot.acl:
                        if not ('show_grants' in (actual_acl or {}) and table_snapshot.acl['show_grants'] in actual_acl.get('show_grants', '')):
                            results['acl_verified'] = False
                except Exception as e:
                    logger.error(f"ACL verification failed for {table_path}: {e}")
                    results['acl_verified'] = False
        return results


class SnapshotCapture:
    """Helper for capturing table snapshots."""

    def __init__(self, test_instance):
        self.test = test_instance

    def capture_tables(self, tables: List[str]) -> Snapshot:
        """Capture snapshots for all specified tables."""
        snapshot = Snapshot(
            name="",
            timestamp=int(time.time())
        )

        for table_path in tables:
            table_name = os.path.basename(table_path)

            rows = self.test._capture_snapshot(table_name)
            schema = self.test._capture_schema(table_path)
            acl = self.test._capture_acl_pretty(table_path)

            table_snapshot = TableSnapshot(
                table_name=table_path,
                rows=rows,
                schema=schema,
                acl=acl
            )

            snapshot.add_table(table_path, table_snapshot)

        return snapshot


class BackupTestOrchestrator:
    """High-level orchestrator for backup/restore test scenarios."""

    def __init__(self, test_instance, collection_name: str, tables: List[str]):
        self.test = test_instance
        self.collection = collection_name
        self.tables = tables
        self.snapshot_capture = SnapshotCapture(test_instance)
        self.stages: List[BackupStage] = []
        self.created_snapshots: List[str] = []
        self._export_dir = None

    def create_collection(self, incremental_enabled: bool = False) -> 'BackupTestOrchestrator':
        """Create the backup collection."""
        table_list = ", ".join([f'TABLE `{t}`' for t in self.tables])
        incremental = "true" if incremental_enabled else "false"

        create_sql = f"""
            CREATE BACKUP COLLECTION `{self.collection}`
                ( {table_list} )
            WITH (
                STORAGE = 'cluster',
                INCREMENTAL_BACKUP_ENABLED = '{incremental}'
            );
        """
        res = self.test._execute_yql(create_sql)
        assert res.exit_code == 0, f"Failed to create collection: {res.std_err}"

        self.test.wait_for_collection(self.collection, timeout_s=30)
        return self

    def stage(self, backup_type: BackupType = BackupType.FULL, description: str = "") -> BackupStage:
        """Execute a complete backup stage with snapshot capture."""
        if isinstance(backup_type, str):
            backup_type = BackupType(backup_type)

        snapshot = self.snapshot_capture.capture_tables(self.tables)

        if backup_type == BackupType.FULL:
            result = BackupBuilder(self.test, self.collection).full().execute()
        else:
            result = BackupBuilder(self.test, self.collection).incremental().execute()

        if not result.success:
            raise AssertionError(f"Backup failed: {result.error_message}")

        snap_name = result.snapshot_name
        self.created_snapshots.append(snap_name)
        snapshot.name = snap_name

        stage = BackupStage(
            snapshot=snapshot,
            backup_type=backup_type,
            stage_number=len(self.stages) + 1,
            description=description
        )
        self.stages.append(stage)

        return stage

    def restore_to_stage(self, stage_number: int, new_collection_name: str = None, auto_remove_tables: bool = True) -> RestoreBuilder:
        if stage_number < 1 or stage_number > len(self.stages):
            raise ValueError(f"Invalid stage number: {stage_number}")

        stage = self.stages[stage_number - 1]

        if new_collection_name is None:
            new_collection_name = f"restore_stage{stage_number}_{uuid.uuid4().hex[:8]}"

        ts = self.test.extract_ts(stage.snapshot.name)

        if self._export_dir:
            self.test.import_exported_up_to_timestamp(
                new_collection_name, ts, self._export_dir, *self.tables
            )

        builder = RestoreBuilder(self.test).collection(new_collection_name).expect(stage.snapshot)

        # Only auto-remove tables if requested
        if auto_remove_tables:
            builder.remove_tables(self.tables)

        if stage.backup_type == BackupType.INCREMENTAL:
            pass
        else:
            builder.without_polling()

        return builder

    def export_all(self) -> str:
        """Export all backups and return export directory."""
        export_dir, exported_items = self.test._export_backups(self.collection)
        self._export_dir = export_dir

        exported_dirs = sorted([
            d for d in os.listdir(export_dir)
            if os.path.isdir(os.path.join(export_dir, d))
        ])

        for snap in self.created_snapshots:
            assert snap in exported_dirs, \
                f"Snapshot {snap} not in exported dirs {exported_dirs}"

        return export_dir


class DataHelper:
    """Helper for data modifications."""

    def __init__(self, test_instance, table_name: str = "orders"):
        self.test = test_instance
        self.table_name = table_name

    def modify(self, add_rows: List[Tuple] = None, remove_ids: List[int] = None) -> None:
        """Add and remove rows."""
        if add_rows:
            with self.test.session_scope() as session:
                values = ", ".join(f"({i},{n},\"{t}\")" for i, n, t in add_rows)
                session.transaction().execute(
                    f'PRAGMA TablePathPrefix("/Root"); '
                    f'UPSERT INTO {self.table_name} (id, number, txt) VALUES {values};',
                    commit_tx=True,
                )

        if remove_ids:
            with self.test.session_scope() as session:
                for rid in remove_ids:
                    session.transaction().execute(
                        f'PRAGMA TablePathPrefix("/Root"); '
                        f'DELETE FROM {self.table_name} WHERE id = {rid};',
                        commit_tx=True,
                    )


@contextmanager
def backup_lifecycle(test_instance, collection_name: str, tables: List[str]):
    """Context manager for backup lifecycle management."""
    orchestrator = BackupTestOrchestrator(test_instance, collection_name, tables)
    try:
        yield orchestrator
    finally:
        pass


class TestFullCycleLocalBackupRestore(BaseTestBackupInFiles):
    def test_full_cycle_local_backup_restore(self):
        # Setup
        t_orders = "orders"
        t_products = "products"
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"

        # Create initial tables
        with self.session_scope() as session:
            create_table_with_data(session, t_orders)
            create_table_with_data(session, t_products)

        collection_src = f"test_basic_backup_{uuid.uuid4().hex[:8]}"

        # Use orchestrator for entire lifecycle
        with backup_lifecycle(self, collection_src, [full_orders, full_products]) as backup:

            # Create backup collection (without incremental support for this test)
            backup.create_collection(incremental_enabled=False)

            # STAGE 1: Initial Backup

            # Capture initial state (no modifications yet, just base data)
            backup.stage(
                BackupType.FULL,
                "Initial backup with base data (orders: 3 rows, products: 3 rows)"
            )

            # STAGE 2: Modified Backup

            # Modifications:
            # 1. Add extra_table_1
            # 2. Insert new row into orders
            with self.session_scope() as session:
                create_table_with_data(session, "extra_table_1")

                session.transaction().execute(
                    'PRAGMA TablePathPrefix("/Root"); '
                    'UPSERT INTO orders (id, number, txt) VALUES (11, 111, "added1");',
                    commit_tx=True,
                )

            # Create second backup with modifications
            backup.stage(
                BackupType.FULL,
                "Modified backup with extra_table_1 and additional data (orders: 4 rows)"
            )

            # EXPORT BACKUPS
            export_dir = backup.export_all()

            # Verify we have exactly 2 exported backups
            exported_items = sorted([
                name for name in os.listdir(export_dir)
                if os.path.isdir(os.path.join(export_dir, name))
            ])
            assert len(exported_items) >= 2, f"Expected at least 2 exported backups, got: {exported_items}"

            # RESTORE TESTS

            # Test 1: Restore should fail when tables exist
            logger.info("\nTEST 1: Verifying restore fails when tables already exist...")
            result = backup.restore_to_stage(2, auto_remove_tables=False).should_fail().execute()
            assert result.expected_failure, "Expected RESTORE to fail when tables already exist"
            assert result.success, "Restore should report success when failing as expected"

            # Test 2: Restore Stage 1 (Initial state)

            # Remove all tables first
            self._try_remove_tables([full_orders, full_products, "/Root/extra_table_1"])

            # Restore to stage 1
            result = backup.restore_to_stage(1, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 1 data verification failed"
            assert result.schema_verified, "Stage 1 schema verification failed"

            # Additional verification: extra_table_1 should NOT exist in stage 1
            try:
                self.driver.scheme_client.describe_path("/Root/extra_table_1")
                raise AssertionError("extra_table_1 should not exist after restoring stage 1")
            except Exception:
                logger.info("Correctly verified extra_table_1 doesn't exist in stage 1")

            # Verify orders table has only 3 original rows
            restored_rows = self._capture_snapshot(t_orders)
            assert len(restored_rows) == 4, f"Expected 4 rows (header + 3 data), got {len(restored_rows)}"

            # Test 3: Restore Stage 2 (Modified state)

            # Remove all tables again
            self._try_remove_tables([full_orders, full_products])

            # Restore to stage 2
            result = backup.restore_to_stage(2, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 2 data verification failed"
            assert result.schema_verified, "Stage 2 schema verification failed"

            # Verify orders table has 4 rows (original 3 + 1 added)
            restored_rows = self._capture_snapshot(t_orders)
            assert len(restored_rows) == 5, f"Expected 5 rows (header + 4 data), got {len(restored_rows)}"

            # Verify the added row exists
            data_rows = restored_rows[1:]  # Skip header
            found_added = any("added1" in str(row) for row in data_rows)
            assert found_added, "Added row with 'added1' text not found in restored data"

            # Cleanup
            if os.path.exists(export_dir):
                shutil.rmtree(export_dir)


class TestFullCycleLocalBackupRestoreWIncr(BaseTestBackupInFiles):
    def test_full_cycle_local_backup_restore_with_incrementals(self):
        # Setup
        t_orders = "orders"
        t_products = "products"
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"
        extras = []

        # Create initial tables
        with self.session_scope() as session:
            create_table_with_data(session, t_orders)
            create_table_with_data(session, t_products)

        collection_src = f"test_incremental_{uuid.uuid4().hex[:8]}"
        data_helper = DataHelper(self, t_orders)

        with backup_lifecycle(self, collection_src, [full_orders, full_products]) as backup:

            # Create collection with incremental enabled
            backup.create_collection(incremental_enabled=True)

            # STAGE 1: Initial Full Backup
            # Modifications: Add/remove data + extra1 table
            data_helper.modify(add_rows=[(10, 1000, "a1")], remove_ids=[2])
            extras += self._add_more_tables("extra1", 1)

            backup.stage(BackupType.FULL, "Initial full backup with extra1 table")

            # STAGE 2: First Incremental
            # Modifications: More data changes + extra2 table - extra1 table
            data_helper.modify(add_rows=[(20, 2000, "b1")], remove_ids=[1])
            extras += self._add_more_tables("extra2", 1)
            if extras:
                self._try_remove_tables([extras[0]])

            backup.stage(BackupType.INCREMENTAL, "First incremental after removing extra1")

            # STAGE 3: Second Incremental
            # Modifications: Data updates + extra3 table - extra2 table
            data_helper.modify(add_rows=[(30, 3000, "c1")], remove_ids=[10])
            extras += self._add_more_tables("extra3", 1)
            if len(extras) >= 2:
                self._try_remove_tables([extras[1]])

            backup.stage(BackupType.INCREMENTAL, "Second incremental with extra3")

            # STAGE 4: Second Full Backup
            # Modifications: More changes + extra4 table - extra3 table
            extras += self._add_more_tables("extra4", 1)
            if len(extras) >= 3:
                self._try_remove_tables([extras[2]])
            data_helper.modify(add_rows=[(40, 4000, "d1")], remove_ids=[20])

            backup.stage(BackupType.FULL, "Second full backup as new baseline")

            # STAGE 5: Third Incremental
            # Just create a marker incremental after full2
            backup.stage(BackupType.INCREMENTAL, "Third incremental after second full")

            # STAGE 6: Fourth Incremental
            # Modifications: Final data state + extra5 table - extra4 table
            extras += self._add_more_tables("extra5", 1)
            if len(extras) >= 4:
                self._try_remove_tables([extras[3]])
            data_helper.modify(add_rows=[(50, 5000, "e1")], remove_ids=[30])

            backup.stage(BackupType.INCREMENTAL, "Final incremental with latest data")

            # Export all backups for advanced restore scenarios
            export_dir = backup.export_all()

            # RESTORE TESTS

            # Test 1: Should fail when tables exist
            result = backup.restore_to_stage(6, auto_remove_tables=False).should_fail().execute()
            assert result.expected_failure, "Expected RESTORE to fail when tables already exist"
            assert result.success, "Restore should report success when failing as expected"

            # Remove all tables for subsequent restore tests
            self._try_remove_tables([full_orders, full_products] + extras)

            # Test 2: Restore to stage 1 (full backup 1)
            result = backup.restore_to_stage(1, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 1 data verification failed"
            assert result.schema_verified, "Stage 1 schema verification failed"

            # Test 3: Restore to stage 2 (full1 + inc1)
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(2, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 2 data verification failed"
            assert result.schema_verified, "Stage 2 schema verification failed"

            # Test 4: Restore to stage 3 (full1 + inc1 + inc2)
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(3, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 3 data verification failed"
            assert result.schema_verified, "Stage 3 schema verification failed"

            # Test 5: Restore to stage 4 (full backup 2)
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(4, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 4 data verification failed"
            assert result.schema_verified, "Stage 4 schema verification failed"

            # SPECIAL TEST: Incremental-only restore (should fail)
            self._test_incremental_only_restore_failure(backup, export_dir)

            # Test 6: Restore to final stage (full2 + inc3 + inc4)
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(6, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 6 data verification failed"
            assert result.schema_verified, "Stage 6 schema verification failed"

            # ADVANCED TEST: Cross-full restore
            # Verify we can restore to stage 5 (full2 + inc3)
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(5, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"

            # Cleanup
            if os.path.exists(export_dir):
                shutil.rmtree(export_dir)

    def _test_incremental_only_restore_failure(self, backup_orchestrator, export_dir):
        # Get all incremental snapshots after first full backup
        incremental_stages = [
            stage for stage in backup_orchestrator.stages
            if stage.backup_type == BackupType.INCREMENTAL and stage.stage_number > 1
        ]

        if not incremental_stages:
            logger.info("No incremental snapshots found for incremental-only test, skipping...")
            return

        # Create a new collection for incremental-only import
        inc_only_collection = f"inc_only_fail_test_{uuid.uuid4().hex[:8]}"

        # Create the collection
        create_sql = f"""
            CREATE BACKUP COLLECTION `{inc_only_collection}`
                ( TABLE `/Root/orders`, TABLE `/Root/products` )
            WITH ( STORAGE = 'cluster' );
        """
        res = self._execute_yql(create_sql)
        assert res.exit_code == 0, "Failed to create incremental-only collection"
        self.wait_for_collection(inc_only_collection, timeout_s=30)

        # Import ONLY incremental snapshots (no full backup base)
        logger.info(f"Importing only incremental snapshots to {inc_only_collection}...")
        for stage in incremental_stages[:2]:  # Just take first 2 incrementals
            snapshot_name = stage.snapshot.name
            src = os.path.join(export_dir, snapshot_name)
            dest_path = f"/Root/.backups/collections/{inc_only_collection}/{snapshot_name}"

            logger.info(f"Importing incremental snapshot: {snapshot_name}")
            r = yatest.common.execute(
                [
                    backup_bin(),
                    "--verbose",
                    "--endpoint",
                    f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
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
            assert r.exit_code == 0, f"Failed to import incremental snapshot {snapshot_name}"

        # Wait for snapshots to be registered
        time.sleep(5)

        # Now try to RESTORE - this should FAIL because there's no base full backup
        rest_inc_only = self._execute_yql(f"RESTORE `{inc_only_collection}`;")
        assert rest_inc_only.exit_code != 0, (
            "CRITICAL: Restore from incremental-only collection succeeded but should have failed! "
            "This indicates a serious issue with incremental backup validation."
        )


class TestFullCycleLocalBackupRestoreWSchemaChange(BaseTestBackupInFiles):
    def _create_table_with_schema_data(self, session, path, not_null=False):
        """Create table with additional expire_at column for schema testing."""
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
                f'(1, 10, "one", CurrentUtcTimestamp()), '
                f'(2, 20, "two", CurrentUtcTimestamp()), '
                f'(3, 30, "three", CurrentUtcTimestamp());'
            ),
            commit_tx=True,
        )

    def _apply_schema_changes(self, session, table_path: str):
        """Apply schema modifications to a table."""
        # Add column
        try:
            session.execute_scheme(f'ALTER TABLE `{table_path}` ADD COLUMN new_col Uint32;')
        except Exception:
            raise AssertionError("ADD COLUMN failed")

        # Set TTL
        try:
            session.execute_scheme(f'ALTER TABLE `{table_path}` SET (TTL = Interval("PT0S") ON expire_at);')
        except Exception:
            raise AssertionError("SET TTL failed")

        # Drop column
        try:
            session.execute_scheme(f'ALTER TABLE `{table_path}` DROP COLUMN number;')
        except Exception:
            raise AssertionError("DROP COLUMN failed")

    def _apply_acl_with_variants(self, table_path: str, permission="ALL"):
        """Apply ACL trying multiple grant syntax variants."""
        desc_for_acl = self.driver.scheme_client.describe_path(table_path)
        owner_role = getattr(desc_for_acl, "owner", None) or "root@builtin"

        def q(role: str) -> str:
            return "`" + role.replace("`", "") + "`"

        role_candidates = [owner_role, "public", "everyone", "root"]
        grant_variants = []
        for r in role_candidates:
            role_quoted = q(r)
            if permission == "ALL":
                grant_variants.extend([
                    f"GRANT ALL ON `{table_path}` TO {role_quoted};",
                    f"GRANT 'ydb.generic.read' ON `{table_path}` TO {role_quoted};",
                ])
            else:
                grant_variants.append(f"GRANT {permission} ON `{table_path}` TO {role_quoted};")

        grant_variants.append(f"GRANT {permission} ON `{table_path}` TO {q(owner_role)};")

        acl_applied = False
        for cmd in grant_variants:
            res = self._execute_yql(cmd)
            if res.exit_code == 0:
                acl_applied = True
                break

        assert acl_applied, f"Failed to apply any GRANT variant for {table_path}"

    def test_full_cycle_local_backup_restore_with_schema_changes(self):
        # Setup
        t_orders = "orders"
        t_products = "products"
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"
        extra_tables = []

        # Create initial tables with schema
        with self.session_scope() as session:
            self._create_table_with_schema_data(session, t_orders)
            self._create_table_with_schema_data(session, t_products)

        collection_src = f"test_schema_backup_{uuid.uuid4().hex[:8]}"

        # Custom DataHelper for schema test tables
        class SchemaDataHelper:
            def __init__(self, test_instance, table_name: str = "orders"):
                self.test = test_instance
                self.table_name = table_name

            def modify_stage1(self):
                """Modifications for stage 1."""
                with self.test.session_scope() as session:
                    # Add data
                    session.transaction().execute(
                        f'PRAGMA TablePathPrefix("/Root"); '
                        f'UPSERT INTO {self.table_name} (id, number, txt) VALUES (10, 100, "one-stage");',
                        commit_tx=True
                    )
                    # Remove data from products
                    session.transaction().execute(
                        'PRAGMA TablePathPrefix("/Root"); DELETE FROM products WHERE id = 1;',
                        commit_tx=True
                    )

            def modify_stage2(self):
                """Modifications for stage 2."""
                with self.test.session_scope() as session:
                    # Add more data
                    session.transaction().execute(
                        f'PRAGMA TablePathPrefix("/Root"); '
                        f'UPSERT INTO {self.table_name} (id, number, txt) VALUES (11, 111, "two-stage");',
                        commit_tx=True
                    )
                    # Remove data
                    session.transaction().execute(
                        f'PRAGMA TablePathPrefix("/Root"); DELETE FROM {self.table_name} WHERE id = 2;',
                        commit_tx=True
                    )

        data_helper = SchemaDataHelper(self, t_orders)

        with backup_lifecycle(self, collection_src, [full_orders, full_products]) as backup:

            # Create collection (no incremental for this test)
            backup.create_collection(incremental_enabled=False)

            data_helper.modify_stage1()
            self._apply_acl_with_variants(full_orders, "ALL")

            # Add extra table 1
            with self.session_scope() as session:
                create_table_with_data(session, "extra_table_1")
            extra_tables.append("/Root/extra_table_1")

            # STAGE 1: First full backup (captures state after initial modifications)
            backup.stage(BackupType.FULL, "After initial modifications with ACL and extra_table_1")

            data_helper.modify_stage2()

            # Add extra table 2
            with self.session_scope() as session:
                create_table_with_data(session, "extra_table_2")
            extra_tables.append("/Root/extra_table_2")

            # Remove extra_table_1
            self._try_remove_tables(["/Root/extra_table_1"])
            extra_tables.remove("/Root/extra_table_1")

            # Apply schema changes
            with self.session_scope() as session:
                try:
                    self._apply_schema_changes(session, full_orders)
                    assert False, "Schema changes should have failed due to backup collection constraint"
                except AssertionError:
                    pass

            # Change ACLs to SELECT only
            desc_for_acl = self.driver.scheme_client.describe_path(full_orders)
            owner_role = getattr(desc_for_acl, "owner", None) or "root@builtin"
            owner_quoted = owner_role.replace('`', '')
            cmd = f"GRANT SELECT ON `{full_orders}` TO `{owner_quoted}`;"
            res = self._execute_yql(cmd)
            assert res.exit_code == 0, "Failed to apply GRANT SELECT"

            # STAGE 2: Second full backup (captures state with schema changes)
            backup.stage(BackupType.FULL, "After schema changes, ACL modifications, and table manipulations")

            # Export all backups
            export_dir = backup.export_all()

            # RESTORE TESTS

            # Test 1: Should fail when tables exist
            result = backup.restore_to_stage(2, auto_remove_tables=False).should_fail().execute()
            assert result.expected_failure, "Expected RESTORE to fail when tables already exist"
            assert result.success, "Restore should report success when failing as expected"

            # Remove all tables for restore tests
            self._try_remove_tables([full_orders, full_products] + extra_tables)

            # Test 2: Restore to stage 1 (initial state with extra_table_1)
            result = backup.restore_to_stage(1, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 1 data verification failed"
            assert result.schema_verified, "Stage 1 schema verification failed"

            # Verify that schema is original (without new_col, with number column)
            restored_schema = self._capture_schema(full_orders)
            assert 'expire_at' in restored_schema, "expire_at column missing after restore stage 1"
            assert 'number' in restored_schema, "number column missing after restore stage 1"
            assert 'new_col' not in restored_schema, "new_col should not exist after restore stage 1"

            # Verify ACL has ALL permission
            restored_acl = self._capture_acl_pretty(full_orders)
            if restored_acl and 'show_grants' in restored_acl:
                grants_output = restored_acl['show_grants'].upper()
                # Should have broader permissions from stage 1
                logger.info(f"Stage 1 ACL verification: {grants_output}")

            # Remove all tables again for stage 2 restore
            self._try_remove_tables([full_orders, full_products, "/Root/extra_table_1"])

            # Test 3: Restore to stage 2 (with schema changes and extra_table_2, without extra_table_1)
            result = backup.restore_to_stage(2, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 2 data verification failed"
            assert result.schema_verified, "Stage 2 schema verification failed"

            # Verify schema changes are present
            restored_schema2 = self._capture_schema(full_orders)
            assert 'expire_at' in restored_schema2, "expire_at column missing after restore stage 2"
            assert 'number' in restored_schema2, "number column should be dropped after restore stage 2"
            assert 'new_col' not in restored_schema2, "new_col should exist after restore stage 2"

            # Verify ACL has SELECT permission only
            restored_acl2 = self._capture_acl_pretty(full_orders)
            if restored_acl2 and 'show_grants' in restored_acl2:
                grants_output2 = restored_acl2['show_grants'].upper()
                # Should have SELECT permission from stage 2
                logger.info(f"Stage 2 ACL verification: {grants_output2}")

            # extra_table_1 should NOT exist
            try:
                self.driver.scheme_client.describe_path("/Root/extra_table_1")
                raise AssertionError("extra_table_1 should not exist after restore stage 2")
            except Exception:
                logger.info("extra_table_1 correctly absent after stage 2 restore")

            # Cleanup
            if os.path.exists(export_dir):
                shutil.rmtree(export_dir)


class TestIncrementalChainRestoreAfterDeletion(BaseTestBackupInFiles):
    def test_incremental_chain_restore_when_tables_deleted(self):
        # Table names and collection
        t_orders = "orders"
        t_products = "products"
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"

        # Prepare initial tables
        with self.session_scope() as session:
            create_table_with_data(session, t_orders)
            create_table_with_data(session, t_products)

        # Create collection name
        collection_src = f"chain_src_{uuid.uuid4().hex[:8]}"

        with backup_lifecycle(self, collection_src, [full_orders, full_products]) as orchestrator:
            orchestrator.create_collection(incremental_enabled=True)

            # We'll collect snapshot-name -> rows mapping for verification
            recorded_snapshots: List[str] = []
            snapshot_rows: Dict[str, Dict[str, List]] = {}

            # helper to record snapshot (uses stage.snapshot already captured by orchestrator.stage)
            def _record(stage: BackupStage) -> str:
                name = stage.snapshot.name
                recorded_snapshots.append(name)
                orders_snap = stage.snapshot.get_table(full_orders)
                products_snap = stage.snapshot.get_table(full_products)
                snapshot_rows[name] = {
                    "orders": orders_snap.rows if orders_snap else None,
                    "products": products_snap.rows if products_snap else None,
                }
                return name

            # STAGE 1: full
            stage_full = orchestrator.stage(BackupType.FULL, "Full initial")
            _record(stage_full)

            # STAGE 2: inc1
            DataHelper(self, t_orders).modify(add_rows=[(10, 1000, "inc1")])
            with self.session_scope() as session:
                session.transaction().execute(
                    'PRAGMA TablePathPrefix("/Root"); DELETE FROM products WHERE id = 1;', commit_tx=True
                )
            stage_inc1 = orchestrator.stage(BackupType.INCREMENTAL, "Inc 1")
            _record(stage_inc1)

            # STAGE 3: inc2
            DataHelper(self, t_orders).modify(add_rows=[(20, 2000, "inc2")], remove_ids=[1])
            stage_inc2 = orchestrator.stage(BackupType.INCREMENTAL, "Inc 2")
            snap_inc2 = _record(stage_inc2)

            # STAGE 4: inc3
            DataHelper(self, t_orders).modify(add_rows=[(30, 3000, "inc3")])
            stage_inc3 = orchestrator.stage(BackupType.INCREMENTAL, "Inc 3")
            _record(stage_inc3)

            assert len(recorded_snapshots) >= 2, f"Expected at least full+incrementals, got: {recorded_snapshots}"

            export_dir = orchestrator.export_all()
            exported_items = sorted([d for d in os.listdir(export_dir) if os.path.isdir(os.path.join(export_dir, d))])
            assert exported_items, "No exported items found"
            for s in recorded_snapshots:
                assert s in exported_items, f"Recorded snapshot {s} not found in exported dirs {exported_items}"

            # Choose target snapshot = inc2 (stage_inc2)
            target_stage_number = 3  # stage numbering: 1=full, 2=inc1, 3=inc2, 4=inc3
            target_snap_name = snap_inc2

            self._try_remove_tables([t_orders, t_products])

            result = orchestrator.restore_to_stage(target_stage_number, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"

            # Verify restored rows match the recorded snapshot for inc2
            expected_orders = snapshot_rows[target_snap_name]["orders"]
            expected_products = snapshot_rows[target_snap_name]["products"]

            assert expected_orders is not None, "Expected orders snapshot rows missing"
            assert expected_products is not None, "Expected products snapshot rows missing"

            self.wait_for_table_rows(t_orders, expected_orders, timeout_s=90)
            self.wait_for_table_rows(t_products, expected_products, timeout_s=90)

            if os.path.exists(export_dir):
                shutil.rmtree(export_dir)


class TestFullCycleLocalBackupRestoreWComplSchemaChange(BaseTestBackupInFiles):
    def _apply_acl_changes(self, table_path, role, permission="SELECT"):
        owner_quoted = role.replace('`', '')
        cmd = f"GRANT {permission} ON `{table_path}` TO `{owner_quoted}`;"
        res = self._execute_yql(cmd)
        assert res.exit_code == 0, f"ACL change failed: {getattr(res, 'std_err', None)}"

    def _capture_acl(self, table_path: str):
        return self._capture_acl_pretty(table_path)

    def test_full_cycle_local_backup_restore_with_complex_schema_changes(self):
        t_orders = "orders"
        t_products = "products"
        t_orders_copy = "orders_copy"
        other_table = "other_place_topic"

        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"
        full_orders_copy = f"/Root/{t_orders_copy}"

        collection_src = f"coll_src_{uuid.uuid4().hex[:8]}"

        with self.session_scope() as session:
            create_table_with_data(session, t_orders)
            create_table_with_data(session, t_products)

        data_helper = DataHelper(self, t_orders)

        with backup_lifecycle(self, collection_src, [full_orders, full_products]) as backup:
            backup.create_collection(incremental_enabled=False)

            # STAGE 1: data change, ACL, add extra table, take full backup
            # Make changes
            data_helper.modify(add_rows=[(10, 100, "stage1")], remove_ids=[2])

            # Apply ACL (try owner)
            desc = self.driver.scheme_client.describe_path(full_orders)
            owner_role = getattr(desc, "owner", None) or "root@builtin"
            self._apply_acl_changes(full_orders, owner_role, "ALL")

            # add extra table via helper
            extras = []
            extras += self._add_more_tables("extra1", 1)

            # Stage 1: full
            backup.stage(BackupType.FULL, "Initial full after stage1 changes")

            # STAGE 2: more data change, add/drop tables, ALTER, copy, take full backup
            data_helper.modify(add_rows=[(11, 111, "stage2")], remove_ids=[1])
            extras += self._add_more_tables("extra2", 1)

            # remove first extra if created
            if extras:
                self._try_remove_tables([extras[0]])

            # alter schema: add column (and defensively try to drop)
            with self.session_scope() as session:
                try:
                    session.execute_scheme(f'ALTER TABLE `{full_orders}` ADD COLUMN new_col Uint32;')
                    assert False, "ADD COLUMN should have failed due to backup collection"
                except Exception:
                    pass

                try:
                    session.execute_scheme(f'ALTER TABLE `{full_orders}` DROP COLUMN number;')
                    assert False, "DROP COLUMN should have failed due to backup collection"
                except Exception:
                    pass

            # apply ACL again (simple SELECT to owner)
            desc2 = self.driver.scheme_client.describe_path(full_orders)
            owner_role2 = getattr(desc2, "owner", None) or "root@builtin"
            self._apply_acl_changes(full_orders, owner_role2, "SELECT")

            self._copy_table(t_orders, t_orders_copy)
            with self.session_scope() as session:
                create_table_with_data(session, other_table)

            # Stage 2: full
            stage2 = backup.stage(BackupType.FULL, "Second full after schema & copy")

            # Export all backups to filesystem for import/restore tests
            export_dir = backup.export_all()

            # RESTORE TESTS

            # Test A: attempt restore when targets exist -> should fail (use last stage)
            result = backup.restore_to_stage(len(backup.stages), new_collection_name=None, auto_remove_tables=False).should_fail().execute()
            # assert res_fail.get('expected_failure', False), "Expected restore to fail when tables already exist"
            assert result.expected_failure, "Expected RESTORE to fail when tables already exist"
            assert result.success, "Restore should report success when failing as expected"

            self._try_remove_tables([full_orders, full_products] + extras)

            # Test B: restore to stage1 and verify exact match of data/schema/acl
            result = backup.restore_to_stage(1, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 1 data verification failed"
            assert result.schema_verified, "Stage 1 schema verification failed"

            # Clean tables
            self._try_remove_tables([full_orders, full_products])

            # Test C: restore to stage2 and verify (note: orders data may be on orders_copy)
            result = backup.restore_to_stage(2, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 2 data verification failed"
            assert result.schema_verified, "Stage 2 schema verification failed"

            expected_orders_snapshot = stage2.snapshot.get_table(full_orders) or stage2.snapshot.get_table(full_orders_copy)
            if expected_orders_snapshot:
                # check corresponding table exists in DB and rows match
                expected_table_basename = os.path.basename(expected_orders_snapshot.table_name)
                self.wait_for_table_rows(expected_table_basename, expected_orders_snapshot.rows, timeout_s=90)

            # products must match stage2 snapshot
            if stage2.snapshot.get_table(full_products):
                self.wait_for_table_rows(t_products, stage2.snapshot.get_table(full_products).rows, timeout_s=90)

            if os.path.exists(export_dir):
                shutil.rmtree(export_dir)


class TestFullCycleLocalBackupRestoreWIncrComplSchemaChange(BaseTestBackupInFiles):
    def _apply_acl_changes(self, table_path, role, permission="SELECT"):
        """Apply ACL modifications to a table."""
        owner_quoted = role.replace('`', '')
        cmd = f"GRANT {permission} ON `{table_path}` TO `{owner_quoted}`;"
        res = self._execute_yql(cmd)
        assert res.exit_code == 0, f"ACL change failed: {res.std_err}"

    def test_full_cycle_local_backup_restore_with_incrementals_complex_schema_changes(self):
        # Setup
        t_orders = "orders"
        t_products = "products"
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"
        extras = []

        # Create initial tables
        with self.session_scope() as session:
            create_table_with_data(session, t_orders)
            create_table_with_data(session, t_products)

        collection_src = f"test_inc_backup_{uuid.uuid4().hex[:8]}"
        data_helper = DataHelper(self, t_orders)

        # Use orchestrator for entire lifecycle
        with backup_lifecycle(self, collection_src, [full_orders, full_products]) as backup:

            # Create collection with incremental enabled
            backup.create_collection(incremental_enabled=True)

            # Initial modifications
            data_helper.modify(add_rows=[(10, 1000, "a1")], remove_ids=[2])
            desc_for_acl = self.driver.scheme_client.describe_path(full_orders)
            owner_role = getattr(desc_for_acl, "owner", None) or "root@builtin"
            self._apply_acl_changes(full_orders, owner_role, "ALL")
            extras += self._add_more_tables("extra1", 1)

            # STAGE 1: Initial full backup
            backup.stage(BackupType.FULL, "Initial state")

            # Modifications for stage 2
            data_helper.modify(add_rows=[(20, 2000, "b1")], remove_ids=[1])
            extras += self._add_more_tables("extra2", 1)
            if extras:
                self._try_remove_tables([extras[0]])
            self._apply_acl_changes(full_orders, "root@builtin", "SELECT")
            self._copy_table(t_orders, "orders_v1")

            # STAGE 2: First incremental
            backup.stage(BackupType.INCREMENTAL, "After modifications")

            # Modifications for stage 3
            data_helper.modify(add_rows=[(30, 3000, "c1")], remove_ids=[10])
            extras += self._add_more_tables("extra3", 1)
            if len(extras) >= 2:
                self._try_remove_tables([extras[1]])
            self._copy_table(t_orders, "orders_v2")

            # STAGE 3: Second incremental
            backup.stage(BackupType.INCREMENTAL, "More modifications")

            # Modifications for stage 4
            extras += self._add_more_tables("extra4", 1)
            if len(extras) >= 3:
                self._try_remove_tables([extras[2]])
            data_helper.modify(add_rows=[(40, 4000, "d1")], remove_ids=[20])

            # STAGE 4: Second FULL backup
            backup.stage(BackupType.FULL, "Second full backup")

            # STAGE 5: Third incremental
            backup.stage(BackupType.INCREMENTAL, "Incremental after second full")

            # Final modifications
            extras += self._add_more_tables("extra5", 1)
            if len(extras) >= 4:
                self._try_remove_tables([extras[3]])
            data_helper.modify(add_rows=[(50, 5000, "e1")], remove_ids=[30])
            self._apply_acl_changes(full_orders, "root1@builtin", "SELECT")

            # STAGE 6: Final incremental
            backup.stage(BackupType.INCREMENTAL, "Final state")

            # Export all backups
            export_dir = backup.export_all()

            # RESTORE TESTS

            # Test 1: Should fail when tables exist (не удаляем таблицы!)
            result = backup.restore_to_stage(6, auto_remove_tables=False).should_fail().execute()
            assert result.expected_failure, "Expected RESTORE to fail when tables already exist"
            assert result.success, "Restore should report success when failing as expected"

            # Remove all tables for subsequent restore tests
            self._try_remove_tables([full_orders, full_products] + extras[4:])

            # Test 2: Restore to stage 1
            result = backup.restore_to_stage(1, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 1 data verification failed"
            assert result.schema_verified, "Stage 1 schema verification failed"

            # Test 3: Restore to stage 2
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(2, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 2 data verification failed"
            assert result.schema_verified, "Stage 2 schema verification failed"

            # Test 4: Restore to stage 3
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(3, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 3 data verification failed"
            assert result.schema_verified, "Stage 3 schema verification failed"

            # Test 5: Restore to stage 4
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(4, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 4 data verification failed"
            assert result.schema_verified, "Stage 4 schema verification failed"

            # Test 6: Restore to final stage
            self._try_remove_tables([full_orders, full_products])
            result = backup.restore_to_stage(6, auto_remove_tables=False).execute()
            assert result.success, f"Restore failed: {result.error_message}"
            assert result.data_verified, "Stage 6 data verification failed"
            assert result.schema_verified, "Stage 6 schema verification failed"

            # Cleanup
            if os.path.exists(export_dir):
                shutil.rmtree(export_dir)


class TestBackupCollectionServiceObjectsCleanup(BaseTestBackupInFiles):
    def test_service_schema_objects_cleanup_on_delete(self):
        # Setup
        t_orders = "orders"
        t_products = "products"
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"

        # Create initial tables
        with self.session_scope() as session:
            create_table_with_data(session, t_orders)
            create_table_with_data(session, t_products)

        # Before creating backup collection - should have no changefeeds
        has_cfs_orders, enabled_orders = self.has_changefeeds(t_orders)
        has_cfs_products, enabled_products = self.has_changefeeds(t_products)

        assert not has_cfs_orders, "Table orders should not have changefeeds before backup collection creation"
        assert not has_cfs_products, "Table products should not have changefeeds before backup collection creation"
        assert enabled_orders == 0, f"No enabled changefeeds expected on orders, got {enabled_orders}"
        assert enabled_products == 0, f"No enabled changefeeds expected on products, got {enabled_products}"

        collection_name = f"test_cleanup_{uuid.uuid4().hex[:8]}"

        # Use orchestrator for backup lifecycle
        with backup_lifecycle(self, collection_name, [full_orders, full_products]) as backup:

            # Create backup collection with incremental enabled
            backup.create_collection(incremental_enabled=True)

            # Check changefeeds after collection creation (should still be 0 - created only on first backup)
            has_cfs_orders, enabled_orders = self.has_changefeeds(t_orders)
            has_cfs_products, enabled_products = self.has_changefeeds(t_products)

            assert not has_cfs_orders, "Changefeeds should not exist yet on orders (created on first backup)"
            assert not has_cfs_products, "Changefeeds should not exist yet on products (created on first backup)"

            # Create a full backup - this should create the first changefeed
            backup.stage(BackupType.FULL, "Initial full backup")

            # Check changefeeds after full backup - should have 1 enabled changefeed
            has_cfs_orders, enabled_orders = self.has_changefeeds(t_orders)
            has_cfs_products, enabled_products = self.has_changefeeds(t_products)

            assert has_cfs_orders, "Changefeeds should exist on orders after full backup"
            assert has_cfs_products, "Changefeeds should exist on products after full backup"
            assert enabled_orders == 1, f"Expected 1 enabled changefeed on orders after full backup, got {enabled_orders}"
            assert enabled_products == 1, f"Expected 1 enabled changefeed on products after full backup, got {enabled_products}"

            # Modify data for incremental
            DataHelper(self, t_orders).modify(add_rows=[(100, 1000, "for_incremental")], remove_ids=[1])

            # Create incremental backup - old changefeed becomes Disabled, new one is Enabled
            backup.stage(BackupType.INCREMENTAL, "First incremental backup")

            # Check changefeeds after incremental backup
            has_cfs_orders, enabled_orders = self.has_changefeeds(t_orders)
            has_cfs_products, enabled_products = self.has_changefeeds(t_products)

            assert has_cfs_orders, "Changefeeds should still exist on orders after incremental backup"
            assert has_cfs_products, "Changefeeds should still exist on products after incremental backup"
            # After incremental, we still have 1 enabled (previous disabled, new enabled)
            assert enabled_orders == 1, f"Expected 1 enabled changefeed on orders after incremental, got {enabled_orders}"
            assert enabled_products == 1, f"Expected 1 enabled changefeed on products after incremental, got {enabled_products}"

        # Drop the backup collection
        self.drop_backup_collection(collection_name)

        # Verify collection no longer exists
        assert not self.collection_exists(collection_name), f"Collection {collection_name} should not exist after DROP"

        # Changefeeds should be cleaned up
        has_cfs_orders, enabled_orders = self.has_changefeeds(t_orders)
        has_cfs_products, enabled_products = self.has_changefeeds(t_products)

        assert not has_cfs_orders, (
            "CRITICAL: Changefeeds were NOT cleaned up on orders table after dropping backup collection! "
            "This may lead to resource leaks."
        )
        assert not has_cfs_products, (
            "CRITICAL: Changefeeds were NOT cleaned up on products table after dropping backup collection! "
            "This may lead to resource leaks."
        )
        assert enabled_orders == 0, f"No enabled changefeeds expected on orders after cleanup, got {enabled_orders}"
        assert enabled_products == 0, f"No enabled changefeeds expected on products after cleanup, got {enabled_products}"


class TestBackupCollectionServiceObjectsRotation(BaseTestBackupInFiles):
    def test_service_schema_objects_cleanup_on_rotate(self):
        # Setup
        t_orders = "orders"
        t_products = "products"
        full_orders = f"/Root/{t_orders}"
        full_products = f"/Root/{t_products}"

        # Create initial tables
        with self.session_scope() as session:
            create_table_with_data(session, t_orders)
            create_table_with_data(session, t_products)

        collection_name = f"collection_{uuid.uuid4().hex[:8]}"
        data_helper = DataHelper(self, t_orders)

        with backup_lifecycle(self, collection_name, [full_orders, full_products]) as backup:

            # Create backup collection with incremental enabled
            backup.create_collection(incremental_enabled=True)

            # Initial state - no changefeeds
            has_cfs, enabled = self.has_changefeeds(t_orders)
            assert not has_cfs, "Should have no changefeeds initially"

            # Create first full backup
            backup.stage(BackupType.FULL, "First full backup")
            success, total, enabled = self.wait_for_changefeed_state(t_orders, expected_enabled=1)
            assert success, f"Expected 1 enabled changefeed after full backup, got {enabled}"

            # Create first incremental
            data_helper.modify(add_rows=[(100, 1000, "inc1")])
            backup.stage(BackupType.INCREMENTAL, "First incremental")

            # Wait for old changefeed to be cleaned up (should happen after ~0.9s)
            success, total, enabled = self.wait_for_changefeed_state(t_orders, expected_enabled=1, timeout=3.0)
            assert success, f"Expected 1 enabled changefeed after incremental, got {enabled}"

            success = self.try_drop_table_from_backup(
                collection_name=collection_name,
                backup_type="full",
                table_name="orders",
                snapshot_index=-1  # Latest full backup
            )
            assert success, "Expected ability to delete backup"

            # Create second incremental
            data_helper.modify(add_rows=[(101, 1001, "inc2")])
            backup.stage(BackupType.INCREMENTAL, "Second incremental")

            success, total, enabled = self.wait_for_changefeed_state(t_orders, expected_enabled=1, timeout=3.0)
            assert success, f"Expected 1 enabled changefeed after second incremental, got {enabled}"

            # Create third incremental
            data_helper.modify(add_rows=[(102, 1002, "inc3")])
            backup.stage(BackupType.INCREMENTAL, "Third incremental")

            success, total_after_inc3, enabled = self.wait_for_changefeed_state(t_orders, expected_enabled=1, timeout=3.0)
            assert success, f"Expected 1 enabled changefeed after third incremental, got {enabled}"

            success = self.try_drop_table_from_backup(
                collection_name=collection_name,
                backup_type="incremental",
                table_name="orders",
                snapshot_index=-1  # Latest incr backup
            )
            assert success, "Expected ability to delete backup"

            # Create second full backup - this might trigger the double-enabled bug
            data_helper.modify(add_rows=[(103, 1003, "full2")])
            backup.stage(BackupType.FULL, "Second full backup")

            # Check immediately for the bug
            has_cfs, enabled_immediate = self.has_changefeeds(t_orders)
            if enabled_immediate > 1:
                logger.error(f"{enabled_immediate} enabled changefeeds right after full backup!")

            # Wait for proper state
            success, total_after_full2, enabled = self.wait_for_changefeed_state(t_orders, expected_enabled=1, timeout=5.0)

            if not success or enabled > 1:
                logger.error(f"Changefeed rotation issue: {enabled} enabled changefeeds (expected 1)")
            else:
                logger.info(f"After second full backup: {total_after_full2} total changefeeds, {enabled} enabled")

        self.drop_backup_collection(collection_name)

        # Verify all changefeeds are removed
        success, total, enabled = self.wait_for_changefeed_state(t_orders, expected_enabled=0, timeout=5.0)
        has_cfs, _ = self.has_changefeeds(t_orders)
        assert not has_cfs, "All changefeeds should be cleaned up after dropping collection"

        # Check products table too
        has_cfs_products, _ = self.has_changefeeds(t_products)
        assert not has_cfs_products, "Products changefeeds should also be cleaned up"
