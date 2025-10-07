# -*- coding: utf-8 -*-

import os
import time
import logging
import shutil
import re

import yatest
import pytest


from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb

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


def parse_yql_table(text):
    if not text:
        return []

    lines = text.splitlines()
    rows = []
    border_re = re.compile(r'^[\s┌┬┐└┴┘├┼┤─]+$')
    for ln in lines:
        ln = ln.rstrip("\r\n")
        if not ln:
            continue
        if border_re.match(ln):
            continue
        if '│' not in ln:
            continue
        parts = [p.strip() for p in ln.split('│')]
        if parts and parts[0] == '':
            parts = parts[1:]
        if parts and parts[-1] == '':
            parts = parts[:-1]
        rows.append(parts)
    return rows


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

    @classmethod
    def run_tools_dump(cls, path, output_dir):
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
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database",
            cls.root_dir,
            "tools",
            "restore",
            "-p",
            collection_path,
            "-i",
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


class TestFullCycleLocalBackupRestore(BaseTestBackupInFiles):
    def test_full_cycle_local_backup_restore(self):
        collection_src = f"coll_src_{int(time.time())}"
        collection_restore_v1 = f"coll_restore_v1_{int(time.time())}"
        collection_restore_v2 = f"coll_restore_v2_{int(time.time())}"
        export_dir = output_path(self.test_name, collection_src)

        t1 = "orders"
        t2 = "products"
        full_t1 = f"/Root/{t1}"
        full_t2 = f"/Root/{t2}"

        session = self.driver.table_client.session().create()
        create_table_with_data(session, t1)
        create_table_with_data(session, t2)

        yql_cmd = ('PRAGMA TablePathPrefix("/Root"); ' f"SELECT id, number, txt FROM {t1} ORDER BY id;")
        execution = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", yql_cmd],
            check_exit_code=False,
        )
        assert execution.exit_code == 0

        create_collection_sql = (
            f"CREATE BACKUP COLLECTION `{collection_src}`\n"
            f"    ( TABLE `{full_t1}`\n"
            f"    , TABLE `{full_t2}`\n"
            f"    )\n"
            "WITH\n"
            "    ( STORAGE = 'cluster'\n"
            "    , INCREMENTAL_BACKUP_ENABLED = 'false'\n"
            "    );\n"
        )
        create_res = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", create_collection_sql],
            check_exit_code=False,
        )

        stderr_out = ""
        if create_res.std_err:
            stderr_out += create_res.std_err.decode("utf-8")
        if create_res.std_out:
            stderr_out += create_res.std_out.decode("utf-8")
        assert create_res.exit_code == 0, f"CREATE BACKUP COLLECTION failed: {stderr_out}"

        backup_res1 = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", f"BACKUP `{collection_src}`;"],
            check_exit_code=False,
        )
        assert backup_res1.exit_code == 0, "BACKUP (1) failed"

        snap1_res = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", yql_cmd],
            check_exit_code=False,
        )
        out_t1_after_full1_s = snap1_res.std_out.decode("utf-8") if snap1_res.std_out else ""
        out_t1_after_full1_rows = parse_yql_table(out_t1_after_full1_s)

        create_table_with_data(session, "extra_table_1")
        session.transaction().execute(
            'PRAGMA TablePathPrefix("/Root"); UPSERT INTO orders (id, number, txt) VALUES (11, 111, "added1");',
            commit_tx=True,
        )
        backup_res2 = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", f"BACKUP `{collection_src}`;"],
            check_exit_code=False,
        )
        assert backup_res2.exit_code == 0, "BACKUP (2) failed"

        snap2_res = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", yql_cmd],
            check_exit_code=False,
        )
        out_t1_after_full2_s = snap2_res.std_out.decode("utf-8") if snap2_res.std_out else ""
        out_t1_after_full2_rows = parse_yql_table(out_t1_after_full2_s)

        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)
        os.makedirs(export_dir, exist_ok=True)

        dump_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database",
            "/Root",
            "tools",
            "dump",
            "-p",
            f".backups/collections/{collection_src}",
            "-o",
            export_dir,
        ]
        dump_res = yatest.common.execute(dump_cmd, check_exit_code=False)
        assert dump_res.exit_code == 0, "tools dump failed"

        exported_items = sorted([name for name in os.listdir(export_dir)
                                 if os.path.isdir(os.path.join(export_dir, name))])
        assert len(exported_items) >= 2, "Expected at least 2 exported backups for this test"

        create_restore_v1_sql = (
            f"CREATE BACKUP COLLECTION `{collection_restore_v1}`\n"
            f"    ( TABLE `{full_t1}`\n"
            f"    , TABLE `{full_t2}`\n"
            f"    )\n"
            "WITH ( STORAGE = 'cluster' );\n"
        )
        create_restore_v2_sql = (
            f"CREATE BACKUP COLLECTION `{collection_restore_v2}`\n"
            f"    ( TABLE `{full_t1}`\n"
            f"    , TABLE `{full_t2}`\n"
            f"    )\n"
            "WITH ( STORAGE = 'cluster' );\n"
        )

        res_v1 = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", create_restore_v1_sql],
            check_exit_code=False,
        )
        stderr_out = ""
        if res_v1.std_err:
            stderr_out += res_v1.std_err.decode("utf-8")
        if res_v1.std_out:
            stderr_out += res_v1.std_out.decode("utf-8")
        assert res_v1.exit_code == 0, f"CREATE restore collection v1 failed: {stderr_out}"

        res_v2 = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", create_restore_v2_sql],
            check_exit_code=False,
        )
        stderr_out = ""
        if res_v2.std_err:
            stderr_out += res_v2.std_err.decode("utf-8")
        if res_v2.std_out:
            stderr_out += res_v2.std_out.decode("utf-8")
        assert res_v2.exit_code == 0, f"CREATE restore collection v2 failed: {stderr_out}"

        bdir_v1 = os.path.join(export_dir, exported_items[0])
        r1 = yatest.common.execute(
            [backup_bin(), "--verbose", "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "tools", "restore", "-p", f".backups/collections/{collection_restore_v1}", "-i", bdir_v1],
            check_exit_code=False,
        )
        assert r1.exit_code == 0, f"tools restore import v1 failed for {bdir_v1}"

        bdir_v2 = os.path.join(export_dir, exported_items[1])
        r2 = yatest.common.execute(
            [backup_bin(), "--verbose", "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "tools", "restore", "-p", f".backups/collections/{collection_restore_v2}", "-i", bdir_v2],
            check_exit_code=False,
        )
        assert r2.exit_code == 0, f"tools restore import v2 failed for {bdir_v2}"

        rest_call_v1 = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", f"RESTORE `{collection_restore_v1}`;"],
            check_exit_code=False,
        )
        assert rest_call_v1.exit_code != 0, "Expected RESTORE v1 to fail when target tables already exist"

        session.execute_scheme(f"DROP TABLE `{full_t1}`;")
        session.execute_scheme(f"DROP TABLE `{full_t2}`;")
        try:
            session.execute_scheme('DROP TABLE `/Root/extra_table_1`;')
        except Exception:
            pass

        restore_exec_v1 = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", f"RESTORE `{collection_restore_v1}`;"],
            check_exit_code=False,
        )
        assert restore_exec_v1.exit_code == 0, "RESTORE v1 failed"

        select_cmd = [
            backup_bin(),
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database",
            "/Root",
            "yql",
            "--script",
            f'PRAGMA TablePathPrefix("/Root"); SELECT id, number, txt FROM {t1} ORDER BY id;',
        ]
        select_res1 = yatest.common.execute(select_cmd, check_exit_code=False)
        select_text1 = select_res1.std_out.decode("utf-8") if select_res1.std_out else ""
        select_rows1 = parse_yql_table(select_text1)
        assert select_rows1 == out_t1_after_full1_rows, "Restored data (v1) does not match snapshot after full1"

        session.execute_scheme(f"DROP TABLE `{full_t1}`;")
        session.execute_scheme(f"DROP TABLE `{full_t2}`;")
        try:
            session.execute_scheme('DROP TABLE `/Root/extra_table_1`;')
        except Exception:
            pass

        restore_exec_v2 = yatest.common.execute(
            [backup_bin(), "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
             "--database", "/Root", "yql", "--script", f"RESTORE `{collection_restore_v2}`;"],
            check_exit_code=False,
        )
        assert restore_exec_v2.exit_code == 0, "RESTORE v2 failed"

        select_res2 = yatest.common.execute(select_cmd, check_exit_code=False)
        select_text2 = select_res2.std_out.decode("utf-8") if select_res2.std_out else ""
        select_rows2 = parse_yql_table(select_text2)
        assert select_rows2 == out_t1_after_full2_rows, "Restored data (v2) does not match snapshot after full2"

        if os.path.exists(export_dir):
            shutil.rmtree(export_dir)
