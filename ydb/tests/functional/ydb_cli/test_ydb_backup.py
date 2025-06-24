# -*- coding: utf-8 -*-

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb

from hamcrest import assert_that, is_, is_not, contains_inanyorder, has_items, equal_to, empty
import enum
import logging
import os
import pytest

import yatest

logger = logging.getLogger(__name__)


def backup_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
    raise RuntimeError("YDB_CLI_BINARY enviroment variable is not specified")


def upsert_simple(session, full_path):
    path, table = os.path.split(full_path)
    session.transaction().execute(
        """
        PRAGMA TablePathPrefix("{0}");
        UPSERT INTO {1} (`id`, `number`, `string`, `fixed_point`) VALUES (2, 6,  "pen",       CAST("2.4" AS Decimal(22,9)));
        UPSERT INTO {1} (`id`,           `string`, `fixed_point`) VALUES (3,     "pineapple", CAST("3.5" AS Decimal(22,9)));
        UPSERT INTO {1} (`id`, `number`,          `fixed_point`) VALUES (5, 12,              CAST("512.6" AS Decimal(22,9)));
        UPSERT INTO {1} (`id`, `number`, `string`             ) VALUES (7, 15, "pen"          );
        """.format(path, table),
        commit_tx=True,
    )


def output_path(*args):
    path = os.path.join(yatest.common.output_path(), *args)
    os.makedirs(path, exist_ok=False)
    return path


def list_to_string(arr, formatter=lambda x: x):
    string = "{"
    needsComma = False
    for x in arr:
        if needsComma:
            string += ", "
        needsComma = True
        string += formatter(x)
    string += "}"
    return string


def columns_to_string(columns):
    return list_to_string(columns, lambda col: col.name + ":" + str(col.type.item).strip())


def permissions_to_string(permissions):
    return list_to_string(permissions, lambda p: p.subject + ":" + str(p.permission_names))


def sort_permissions(permissions):
    for permission in permissions:
        permission.permission_names = sorted(permission.permission_names)
    return sorted(permissions, key=lambda p: p.subject)


def create_table_with_data(session, path, not_null=False):
    path = "/Root/" + path
    session.create_table(
        path,
        ydb.TableDescription()
        .with_column(ydb.Column("id", ydb.PrimitiveType.Uint32 if not_null else ydb.OptionalType(ydb.PrimitiveType.Uint32)))
        .with_column(ydb.Column("number", ydb.OptionalType(ydb.PrimitiveType.Uint64)))
        .with_column(ydb.Column("string", ydb.OptionalType(ydb.PrimitiveType.String)))
        .with_column(ydb.Column("fixed_point", ydb.OptionalType(ydb.DecimalType())))
        .with_primary_keys("id")
    )

    upsert_simple(session, path)


def modify_permissions(scheme_client, path):
    path = "/Root/" + path
    scheme_client.modify_permissions(
        path,
        ydb.ModifyPermissionsSettings()
        .grant_permissions(
            "alice", (
                "ydb.generic.read",
                "ydb.generic.write",
            )
        )
        .grant_permissions(
            "bob", (
                "ydb.generic.read",
            )
        )
        .change_owner("eve")
    )


def is_tables_the_same(session, scheme_client, path_left, path_right, check_data=True):
    if not is_tables_descriptions_the_same(session, path_left, path_right):
        return False

    if not is_permissions_the_same(scheme_client, path_left, path_right):
        return False

    if check_data:
        return is_data_the_same(session, path_left, path_right)

    return True


def is_tables_descriptions_the_same(session, path_left, path_right):
    table_desc_left = session.describe_table(path_left)
    table_desc_right = session.describe_table(path_right)
    if (
        sorted(table_desc_left.columns, key=lambda x: x.name) != sorted(table_desc_right.columns, key=lambda x: x.name)
            or table_desc_left.primary_key != table_desc_right.primary_key):
        left_cols = columns_to_string(table_desc_left.columns)
        left_pk = list_to_string(table_desc_left.primary_key)
        right_cols = columns_to_string(table_desc_right.columns)
        right_pk = list_to_string(table_desc_right.primary_key)
        logging.debug("Tables descriptions (is not the same)!" +
                      "\npath_left# " + path_left + " has columns# " + left_cols + " primary_key# " + left_pk +
                      "\npath_right# " + path_right + " has columns# " + right_cols + " primary_key# " + right_pk)
        return False
    return True


def is_data_the_same(session, path_left, path_right):
    table_it_left = session.read_table(path_left, ordered=True)
    table_it_right = session.read_table(path_right, ordered=True)
    left_rows = []
    right_rows = []
    processed_rows = 0
    while True:
        if len(left_rows) == 0:
            try:
                left_rows = next(table_it_left).rows
            except StopIteration:
                if len(right_rows) == 0:
                    return True
                else:
                    logging.debug(path_left + " is shorter than " + path_right + " processed# " + str(processed_rows) +
                                  " len(right_rows)#" + str(len(right_rows)))
                    return False
        if len(right_rows) == 0:
            try:
                right_rows = next(table_it_right).rows
            except StopIteration:
                if len(left_rows) == 0:
                    return True
                else:
                    logging.debug(path_right + " is shorter than " + path_left + " processed# " + str(processed_rows) +
                                  " len(left_rows)#" + str(len(left_rows)))
                    return False

        rows_to_process = min(len(left_rows), len(right_rows))
        for i in range(rows_to_process):
            if left_rows[i] != right_rows[i]:
                logging.debug(str(left_rows[i]) + " != " + str(right_rows[i]))
                return False
        processed_rows += rows_to_process
        left_rows = left_rows[rows_to_process:]
        right_rows = right_rows[rows_to_process:]


def is_permissions_the_same(scheme_client, path_left, path_right):
    path_left_desc = scheme_client.describe_path(path_left)
    path_right_desc = scheme_client.describe_path(path_right)

    path_left_permissions = permissions_to_string(sort_permissions(path_left_desc.permissions))
    path_right_permsissions = permissions_to_string(sort_permissions(path_right_desc.permissions))

    if path_left_desc.owner != path_right_desc.owner or path_left_permissions != path_right_permsissions:
        logging.debug("Permissions (is not the same)!" +
                      "\npath_left# " + path_left + " has owner# " + path_left_desc.owner +
                      " permissions# " + path_left_permissions +
                      "\npath_right# " + path_right + " has owner# " + path_right_desc.owner +
                      " permissions# " + path_right_permsissions)
        return False
    return True


@enum.unique
class ListMode(enum.IntEnum):
    DIRS = 0,
    FILES = 1,


def fs_recursive_list(prefix, mode=ListMode.DIRS, path=""):
    paths = []
    full_path = os.path.join(prefix, path)
    logger.debug("prefix# " + prefix + " path# " + path)
    for item in os.listdir(full_path):
        item_path = os.path.join(full_path, item)
        if os.path.isdir(item_path):
            if mode == ListMode.DIRS:
                paths.append(os.path.join(path, item))
            paths += fs_recursive_list(prefix, mode, os.path.join(path, item))
        elif os.path.isfile(item_path):
            if mode == ListMode.FILES:
                paths.append(os.path.join(path, item))
    return paths


def is_system_object(object):
    return object.name.startswith(".")


class BaseTestBackupInFiles(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(extra_feature_flags=["enable_resource_pools"]))
        cls.cluster.start()
        cls.root_dir = "/Root"
        driver_config = ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port))
        cls.driver = ydb.Driver(driver_config)
        cls.driver.wait(timeout=4)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.fixture(autouse=True, scope='class')
    @classmethod
    def set_test_name(cls, request):
        cls.test_name = request.node.name

    @classmethod
    def create_backup(cls, path, expected_dirs, check_data, additional_args=[]):
        _, name = os.path.split(path)
        backup_files_dir = output_path(cls.test_name, "backup_files_dir_" + path.replace("/", "_"))
        execution = yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", os.path.join('/Root', path),
                "--output", backup_files_dir
            ] +
            additional_args
        )

        logger.debug("std_out:\n" + execution.std_out.decode('utf-8'))
        fs_recursive_list(backup_files_dir)
        logger.debug("fs_recursive_list(backup_files_dir)# " + str(fs_recursive_list(backup_files_dir)))
        logger.debug("expected_dirs# " + str(expected_dirs))

        assert_that(
            fs_recursive_list(backup_files_dir),
            has_items(*expected_dirs)
        )

        for _dir in expected_dirs:
            if check_data:
                assert_that(
                    os.listdir(backup_files_dir + "/" + _dir),
                    contains_inanyorder("data_00.csv", "scheme.pb", "permissions.pb")
                )
            else:
                assert_that(
                    os.listdir(backup_files_dir + "/" + _dir),
                    has_items("scheme.pb", "permissions.pb")
                )

    def scheme_listdir(self, path):
        return [
            child.name
            for child in self.driver.scheme_client.list_directory(path).children
            if not is_system_object(child)
        ]

    def create_user(self, user, password="password"):
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "yql",
                "--script", f"CREATE USER {user} PASSWORD '{password}'",
            ]
        )

    def create_users(self):
        self.create_user("alice")
        self.create_user("bob")
        self.create_user("eve")


class TestBackupSingle(BaseTestBackupInFiles):
    def test_single_table_backup(self):
        session = self.driver.table_client.session().create()
        # Create table
        path = "table"
        create_table_with_data(session, path)

        # Backup table
        self.create_backup(path, [path], False)

        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("table", ".metadata", ".sys")
        )


class TestBackupSingleNotNull(BaseTestBackupInFiles):
    def test_single_table_backup(self):
        session = self.driver.table_client.session().create()
        # Create table
        path = "table"
        create_table_with_data(session, path, True)

        # Backup table
        self.create_backup(path, [path], False)

        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("table", ".metadata", ".sys")
        )


class TestBaseSingleFromDifPlaces(BaseTestBackupInFiles):
    def test_single_table_backup_from_different_places(self):
        session = self.driver.table_client.session().create()
        # Create table
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )
        self.driver.scheme_client.make_directory(
            '/Root/folder/sub_folder'
        )
        tables_paths = [
            "first",
            "second",
            "folder/third",
            "folder/fourth",
            "folder/sub_folder/fifth",
        ]

        for path in tables_paths:
            create_table_with_data(session, path)

        # Backup table
        for path in tables_paths:
            _, table_name = os.path.split(path)
            self.create_backup(path, [table_name], True)


class TestRecursiveNonConsistent(BaseTestBackupInFiles):
    def test_recursive_table_backup_from_different_places(self):
        session = self.driver.table_client.session().create()
        # Create table
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )
        self.driver.scheme_client.make_directory(
            '/Root/folder/sub_folder'
        )
        tables_paths = [
            "first",
            "second",
            "folder/third",
            "folder/fourth",
            "folder/sub_folder/fifth",
        ]

        for path in tables_paths:
            create_table_with_data(session, path)

        # Backup all tables from Root recursively
        self.create_backup("/Root", tables_paths, True, ["--consistency-level", "table"])

        # Backup single table
        self.create_backup("first", ["first"], True, ["--consistency-level", "table"])
        self.create_backup("folder/third", ["third"], True, ["--consistency-level", "table"])

        # Backup tables from folder recursively
        tables_paths = [
            "third",
            "fourth",
            "sub_folder/fifth",
        ]
        self.create_backup("folder", tables_paths, True, ["--consistency-level", "table"])

        # Backup table from sub_folder recursively
        tables_paths = [
            "fifth",
        ]
        self.create_backup("folder/sub_folder", tables_paths, True, ["--consistency-level", "table"])


class TestRecursiveSchemeOnly(BaseTestBackupInFiles):
    def test_recursive_table_backup_from_different_places(self):
        session = self.driver.table_client.session().create()
        # Create table
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )
        self.driver.scheme_client.make_directory(
            '/Root/folder/sub_folder'
        )
        tables_paths = [
            "first",
            "second",
            "folder/third",
            "folder/fourth",
            "folder/sub_folder/fifth",
        ]

        for path in tables_paths:
            create_table_with_data(session, path)

        # Backup all tables from Root recursively
        self.create_backup("/Root", tables_paths, False, ["--scheme-only"])

        # Backup single table
        self.create_backup("first", ["first"], False, ["--scheme-only"])
        self.create_backup("folder/third", ["third"], False, ["--scheme-only"])

        # Backup tables from folder recursively
        tables_paths = [
            "third",
            "fourth",
            "sub_folder/fifth",
        ]
        self.create_backup("folder", tables_paths, False, ["--scheme-only"])

        # Backup table from sub_folder recursively
        tables_paths = [
            "fifth",
        ]
        self.create_backup("folder/sub_folder", tables_paths, False, ["--scheme-only"])


class TestRecursiveConsistent(BaseTestBackupInFiles):
    def test_recursive_table_backup_from_different_places(self):
        session = self.driver.table_client.session().create()
        # Create table
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )
        self.driver.scheme_client.make_directory(
            '/Root/folder/sub_folder'
        )
        tables_paths = [
            "first",
            "second",
            "folder/third",
            "folder/fourth",
            "folder/sub_folder/fifth",
        ]

        for path in tables_paths:
            create_table_with_data(session, path)

        # Backup all tables from Root recursively
        self.create_backup("/Root", tables_paths, True, ["--consistency-level", "database"])

        # Backup single table
        self.create_backup("first", ["first"], True, ["--consistency-level", "database"])
        self.create_backup("folder/third", ["third"], True, ["--consistency-level", "database"])

        # Backup tables from folder recursively
        tables_paths = [
            "third",
            "fourth",
            "sub_folder/fifth",
        ]
        self.create_backup("folder", tables_paths, True, ["--consistency-level", "database"])

        # Backup table from sub_folder recursively
        tables_paths = [
            "fifth",
        ]
        self.create_backup("folder/sub_folder", tables_paths, True, ["--consistency-level", "database"])


class TestSingleBackupRestore(BaseTestBackupInFiles):
    def test_single_table_with_data_backup_restore(self):
        self._test_single_table_with_data_backup_restore_impl(False, False)
        self._test_single_table_with_data_backup_restore_impl(False, True)
        self._test_single_table_with_data_backup_restore_impl(True, False)
        self._test_single_table_with_data_backup_restore_impl(True, True)

    @classmethod
    def _test_single_table_with_data_backup_restore_impl(self, use_bulk_upsert, not_null):
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )
        postfix = '_bulk_upsert' if use_bulk_upsert else ''
        postfix += '_not_null' if not_null else ''

        session = self.driver.table_client.session().create()

        # Create table and fill with data
        create_table_with_data(session, "folder/table", not_null)

        # Backup table
        backup_files_dir = output_path(self.test_name, 'test_single_table_with_data_backup_restore' + postfix, "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root/folder",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )

        # Restore table
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored" + postfix,
            "--input", backup_files_dir
        ]
        if use_bulk_upsert:
            restore_cmd.append("--bulk-upsert")
        yatest.common.execute(restore_cmd)

        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", "restored" + postfix, ".metadata", ".sys")
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/restored" + postfix).children],
            is_(["table"])
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored" + postfix + "/table"),
            is_(True)
        )
        session.drop_table("/Root/restored" + postfix + "/table")
        self.driver.scheme_client.remove_directory("/Root/restored" + postfix)


class TestBackupRestoreInRoot(BaseTestBackupInFiles):
    def test_table_backup_restore_in_root(self):
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )

        session = self.driver.table_client.session().create()

        # Create table and fill with data
        create_table_with_data(session, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, 'test_single_table_with_data_backup_restore', "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root/folder",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )

        # Restore table
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "restore",
                "--path", "/Root/",
                "--input", backup_files_dir
            ]
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", "table", ".metadata", ".sys")
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/table"),
            is_(True)
        )


class TestBackupRestoreInRootSchemeOnly(BaseTestBackupInFiles):
    def test_table_backup_restore_in_root_scheme_only(self):
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )

        session = self.driver.table_client.session().create()

        # Create table and fill with data
        create_table_with_data(session, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, 'test_single_table_with_data_backup_restore', "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--scheme-only",
                "--path", "/Root/folder",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )

        # Restore table
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "restore",
                "--path", "/Root/",
                "--input", backup_files_dir
            ]
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", "table", ".metadata", ".sys")
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/table", False),
            is_(True)
        )


class TestIncompleteBackup(BaseTestBackupInFiles):
    def test_incomplete_backup_will_not_be_restored(self):
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )

        session = self.driver.table_client.session().create()

        create_table_with_data(session, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                'tools', 'dump',
                "--path", '/Root/folder',
                "--output", backup_files_dir
            ]
        )
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )

        # Create "incomplete" file in folder with backup files
        open(os.path.join(backup_files_dir, "incomplete"), "w").close()
        open(os.path.join(backup_files_dir, "table", "incomplete"), "w").close()

        # Restore table and check that it fails without restoring anything
        execution = yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                'tools', 'restore',
                "--path", "/Root/restored",
                "--input", backup_files_dir
            ],
            check_exit_code=False
        )
        assert_that(
            execution.exit_code,
            is_not(0)
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/folder").children],
            is_(["table"])
        )

        execution = yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root"
                'tools', 'restore',
                "--path", "/Root/restored",
                "--input", os.path.join(backup_files_dir, "table")
            ],
            check_exit_code=False
        )

        assert_that(
            execution.exit_code,
            is_not(0)
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/folder").children],
            is_(["table"])
        )


class TestAlterBackupRestore(BaseTestBackupInFiles):
    def test_alter_table_with_data_backup_restore(self):
        self.driver.scheme_client.make_directory(
            '/Root/folder'
        )

        session = self.driver.table_client.session().create()

        # Create table and fill with data
        path = "/Root/folder/table"
        session.create_table(
            path,
            ydb.TableDescription()
            .with_column(ydb.Column("a", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
            .with_column(ydb.Column("b", ydb.OptionalType(ydb.PrimitiveType.String)))
            .with_column(ydb.Column("c", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
            .with_column(ydb.Column("d", ydb.OptionalType(ydb.PrimitiveType.String)))
            .with_column(ydb.Column("e", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
            .with_column(ydb.Column("f", ydb.OptionalType(ydb.PrimitiveType.String)))
            .with_column(ydb.Column("g", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
            .with_column(ydb.Column("h", ydb.OptionalType(ydb.PrimitiveType.String)))
            .with_primary_keys("a")
        )
        prefix, table = os.path.split(path)
        session.transaction().execute(
            """
            PRAGMA TablePathPrefix("{0}");
            UPSERT INTO {1} (a, b, c, d, e, f, g, h) VALUES (5, "b", 5, "b", 5, "b", 5, "b");
            """.format(prefix, table),
            commit_tx=True,
        )
        session.alter_table(
            path,
            [],
            ['b']
        )

        # Backup table
        backup_files_dir = output_path(self.test_name, 'test_single_table_with_data_backup_restore', "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root/folder",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )

        # Restore table
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "restore",
                "--path", "/Root/restored",
                "--input", backup_files_dir
            ]
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", "restored", ".metadata", ".sys")
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/restored").children],
            is_(["table"])
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/table"),
            is_(True)
        )


class TestPermissionsBackupRestoreSingleTable(BaseTestBackupInFiles):
    def test_single_table(self):
        self.driver.scheme_client.make_directory("/Root/folder")

        session = self.driver.table_client.session().create()

        # Create table and modify permissions on it
        self.create_users()
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, "test_single_table", "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root/folder",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore table
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored",
            "--input", backup_files_dir
        ]
        yatest.common.execute(restore_cmd)

        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            self.scheme_listdir("/Root/restored"),
            is_(["table"])
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/table"),
            is_(True)
        )


class TestPermissionsBackupRestoreFolderWithTable(BaseTestBackupInFiles):
    def test_folder_with_table(self):
        # Create folder and modify permissions on it
        self.create_users()
        self.driver.scheme_client.make_directory("/Root/folder")
        modify_permissions(self.driver.scheme_client, "folder")

        session = self.driver.table_client.session().create()

        # Create table and modify permissions on it
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup folder with table
        backup_files_dir = output_path(self.test_name, "test_folder_with_table", "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore folder with table
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored",
            "--input", backup_files_dir
        ]
        yatest.common.execute(restore_cmd)

        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/", "/Root/restored/folder/"),
            is_(True)
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/folder/table"),
            is_(True)
        )


class TestPermissionsBackupRestoreDontOverwriteOnAlreadyExisting(BaseTestBackupInFiles):
    def test_dont_overwrite_on_already_existing(self):
        # Create folder and modify permissions on it
        self.create_users()
        self.driver.scheme_client.make_directory("/Root/folder")
        modify_permissions(self.driver.scheme_client, "folder")

        session = self.driver.table_client.session().create()

        # Create table and modify permissions on it
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup folder with table
        backup_files_dir = output_path(self.test_name, "test_dont_overwrite_on_already_existing", "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Recreate table and folder but without permissions and data
        session.drop_table("/Root/folder/table")
        self.driver.scheme_client.remove_directory("/Root/folder")
        self.driver.scheme_client.make_directory("/Root/folder")
        session.create_table(
            "/Root/folder/table",
            ydb.TableDescription()
            .with_column(ydb.Column("id", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
            .with_column(ydb.Column("number", ydb.OptionalType(ydb.PrimitiveType.Uint64)))
            .with_column(ydb.Column("string", ydb.OptionalType(ydb.PrimitiveType.String)))
            .with_column(ydb.Column("fixed_point", ydb.OptionalType(ydb.DecimalType())))
            .with_primary_keys("id")
        )
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore folder with table in already existing folder and table
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root",
            "--input", backup_files_dir
        ]
        yatest.common.execute(restore_cmd)
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore folder with table one more time in another folder
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored",
            "--input", backup_files_dir
        ]
        yatest.common.execute(restore_cmd)

        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )

        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/", "/Root/restored/folder/"),
            is_(False)
        )
        assert_that(
            is_tables_descriptions_the_same(session, "/Root/folder/table", "/Root/restored/folder/table"),
            is_(True)
        )
        assert_that(
            is_data_the_same(session, "/Root/folder/table", "/Root/restored/folder/table"),
            is_(True)
        )
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/table", "/Root/restored/folder/table"),
            is_(False)
        )


class TestPermissionsBackupRestoreSchemeOnly(BaseTestBackupInFiles):
    def test_scheme_only(self):
        # Create folder and modify permissions on it
        self.create_users()
        self.driver.scheme_client.make_directory("/Root/folder")
        modify_permissions(self.driver.scheme_client, "folder")

        session = self.driver.table_client.session().create()

        # Create table and modify permissions on it
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup folder with table
        backup_files_dir = output_path(self.test_name, "test_scheme_only", "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root",
                "--output", backup_files_dir,
                "--scheme-only",
            ]
        )
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore folder with table
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored",
            "--input", backup_files_dir,
        ]
        yatest.common.execute(restore_cmd)

        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/", "/Root/restored/folder/"),
            is_(True)
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/folder/table", False),
            is_(True)
        )


class TestPermissionsBackupRestoreEmptyDir(BaseTestBackupInFiles):
    def test_empty_dir(self):
        # Create empty folder and modify permissions on it
        self.create_users()
        self.driver.scheme_client.make_directory("/Root/folder")
        modify_permissions(self.driver.scheme_client, "folder")

        # Backup folder
        backup_files_dir = output_path(self.test_name, "test_empty_dir", "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore folder
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored",
            "--input", backup_files_dir
        ]
        yatest.common.execute(restore_cmd)

        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/", "/Root/restored/folder/"),
            is_(True)
        )


class TestRestoreACLOption(BaseTestBackupInFiles):
    def test_restore_acl_option(self):
        self.driver.scheme_client.make_directory("/Root/folder")

        session = self.driver.table_client.session().create()

        # Create table and modify permissions on it
        self.create_users()
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, "test_single_table", "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root/folder",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore table
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored",
            "--input", backup_files_dir,
            "--restore-acl", "false"
        ]
        yatest.common.execute(restore_cmd)

        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            self.scheme_listdir("/Root/restored"),
            is_(["table"])
        )
        assert_that(
            is_tables_descriptions_the_same(session, "/Root/folder/table", "/Root/restored/table"),
            is_(True)
        )
        assert_that(
            is_data_the_same(session, "/Root/folder/table", "/Root/restored/table"),
            is_(True)
        )
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/table", "/Root/restored/table"),
            is_(False)
        )


class TestRestoreNoData(BaseTestBackupInFiles):
    def test_restore_no_data(self):
        self.driver.scheme_client.make_directory("/Root/folder")

        session = self.driver.table_client.session().create()

        # Create table and modify permissions on it
        self.create_users()
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, "test_single_table", "backup_files_dir")
        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
                "tools", "dump",
                "--path", "/Root/folder",
                "--output", backup_files_dir
            ]
        )
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore table
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored",
            "--input", backup_files_dir,
            "--restore-data", "false",
        ]
        yatest.common.execute(restore_cmd)

        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            self.scheme_listdir("/Root/restored"),
            is_(["table"])
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/table", False),
            is_(True)
        )
        assert_that(
            is_data_the_same(session, "/Root/folder/table", "/Root/restored/table"),
            is_(False)
        )


class BaseTestClusterBackupInFiles(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            extra_feature_flags=[
                "enable_strict_acl_check",
                "enable_strict_user_management",
                "enable_database_admin"
            ],
            domain_login_only=False,
            enforce_user_token_requirement=True,
            default_clusteradmin="root@builtin",
        ))
        cls.cluster.start()

        cls.root_dir = "/Root"
        cls.database = os.path.join(cls.root_dir, "db1")

        cls.cluster.create_database(
            cls.database,
            storage_pool_units_count={
                'hdd': 1
            },
            timeout_seconds=240,
            token=cls.cluster.config.default_clusteradmin
        )

        cls.database_nodes = cls.cluster.register_and_start_slots(cls.database, count=3)
        cls.cluster.wait_tenant_up(cls.database, cls.cluster.config.default_clusteradmin)

        driver_config = ydb.DriverConfig(
            database=cls.database,
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port),
            credentials=ydb.AuthTokenCredentials(cls.cluster.config.default_clusteradmin))
        cls.driver = ydb.Driver(driver_config)
        cls.driver.wait(timeout=4)

    @classmethod
    def teardown_class(cls):
        cls.cluster.unregister_and_stop_slots(cls.database_nodes)
        cls.cluster.stop()

    @pytest.fixture(autouse=True, scope='class')
    @classmethod
    def set_test_name(cls, request):
        cls.test_name = request.node.name

    @classmethod
    def create_backup(cls, command, expected_files, output, additional_args=[], token=""):
        backup_files_dir = output_path(cls.test_name, output)
        execution = yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            ]
            + command
            + ["--output", backup_files_dir]
            + additional_args,
            env={"YDB_TOKEN": token}
        )

        list_result = fs_recursive_list(backup_files_dir, ListMode.FILES)

        logger.debug("std_out:\n" + execution.std_out.decode('utf-8'))
        logger.debug("fs_recursive_list(backup_files_dir)# " + str(list_result))
        logger.debug("expected_files# " + str(expected_files))

        assert_that(
            len(list_result),
            equal_to(len(expected_files))
        )

        assert_that(
            list_result,
            has_items(*expected_files)
        )

    @classmethod
    def create_cluster_backup(cls, expected_files, output="backup_files_dir", additional_args=[]):
        cls.create_backup(
            [
                "--database", cls.root_dir,
                "admin", "cluster", "dump",
            ],
            expected_files,
            output,
            additional_args,
            token=cls.cluster.config.default_clusteradmin,
        )

    @classmethod
    def create_database_backup(cls, expected_files, output="backup_files_dir", additional_args=[]):
        cls.create_backup(
            [
                "--database", cls.database,
                "--user", "dbadmin1", "--no-password",
                "admin", "database", "dump",
            ],
            expected_files,
            output,
            additional_args
        )

    @classmethod
    def setup_sample_data(cls):
        pool = ydb.SessionPool(cls.driver)
        with pool.checkout() as session:
            session.execute_scheme("CREATE GROUP people")
            session.execute_scheme("CREATE USER alice PASSWORD '1234'")
            session.execute_scheme("CREATE USER bob PASSWORD '1234'")
            session.execute_scheme("ALTER GROUP people ADD USER alice")
            session.execute_scheme("ALTER GROUP people ADD USER bob")

            session.execute_scheme("CREATE GROUP dbadmins")
            session.execute_scheme("CREATE USER dbadmin1")
            session.execute_scheme("CREATE USER dbadmin2 PASSWORD '1234'")
            session.execute_scheme("ALTER GROUP dbadmins ADD USER dbadmin1")
            session.execute_scheme("ALTER GROUP dbadmins ADD USER dbadmin2")
            session.execute_scheme(f'GRANT "ydb.generic.use" on `{cls.database}` TO dbadmins')

            cls.driver.scheme_client.modify_permissions(
                cls.database,
                ydb.ModifyPermissionsSettings().change_owner("dbadmins")
            )

        create_table_with_data(cls.driver.table_client.session().create(), "db1/table")


class TestClusterBackup(BaseTestClusterBackupInFiles):
    def test_cluster_backup(self):
        self.setup_sample_data()

        self.create_cluster_backup(expected_files=[
            # cluster metadata
            "permissions.pb",
            "create_user.sql",
            "create_group.sql",
            "alter_group.sql",

            # database metadata
            "Root/db1/database.pb",
            "Root/db1/permissions.pb",
            "Root/db1/create_user.sql",
            "Root/db1/create_group.sql",
            "Root/db1/alter_group.sql",
        ])


class TestDatabaseBackup(BaseTestClusterBackupInFiles):
    def test_database_backup(self):
        self.setup_sample_data()

        self.create_database_backup(expected_files=[
            # database metadata
            "database.pb",
            "permissions.pb",
            "create_user.sql",
            "create_group.sql",
            "alter_group.sql",

            # database table
            "table/scheme.pb",
            "table/permissions.pb",
            "table/data_00.csv",
        ])


class BaseTestMultipleClusterBackupInFiles(BaseTestClusterBackupInFiles):
    @classmethod
    def setup_class(cls):
        super().setup_class()

        cfg = KikimrConfigGenerator(
            extra_feature_flags=[
                "enable_strict_acl_check",
                "enable_strict_user_management",
                "enable_database_admin"
            ],
            domain_name="Root2",
            domain_login_only=False,
            enforce_user_token_requirement=True,
            default_clusteradmin="root@builtin",
        )

        cls.restore_cluster = KiKiMR(
            cfg,
            cluster_name="restore_cluster"
        )

        cls.restore_cluster.start()

        cls.restore_root_dir = "/Root2"
        cls.restore_database = os.path.join(cls.restore_root_dir, "db1")
        cls.restore_database_nodes = cls.restore_cluster.register_and_start_slots(
            cls.restore_database,
            count=1
        )

        restore_driver_config = ydb.DriverConfig(
            database=cls.restore_database,
            endpoint="%s:%s" % (
                cls.restore_cluster.nodes[1].host,
                cls.restore_cluster.nodes[1].port
            ),
            credentials=ydb.AuthTokenCredentials(cls.cluster.config.default_clusteradmin),
        )
        cls.restore_driver = ydb.Driver(restore_driver_config)

    @classmethod
    def teardown_class(cls):
        cls.restore_cluster.unregister_and_stop_slots(cls.restore_database_nodes)
        cls.restore_cluster.stop()
        super().teardown_class()

    @classmethod
    def restore(cls, command, input, additional_args=[], token=""):
        backup_files_dir = os.path.join(
            yatest.common.output_path(),
            cls.test_name,
            input
        )

        yatest.common.execute(
            [
                backup_bin(),
                "--verbose",
                "--assume-yes",
                "--endpoint", f"grpc://localhost:{cls.restore_cluster.nodes[1].grpc_port}",
            ]
            + command
            + ["--input", backup_files_dir, "-w", "240s"]
            + additional_args,
            env={"YDB_TOKEN": token}
        )

    @classmethod
    def restore_cluster_backup(cls, input="backup_files_dir", additional_args=[]):
        cls.restore(
            [
                "--database", cls.restore_root_dir,
                "admin", "cluster", "restore"
            ],
            input,
            additional_args,
            token=cls.restore_cluster.config.default_clusteradmin,
        )

    @classmethod
    def restore_database_backup(cls, input="backup_files_dir", additional_args=[]):
        cls.restore(
            [
                "--database", cls.restore_database,
                "--user", "dbadmin1", "--no-password",
                "admin", "database", "restore"
            ],
            input,
            additional_args
        )


class TestClusterBackupRestore(BaseTestMultipleClusterBackupInFiles):
    def test_cluster_backup_restore(self):
        self.setup_sample_data()

        self.create_cluster_backup(expected_files=[
            # cluster metadata
            "permissions.pb",
            "create_user.sql",
            "create_group.sql",
            "alter_group.sql",

            # database metadata
            "Root/db1/database.pb",
            "Root/db1/permissions.pb",
            "Root/db1/create_user.sql",
            "Root/db1/create_group.sql",
            "Root/db1/alter_group.sql",
        ])

        self.restore_cluster_backup()

        self.restore_cluster.wait_tenant_up(self.restore_database, self.cluster.config.default_clusteradmin)
        self.restore_driver.wait(timeout=10)


class TestDatabaseBackupRestore(BaseTestMultipleClusterBackupInFiles):
    def test_database_backup_restore(self):
        self.setup_sample_data()

        self.create_cluster_backup(output="cluster_backup", expected_files=[
            # cluster metadata
            "permissions.pb",
            "create_user.sql",
            "create_group.sql",
            "alter_group.sql",

            # database metadata
            "Root/db1/database.pb",
            "Root/db1/permissions.pb",
            "Root/db1/create_user.sql",
            "Root/db1/create_group.sql",
            "Root/db1/alter_group.sql",
        ])

        self.create_database_backup(output="database_backup", expected_files=[
            # database metadata
            "database.pb",
            "permissions.pb",
            "create_user.sql",
            "create_group.sql",
            "alter_group.sql",

            # database table
            "table/scheme.pb",
            "table/permissions.pb",
            "table/data_00.csv",
        ])

        self.restore_cluster_backup(input="cluster_backup")

        self.restore_cluster.wait_tenant_up(self.restore_database, self.cluster.config.default_clusteradmin)
        self.restore_driver.wait(timeout=10)

        self.restore_database_backup(input="database_backup")


class TestRestoreReplaceOption(BaseTestBackupInFiles):
    def ydb_cli(self, args):
        yatest.common.execute(
            [
                backup_bin(),
                "-vvv",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database", "/Root",
            ]
            + args
        )

    def try_ydb_cli(self, args):
        try:
            result = self.ydb_cli(args)
            return True, result.std_out.decode("utf-8")

        except yatest.common.process.ExecutionError as exception:
            return False, exception.execution_result.std_err.decode("utf-8")

    def delete_some_rows(self, session, table):
        session.transaction().execute(
            f"""
            DELETE FROM `{table}` WHERE id == 2;
            """,
            commit_tx=True,
        )

    def test_table_replacement(self):
        session = self.driver.table_client.session().create()

        create_table_with_data(session, "table")

        # Backup the table
        backup_files_dir = output_path(self.test_name, "test_table_replacement", "backup_files_dir")
        self.ydb_cli(["tools", "dump", "--path", ".", "--output", backup_files_dir])
        assert_that(os.listdir(backup_files_dir), is_(["table"]))

        # Restore the table
        assert_that(
            self.try_ydb_cli(
                ["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir, "--dry-run"]
            )[0],
            is_(False),
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            is_not(has_items("restoration")),
        )
        self.ydb_cli(["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir])
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/table", "/Root/restoration/point/table"),
            is_(True),
        )

        # Replace the table
        self.delete_some_rows(session, "/Root/restoration/point/table")
        self.ydb_cli(
            ["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir, "--replace", "--dry-run"]
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/table", "/Root/restoration/point/table"),
            is_(False),
        )
        self.ydb_cli(["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir, "--replace"])
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/table", "/Root/restoration/point/table"),
            is_(True),
        )

        # Drop the folder
        self.ydb_cli(["scheme", "rmdir", "--force", "--recursive", "./restoration/point"])

        # Replace a non-existent table with the --verify-existence option turned on
        assert_that(
            self.try_ydb_cli(
                [
                    "tools",
                    "restore",
                    "--path", "./restoration/point",
                    "--input", backup_files_dir,
                    "--replace",
                    "--verify-existence",
                    "--dry-run",
                ]
            )[0],
            is_(False),
        )
        assert_that(
            self.try_ydb_cli(
                [
                    "tools",
                    "restore",
                    "--path", "./restoration/point",
                    "--input", backup_files_dir,
                    "--replace",
                    "--verify-existence",
                ]
            )[0],
            is_(False),
        )

        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("table", "restoration", ".metadata", ".sys"),
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/restoration").children],
            empty()
        )

        # Replace a non-existent table
        assert_that(
            self.try_ydb_cli(
                [
                    "tools",
                    "restore",
                    "--path", "./restoration/point",
                    "--input", backup_files_dir,
                    "--replace",
                    "--dry-run",
                ]
            )[0],
            is_(False),
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/restoration").children],
            empty()
        )
        self.ydb_cli(["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir, "--replace"])
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/table", "/Root/restoration/point/table"),
            is_(True),
        )
