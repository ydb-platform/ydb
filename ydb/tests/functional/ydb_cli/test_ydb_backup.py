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
    logger.debug("Getting backup binary path")
    if os.getenv("YDB_CLI_BINARY"):
        binary_path = yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
        logger.info(f"Found backup binary at: {binary_path}")
        return binary_path
    logger.error("YDB_CLI_BINARY environment variable is not specified")
    raise RuntimeError("YDB_CLI_BINARY enviroment variable is not specified")


def upsert_simple(session, full_path):
    logger.info(f"Upserting simple data into table: {full_path}")
    path, table = os.path.split(full_path)
    logger.debug(f"Path: {path}, Table: {table}")
    
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
    logger.info(f"Successfully upserted data into {full_path}")


def output_path(*args):
    path = os.path.join(yatest.common.output_path(), *args)
    logger.debug(f"Creating output path: {path}")
    os.makedirs(path, exist_ok=False)
    logger.info(f"Created output directory: {path}")
    return path


def list_to_string(arr, formatter=lambda x: x):
    logger.debug(f"Converting list to string, array length: {len(arr)}")
    string = "{"
    needsComma = False
    for x in arr:
        if needsComma:
            string += ", "
        needsComma = True
        string += formatter(x)
    string += "}"
    logger.debug(f"Converted list to string: {string}")
    return string


def columns_to_string(columns):
    logger.debug(f"Converting {len(columns)} columns to string")
    result = list_to_string(columns, lambda col: col.name + ":" + str(col.type.item).strip())
    logger.debug(f"Columns string: {result}")
    return result


def permissions_to_string(permissions):
    logger.debug(f"Converting {len(permissions)} permissions to string")
    result = list_to_string(permissions, lambda p: p.subject + ":" + str(p.permission_names))
    logger.debug(f"Permissions string: {result}")
    return result


def sort_permissions(permissions):
    logger.debug(f"Sorting {len(permissions)} permissions")
    for permission in permissions:
        permission.permission_names = sorted(permission.permission_names)
    sorted_perms = sorted(permissions, key=lambda p: p.subject)
    logger.debug("Permissions sorted successfully")
    return sorted_perms


def create_table_with_data(session, path, not_null=False):
    full_path = "/Root/" + path
    logger.info(f"Creating table with data at path: {full_path}, not_null: {not_null}")
    
    session.create_table(
        full_path,
        ydb.TableDescription()
        .with_column(ydb.Column("id", ydb.PrimitiveType.Uint32 if not_null else ydb.OptionalType(ydb.PrimitiveType.Uint32)))
        .with_column(ydb.Column("number", ydb.OptionalType(ydb.PrimitiveType.Uint64)))
        .with_column(ydb.Column("string", ydb.OptionalType(ydb.PrimitiveType.String)))
        .with_column(ydb.Column("fixed_point", ydb.OptionalType(ydb.DecimalType())))
        .with_primary_keys("id")
    )
    logger.info(f"Table schema created for {full_path}")

    upsert_simple(session, full_path)
    logger.info(f"Table {full_path} created and populated with data")


def modify_permissions(scheme_client, path):
    full_path = "/Root/" + path
    logger.info(f"Modifying permissions for path: {full_path}")
    
    scheme_client.modify_permissions(
        full_path,
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
    logger.info(f"Permissions modified for {full_path} - granted read/write to alice, read to bob, changed owner to eve")


def is_tables_the_same(session, scheme_client, path_left, path_right, check_data=True):
    logger.info(f"Comparing tables: {path_left} vs {path_right}, check_data: {check_data}")
    
    if not is_tables_descriptions_the_same(session, path_left, path_right):
        logger.warning(f"Table descriptions differ between {path_left} and {path_right}")
        return False

    if not is_permissions_the_same(scheme_client, path_left, path_right):
        logger.warning(f"Permissions differ between {path_left} and {path_right}")
        return False

    if check_data:
        data_same = is_data_the_same(session, path_left, path_right)
        if not data_same:
            logger.warning(f"Data differs between {path_left} and {path_right}")
        else:
            logger.info(f"Tables {path_left} and {path_right} are identical")
        return data_same

    logger.info(f"Tables {path_left} and {path_right} have same schema and permissions (data not checked)")
    return True


def is_tables_descriptions_the_same(session, path_left, path_right):
    logger.debug(f"Comparing table descriptions: {path_left} vs {path_right}")
    
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
        logger.warning(f"Table descriptions differ: {path_left} != {path_right}")
        return False
    
    logger.debug(f"Table descriptions match for {path_left} and {path_right}")
    return True


def is_data_the_same(session, path_left, path_right):
    logger.debug(f"Comparing table data: {path_left} vs {path_right}")
    
    table_it_left = session.read_table(path_left, ordered=True)
    table_it_right = session.read_table(path_right, ordered=True)
    left_rows = []
    right_rows = []
    processed_rows = 0
    
    while True:
        if len(left_rows) == 0:
            try:
                left_rows = next(table_it_left).rows
                logger.debug(f"Read {len(left_rows)} rows from {path_left}")
            except StopIteration:
                logger.debug(f"Reached end of {path_left}")
                if len(right_rows) == 0:
                    logger.info(f"Data comparison complete: {processed_rows} rows processed, tables are identical")
                    return True
                else:
                    logging.debug(path_left + " is shorter than " + path_right + " processed# " + str(processed_rows) +
                                  " len(right_rows)#" + str(len(right_rows)))
                    logger.warning(f"{path_left} has fewer rows than {path_right}")
                    return False
                    
        if len(right_rows) == 0:
            try:
                right_rows = next(table_it_right).rows
                logger.debug(f"Read {len(right_rows)} rows from {path_right}")
            except StopIteration:
                logger.debug(f"Reached end of {path_right}")
                if len(left_rows) == 0:
                    logger.info(f"Data comparison complete: {processed_rows} rows processed, tables are identical")
                    return True
                else:
                    logging.debug(path_right + " is shorter than " + path_left + " processed# " + str(processed_rows) +
                                  " len(left_rows)#" + str(len(left_rows)))
                    logger.warning(f"{path_right} has fewer rows than {path_left}")
                    return False

        rows_to_process = min(len(left_rows), len(right_rows))
        logger.debug(f"Processing {rows_to_process} rows")
        
        for i in range(rows_to_process):
            if left_rows[i] != right_rows[i]:
                logging.debug(str(left_rows[i]) + " != " + str(right_rows[i]))
                logger.warning(f"Row {processed_rows + i} differs between tables")
                return False
                
        processed_rows += rows_to_process
        left_rows = left_rows[rows_to_process:]
        right_rows = right_rows[rows_to_process:]
        
        if processed_rows % 1000 == 0:
            logger.debug(f"Processed {processed_rows} rows so far")


def is_permissions_the_same(scheme_client, path_left, path_right):
    logger.debug(f"Comparing permissions: {path_left} vs {path_right}")
    
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
        logger.warning(f"Permissions differ between {path_left} and {path_right}")
        return False
    
    logger.debug(f"Permissions match for {path_left} and {path_right}")
    return True


@enum.unique
class ListMode(enum.IntEnum):
    DIRS = 0,
    FILES = 1,


def fs_recursive_list(prefix, mode=ListMode.DIRS, path=""):
    logger.debug(f"Recursively listing filesystem: prefix={prefix}, mode={mode}, path={path}")
    paths = []
    full_path = os.path.join(prefix, path)
    logger.debug("prefix# " + prefix + " path# " + path)
    
    for item in os.listdir(full_path):
        item_path = os.path.join(full_path, item)
        if os.path.isdir(item_path):
            if mode == ListMode.DIRS:
                paths.append(os.path.join(path, item))
                logger.debug(f"Found directory: {os.path.join(path, item)}")
            paths += fs_recursive_list(prefix, mode, os.path.join(path, item))
        elif os.path.isfile(item_path):
            if mode == ListMode.FILES:
                paths.append(os.path.join(path, item))
                logger.debug(f"Found file: {os.path.join(path, item)}")
    
    logger.debug(f"Filesystem listing complete: found {len(paths)} items")
    return paths


def is_system_object(object):
    is_system = object.name.startswith(".")
    if is_system:
        logger.debug(f"Object {object.name} identified as system object")
    return is_system


class BaseTestBackupInFiles(object):
    @classmethod
    def setup_class(cls):
        logger.info(f"Setting up test class: {cls.__name__}")
        cls.cluster = KiKiMR(KikimrConfigGenerator(extra_feature_flags=["enable_resource_pools"]))
        logger.info("Starting KiKiMR cluster")
        cls.cluster.start()
        logger.info("KiKiMR cluster started successfully")
        
        cls.root_dir = "/Root"
        driver_config = ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port))
        logger.info(f"Connecting to YDB at {cls.cluster.nodes[1].host}:{cls.cluster.nodes[1].port}")
        
        cls.driver = ydb.Driver(driver_config)
        cls.driver.wait(timeout=4)
        logger.info("YDB driver connected successfully")

    @classmethod
    def teardown_class(cls):
        logger.info(f"Tearing down test class: {cls.__name__}")
        cls.cluster.stop()
        logger.info("KiKiMR cluster stopped")

    @pytest.fixture(autouse=True, scope='class')
    @classmethod
    def set_test_name(cls, request):
        cls.test_name = request.node.name
        logger.info(f"Setting test name: {cls.test_name}")

    @classmethod
    def create_backup(cls, path, expected_dirs, check_data, additional_args=[]):
        logger.info(f"Creating backup for path: {path}, expected_dirs: {expected_dirs}, check_data: {check_data}")
        logger.debug(f"Additional args: {additional_args}")
        
        _, name = os.path.split(path)
        backup_files_dir = output_path(cls.test_name, "backup_files_dir_" + path.replace("/", "_"))
        
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", os.path.join('/Root', path),
            "--output", backup_files_dir
        ] + additional_args
        
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        execution = yatest.common.execute(cmd)

        logger.debug("std_out:\n" + execution.std_out.decode('utf-8'))
        fs_recursive_list(backup_files_dir)
        backup_dirs = fs_recursive_list(backup_files_dir)
        logger.debug("fs_recursive_list(backup_files_dir)# " + str(backup_dirs))
        logger.debug("expected_dirs# " + str(expected_dirs))

        logger.info("Verifying backup directory structure")
        assert_that(
            backup_dirs,
            has_items(*expected_dirs)
        )

        for _dir in expected_dirs:
            dir_path = backup_files_dir + "/" + _dir
            dir_contents = os.listdir(dir_path)
            logger.debug(f"Directory {_dir} contains: {dir_contents}")
            
            if check_data:
                logger.info(f"Verifying data files in {_dir}")
                assert_that(
                    dir_contents,
                    contains_inanyorder("data_00.csv", "scheme.pb", "permissions.pb")
                )
            else:
                logger.info(f"Verifying schema files in {_dir}")
                assert_that(
                    dir_contents,
                    has_items("scheme.pb", "permissions.pb")
                )
        
        logger.info(f"Backup created successfully for {path}")

    def scheme_listdir(self, path):
        logger.debug(f"Listing scheme directory: {path}")
        children = [
            child.name
            for child in self.driver.scheme_client.list_directory(path).children
            if not is_system_object(child)
        ]
        logger.debug(f"Found {len(children)} non-system objects in {path}: {children}")
        return children

    def create_user(self, user, password="password"):
        logger.info(f"Creating user: {user}")
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
        logger.info(f"User {user} created successfully")

    def create_users(self):
        logger.info("Creating test users: alice, bob, eve")
        self.create_user("alice")
        self.create_user("bob")
        self.create_user("eve")
        logger.info("All test users created successfully")


class TestBackupSingle(BaseTestBackupInFiles):
    def test_single_table_backup(self):
        logger.info("Starting test_single_table_backup")
        session = self.driver.table_client.session().create()
        logger.info("YDB session created")
        
        # Create table
        path = "table"
        create_table_with_data(session, path)

        # Backup table
        self.create_backup(path, [path], False)

        logger.info("Verifying database structure after backup")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("table", ".metadata", ".sys")
        )
        logger.info("test_single_table_backup completed successfully")


class TestBackupSingleNotNull(BaseTestBackupInFiles):
    def test_single_table_backup(self):
        logger.info("Starting test_single_table_backup with not null constraints")
        session = self.driver.table_client.session().create()
        logger.info("YDB session created")
        
        # Create table
        path = "table"
        create_table_with_data(session, path, True)

        # Backup table
        self.create_backup(path, [path], False)

        logger.info("Verifying database structure after backup")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("table", ".metadata", ".sys")
        )
        logger.info("test_single_table_backup with not null completed successfully")


class TestBaseSingleFromDifPlaces(BaseTestBackupInFiles):
    def test_single_table_backup_from_different_places(self):
        logger.info("Starting test_single_table_backup_from_different_places")
        session = self.driver.table_client.session().create()
        logger.info("YDB session created")
        
        # Create table
        logger.info("Creating directory structure")
        self.driver.scheme_client.make_directory('/Root/folder')
        self.driver.scheme_client.make_directory('/Root/folder/sub_folder')
        logger.info("Directory structure created")
        
        tables_paths = [
            "first",
            "second",
            "folder/third",
            "folder/fourth",
            "folder/sub_folder/fifth",
        ]
        logger.info(f"Creating tables at paths: {tables_paths}")

        for path in tables_paths:
            create_table_with_data(session, path)

        # Backup table
        logger.info("Starting individual table backups")
        for path in tables_paths:
            _, table_name = os.path.split(path)
            logger.info(f"Backing up table from {path} as {table_name}")
            self.create_backup(path, [table_name], True)
        
        logger.info("test_single_table_backup_from_different_places completed successfully")


class TestRecursiveNonConsistent(BaseTestBackupInFiles):
    def test_recursive_table_backup_from_different_places(self):
        logger.info("Starting test_recursive_table_backup_from_different_places (non-consistent)")
        session = self.driver.table_client.session().create()
        logger.info("YDB session created")
        
        # Create table
        logger.info("Creating directory structure")
        self.driver.scheme_client.make_directory('/Root/folder')
        self.driver.scheme_client.make_directory('/Root/folder/sub_folder')
        logger.info("Directory structure created")
        
        tables_paths = [
            "first",
            "second",
            "folder/third",
            "folder/fourth",
            "folder/sub_folder/fifth",
        ]
        logger.info(f"Creating tables at paths: {tables_paths}")

        for path in tables_paths:
            create_table_with_data(session, path)

        # Backup all tables from Root recursively
        logger.info("Backing up all tables from Root recursively")
        self.create_backup("/Root", tables_paths, True, ["--consistency-level", "table"])

        # Backup single table
        logger.info("Backing up individual tables")
        self.create_backup("first", ["first"], True, ["--consistency-level", "table"])
        self.create_backup("folder/third", ["third"], True, ["--consistency-level", "table"])

        # Backup tables from folder recursively
        logger.info("Backing up tables from folder recursively")
        tables_paths = [
            "third",
            "fourth",
            "sub_folder/fifth",
        ]
        self.create_backup("folder", tables_paths, True, ["--consistency-level", "table"])

        # Backup table from sub_folder recursively
        logger.info("Backing up table from sub_folder recursively")
        tables_paths = [
            "fifth",
        ]
        self.create_backup("folder/sub_folder", tables_paths, True, ["--consistency-level", "table"])
        
        logger.info("test_recursive_table_backup_from_different_places (non-consistent) completed successfully")


class TestRecursiveSchemeOnly(BaseTestBackupInFiles):
    def test_recursive_table_backup_from_different_places(self):
        logger.info("Starting test_recursive_table_backup_from_different_places (scheme only)")
        session = self.driver.table_client.session().create()
        logger.info("YDB session created")
        
        # Create table
        logger.info("Creating directory structure")
        self.driver.scheme_client.make_directory('/Root/folder')
        self.driver.scheme_client.make_directory('/Root/folder/sub_folder')
        logger.info("Directory structure created")
        
        tables_paths = [
            "first",
            "second",
            "folder/third",
            "folder/fourth",
            "folder/sub_folder/fifth",
        ]
        logger.info(f"Creating tables at paths: {tables_paths}")

        for path in tables_paths:
            create_table_with_data(session, path)

        # Backup all tables from Root recursively
        logger.info("Backing up all tables from Root recursively (scheme only)")
        self.create_backup("/Root", tables_paths, False, ["--scheme-only"])

        # Backup single table
        logger.info("Backing up individual tables (scheme only)")
        self.create_backup("first", ["first"], False, ["--scheme-only"])
        self.create_backup("folder/third", ["third"], False, ["--scheme-only"])

        # Backup tables from folder recursively
        logger.info("Backing up tables from folder recursively (scheme only)")
        tables_paths = [
            "third",
            "fourth",
            "sub_folder/fifth",
        ]
        self.create_backup("folder", tables_paths, False, ["--scheme-only"])

        # Backup table from sub_folder recursively
        logger.info("Backing up table from sub_folder recursively (scheme only)")
        tables_paths = [
            "fifth",
        ]
        self.create_backup("folder/sub_folder", tables_paths, False, ["--scheme-only"])
        
        logger.info("test_recursive_table_backup_from_different_places (scheme only) completed successfully")


class TestRecursiveConsistent(BaseTestBackupInFiles):
    def test_recursive_table_backup_from_different_places(self):
        logger.info("Starting test_recursive_table_backup_from_different_places (consistent)")
        session = self.driver.table_client.session().create()
        logger.info("YDB session created")
        
        # Create table
        logger.info("Creating directory structure")
        self.driver.scheme_client.make_directory('/Root/folder')
        self.driver.scheme_client.make_directory('/Root/folder/sub_folder')
        logger.info("Directory structure created")
        
        tables_paths = [
            "first",
            "second",
            "folder/third",
            "folder/fourth",
            "folder/sub_folder/fifth",
        ]
        logger.info(f"Creating tables at paths: {tables_paths}")

        for path in tables_paths:
            create_table_with_data(session, path)

        # Backup all tables from Root recursively
        logger.info("Backing up all tables from Root recursively (consistent)")
        self.create_backup("/Root", tables_paths, True, ["--consistency-level", "database"])

        # Backup single table
        logger.info("Backing up individual tables (consistent)")
        self.create_backup("first", ["first"], True, ["--consistency-level", "database"])
        self.create_backup("folder/third", ["third"], True, ["--consistency-level", "database"])

        # Backup tables from folder recursively
        logger.info("Backing up tables from folder recursively (consistent)")
        tables_paths = [
            "third",
            "fourth",
            "sub_folder/fifth",
        ]
        self.create_backup("folder", tables_paths, True, ["--consistency-level", "database"])

        # Backup table from sub_folder recursively
        logger.info("Backing up table from sub_folder recursively (consistent)")
        tables_paths = [
            "fifth",
        ]
        self.create_backup("folder/sub_folder", tables_paths, True, ["--consistency-level", "database"])
        
        logger.info("test_recursive_table_backup_from_different_places (consistent) completed successfully")


class TestSingleBackupRestore(BaseTestBackupInFiles):
    def test_single_table_with_data_backup_restore(self):
        logger.info("Starting test_single_table_with_data_backup_restore")
        self._test_single_table_with_data_backup_restore_impl(False, False)
        self._test_single_table_with_data_backup_restore_impl(False, True)
        self._test_single_table_with_data_backup_restore_impl(True, False)
        self._test_single_table_with_data_backup_restore_impl(True, True)
        logger.info("test_single_table_with_data_backup_restore completed successfully")

    @classmethod
    def _test_single_table_with_data_backup_restore_impl(self, use_bulk_upsert, not_null):
        logger.info(f"Starting backup/restore implementation: bulk_upsert={use_bulk_upsert}, not_null={not_null}")
        
        self.driver.scheme_client.make_directory('/Root/folder')
        logger.info("Created folder directory")
        
        postfix = '_bulk_upsert' if use_bulk_upsert else ''
        postfix += '_not_null' if not_null else ''
        logger.debug(f"Using postfix: {postfix}")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and fill with data
        create_table_with_data(session, "folder/table", not_null)

        # Backup table
        backup_files_dir = output_path(self.test_name, 'test_single_table_with_data_backup_restore' + postfix, "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root/folder",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup directory structure")
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
            logger.info("Using bulk upsert for restore")
        
        logger.info(f"Executing restore command: {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)

        logger.info("Verifying restore results")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", "restored" + postfix, ".metadata", ".sys")
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/restored" + postfix).children],
            is_(["table"])
        )
        
        logger.info("Comparing original and restored tables")
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored" + postfix + "/table"),
            is_(True)
        )
        
        logger.info("Cleaning up restored table")
        session.drop_table("/Root/restored" + postfix + "/table")
        self.driver.scheme_client.remove_directory("/Root/restored" + postfix)
        logger.info(f"Backup/restore implementation completed: bulk_upsert={use_bulk_upsert}, not_null={not_null}")


class TestBackupRestoreInRoot(BaseTestBackupInFiles):
    def test_table_backup_restore_in_root(self):
        logger.info("Starting test_table_backup_restore_in_root")
        
        self.driver.scheme_client.make_directory('/Root/folder')
        logger.info("Created folder directory")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and fill with data
        create_table_with_data(session, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, 'test_single_table_with_data_backup_restore', "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root/folder",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup directory structure")
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
            "--path", "/Root/",
            "--input", backup_files_dir
        ]
        logger.info(f"Executing restore command: {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)
        
        logger.info("Verifying restore results")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", "table", ".metadata", ".sys")
        )
        
        logger.info("Comparing original and restored tables")
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/table"),
            is_(True)
        )
        logger.info("test_table_backup_restore_in_root completed successfully")


class TestBackupRestoreInRootSchemeOnly(BaseTestBackupInFiles):
    def test_table_backup_restore_in_root_scheme_only(self):
        logger.info("Starting test_table_backup_restore_in_root_scheme_only")
        
        self.driver.scheme_client.make_directory('/Root/folder')
        logger.info("Created folder directory")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and fill with data
        create_table_with_data(session, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, 'test_single_table_with_data_backup_restore', "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--scheme-only",
            "--path", "/Root/folder",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command (scheme only): {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup directory structure")
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
            "--path", "/Root/",
            "--input", backup_files_dir
        ]
        logger.info(f"Executing restore command: {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)
        
        logger.info("Verifying restore results")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", "table", ".metadata", ".sys")
        )
        
        logger.info("Comparing original and restored tables (schema only)")
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/table", False),
            is_(True)
        )
        logger.info("test_table_backup_restore_in_root_scheme_only completed successfully")


class TestIncompleteBackup(BaseTestBackupInFiles):
    def test_incomplete_backup_will_not_be_restored(self):
        logger.info("Starting test_incomplete_backup_will_not_be_restored")
        
        self.driver.scheme_client.make_directory('/Root/folder')
        logger.info("Created folder directory")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        create_table_with_data(session, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            'tools', 'dump',
            "--path", '/Root/folder',
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup directory structure")
        assert_that(
            os.listdir(backup_files_dir),
            is_(["table"])
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )

        # Create "incomplete" file in folder with backup files
        logger.info("Creating 'incomplete' marker files to simulate incomplete backup")
        open(os.path.join(backup_files_dir, "incomplete"), "w").close()
        open(os.path.join(backup_files_dir, "table", "incomplete"), "w").close()
        logger.info("Incomplete marker files created")

        # Restore table and check that it fails without restoring anything
        restore_cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            'tools', 'restore',
            "--path", "/Root/restored",
            "--input", backup_files_dir
        ]
        logger.info(f"Executing restore command (expecting failure): {' '.join(restore_cmd)}")
        execution = yatest.common.execute(restore_cmd, check_exit_code=False)
        
        logger.info(f"Restore command exit code: {execution.exit_code}")
        assert_that(
            execution.exit_code,
            is_not(0)
        )
        
        logger.info("Verifying that restore failed and no objects were created")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/folder").children],
            is_(["table"])
        )

        # Test single table restore with incomplete marker
        restore_cmd2 = [
            backup_bin(),
            "--verbose",
            "--endpoint", "localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root"
            'tools', 'restore',
            "--path", "/Root/restored",
            "--input", os.path.join(backup_files_dir, "table")
        ]
        logger.info(f"Executing single table restore command (expecting failure): {' '.join(restore_cmd2)}")
        execution = yatest.common.execute(restore_cmd2, check_exit_code=False)

        logger.info(f"Single table restore command exit code: {execution.exit_code}")
        assert_that(
            execution.exit_code,
            is_not(0)
        )
        
        logger.info("Verifying that single table restore also failed")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", ".metadata", ".sys")
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/folder").children],
            is_(["table"])
        )
        logger.info("test_incomplete_backup_will_not_be_restored completed successfully")


class TestAlterBackupRestore(BaseTestBackupInFiles):
    def test_alter_table_with_data_backup_restore(self):
        logger.info("Starting test_alter_table_with_data_backup_restore")
        
        self.driver.scheme_client.make_directory('/Root/folder')
        logger.info("Created folder directory")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and fill with data
        path = "/Root/folder/table"
        logger.info(f"Creating table with multiple columns at {path}")
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
        logger.info("Table created with 8 columns")
        
        prefix, table = os.path.split(path)
        logger.info("Inserting data into table")
        session.transaction().execute(
            """
            PRAGMA TablePathPrefix("{0}");
            UPSERT INTO {1} (a, b, c, d, e, f, g, h) VALUES (5, "b", 5, "b", 5, "b", 5, "b");
            """.format(prefix, table),
            commit_tx=True,
        )
        logger.info("Data inserted successfully")
        
        logger.info("Altering table to drop column 'b'")
        session.alter_table(
            path,
            [],
            ['b']
        )
        logger.info("Column 'b' dropped from table")

        # Backup table
        backup_files_dir = output_path(self.test_name, 'test_single_table_with_data_backup_restore', "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root/folder",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup directory structure")
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
            "--path", "/Root/restored",
            "--input", backup_files_dir
        ]
        logger.info(f"Executing restore command: {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)
        
        logger.info("Verifying restore results")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("folder", "restored", ".metadata", ".sys")
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/restored").children],
            is_(["table"])
        )
        
        logger.info("Comparing original and restored tables")
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/table"),
            is_(True)
        )
        logger.info("test_alter_table_with_data_backup_restore completed successfully")


class TestPermissionsBackupRestoreSingleTable(BaseTestBackupInFiles):
    def test_single_table(self):
        logger.info("Starting test_single_table (permissions)")
        
        self.driver.scheme_client.make_directory("/Root/folder")
        logger.info("Created folder directory")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and modify permissions on it
        self.create_users()
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, "test_single_table", "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root/folder",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup directory structure")
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
        logger.info(f"Executing restore command: {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)

        logger.info("Verifying restore results")
        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            self.scheme_listdir("/Root/restored"),
            is_(["table"])
        )
        
        logger.info("Comparing original and restored tables (including permissions)")
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/table"),
            is_(True)
        )
        logger.info("test_single_table (permissions) completed successfully")


class TestPermissionsBackupRestoreFolderWithTable(BaseTestBackupInFiles):
    def test_folder_with_table(self):
        logger.info("Starting test_folder_with_table (permissions)")
        
        # Create folder and modify permissions on it
        self.create_users()
        self.driver.scheme_client.make_directory("/Root/folder")
        logger.info("Created folder directory")
        modify_permissions(self.driver.scheme_client, "folder")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and modify permissions on it
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup folder with table
        backup_files_dir = output_path(self.test_name, "test_folder_with_table", "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup completed")
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
        logger.info(f"Executing restore command: {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)

        logger.info("Verifying restore results")
        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        
        logger.info("Comparing folder permissions")
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/", "/Root/restored/folder/"),
            is_(True)
        )
        
        logger.info("Comparing table data and permissions")
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/folder/table"),
            is_(True)
        )
        logger.info("test_folder_with_table (permissions) completed successfully")


class TestPermissionsBackupRestoreDontOverwriteOnAlreadyExisting(BaseTestBackupInFiles):
    def test_dont_overwrite_on_already_existing(self):
        logger.info("Starting test_dont_overwrite_on_already_existing (permissions)")
        
        # Create folder and modify permissions on it
        self.create_users()
        self.driver.scheme_client.make_directory("/Root/folder")
        logger.info("Created folder directory")
        modify_permissions(self.driver.scheme_client, "folder")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and modify permissions on it
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup folder with table
        backup_files_dir = output_path(self.test_name, "test_dont_overwrite_on_already_existing", "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup completed")
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Recreate table and folder but without permissions and data
        logger.info("Recreating table and folder without permissions and data")
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
        logger.info("Table and folder recreated without permissions")
        
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
        logger.info(f"Executing restore command (overwriting existing): {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)
        
        assert_that(
            self.scheme_listdir("/Root"),
            is_(["folder"])
        )

        # Restore folder with table one more time in another folder
        restore_cmd2 = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "restore",
            "--path", "/Root/restored",
            "--input", backup_files_dir
        ]
        logger.info(f"Executing restore command (new location): {' '.join(restore_cmd2)}")
        yatest.common.execute(restore_cmd2)

        logger.info("Verifying restore results")
        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )

        logger.info("Verifying permissions were NOT overwritten (should be False)")
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/", "/Root/restored/folder/"),
            is_(False)
        )
        
        logger.info("Verifying table descriptions match")
        assert_that(
            is_tables_descriptions_the_same(session, "/Root/folder/table", "/Root/restored/folder/table"),
            is_(True)
        )
        
        logger.info("Verifying data matches")
        assert_that(
            is_data_the_same(session, "/Root/folder/table", "/Root/restored/folder/table"),
            is_(True)
        )
        
        logger.info("Verifying table permissions were NOT overwritten (should be False)")
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/table", "/Root/restored/folder/table"),
            is_(False)
        )
        logger.info("test_dont_overwrite_on_already_existing (permissions) completed successfully")


class TestPermissionsBackupRestoreSchemeOnly(BaseTestBackupInFiles):
    def test_scheme_only(self):
        logger.info("Starting test_scheme_only (permissions)")
        
        # Create folder and modify permissions on it
        self.create_users()
        self.driver.scheme_client.make_directory("/Root/folder")
        logger.info("Created folder directory")
        modify_permissions(self.driver.scheme_client, "folder")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and modify permissions on it
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup folder with table
        backup_files_dir = output_path(self.test_name, "test_scheme_only", "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root",
            "--output", backup_files_dir,
            "--scheme-only",
        ]
        logger.info(f"Executing backup command (scheme only): {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup completed")
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
        logger.info(f"Executing restore command: {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)

        logger.info("Verifying restore results")
        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        
        logger.info("Comparing folder permissions")
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/", "/Root/restored/folder/"),
            is_(True)
        )
        
        logger.info("Comparing table schema and permissions (no data)")
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/folder/table", False),
            is_(True)
        )
        logger.info("test_scheme_only (permissions) completed successfully")


class TestPermissionsBackupRestoreEmptyDir(BaseTestBackupInFiles):
    def test_empty_dir(self):
        logger.info("Starting test_empty_dir (permissions)")
        
        # Create empty folder and modify permissions on it
        self.create_users()
        self.driver.scheme_client.make_directory("/Root/folder")
        logger.info("Created empty folder directory")
        modify_permissions(self.driver.scheme_client, "folder")

        # Backup folder
        backup_files_dir = output_path(self.test_name, "test_empty_dir", "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup completed")
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
        logger.info(f"Executing restore command: {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)

        logger.info("Verifying restore results")
        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        
        logger.info("Comparing folder permissions")
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/", "/Root/restored/folder/"),
            is_(True)
        )
        logger.info("test_empty_dir (permissions) completed successfully")


class TestRestoreACLOption(BaseTestBackupInFiles):
    def test_restore_acl_option(self):
        logger.info("Starting test_restore_acl_option")
        
        self.driver.scheme_client.make_directory("/Root/folder")
        logger.info("Created folder directory")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and modify permissions on it
        self.create_users()
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, "test_single_table", "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root/folder",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup directory structure")
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
        logger.info(f"Executing restore command (without ACL): {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)

        logger.info("Verifying restore results")
        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            self.scheme_listdir("/Root/restored"),
            is_(["table"])
        )
        
        logger.info("Verifying table descriptions match")
        assert_that(
            is_tables_descriptions_the_same(session, "/Root/folder/table", "/Root/restored/table"),
            is_(True)
        )
        
        logger.info("Verifying data matches")
        assert_that(
            is_data_the_same(session, "/Root/folder/table", "/Root/restored/table"),
            is_(True)
        )
        
        logger.info("Verifying permissions do NOT match (ACL restore disabled)")
        assert_that(
            is_permissions_the_same(self.driver.scheme_client, "/Root/folder/table", "/Root/restored/table"),
            is_(False)
        )
        logger.info("test_restore_acl_option completed successfully")


class TestRestoreNoData(BaseTestBackupInFiles):
    def test_restore_no_data(self):
        logger.info("Starting test_restore_no_data")
        
        self.driver.scheme_client.make_directory("/Root/folder")
        logger.info("Created folder directory")

        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        # Create table and modify permissions on it
        self.create_users()
        create_table_with_data(session, "folder/table")
        modify_permissions(self.driver.scheme_client, "folder/table")

        # Backup table
        backup_files_dir = output_path(self.test_name, "test_single_table", "backup_files_dir")
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
            "tools", "dump",
            "--path", "/Root/folder",
            "--output", backup_files_dir
        ]
        logger.info(f"Executing backup command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        
        logger.info("Verifying backup directory structure")
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
        logger.info(f"Executing restore command (without data): {' '.join(restore_cmd)}")
        yatest.common.execute(restore_cmd)

        logger.info("Verifying restore results")
        assert_that(
            self.scheme_listdir("/Root"),
            contains_inanyorder("folder", "restored")
        )
        assert_that(
            self.scheme_listdir("/Root/restored"),
            is_(["table"])
        )
        
        logger.info("Verifying table schema and permissions match (no data)")
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/folder/table", "/Root/restored/table", False),
            is_(True)
        )
        
        logger.info("Verifying data does NOT match (data restore disabled)")
        assert_that(
            is_data_the_same(session, "/Root/folder/table", "/Root/restored/table"),
            is_(False)
        )
        logger.info("test_restore_no_data completed successfully")


class BaseTestClusterBackupInFiles(object):
    @classmethod
    def setup_class(cls):
        logger.info(f"Setting up cluster test class: {cls.__name__}")
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
        logger.info("Starting KiKiMR cluster with authentication features")
        cls.cluster.start()
        logger.info("KiKiMR cluster started successfully")

        cls.root_dir = "/Root"
        cls.database = os.path.join(cls.root_dir, "db1")
        logger.info(f"Creating database: {cls.database}")

        cls.cluster.create_database(
            cls.database,
            storage_pool_units_count={
                'hdd': 1
            },
            timeout_seconds=240,
            token=cls.cluster.config.default_clusteradmin
        )
        logger.info("Database created successfully")

        logger.info("Registering and starting database slots")
        cls.database_nodes = cls.cluster.register_and_start_slots(cls.database, count=3)
        cls.cluster.wait_tenant_up(cls.database, cls.cluster.config.default_clusteradmin)
        logger.info("Database nodes started and tenant is up")

        driver_config = ydb.DriverConfig(
            database=cls.database,
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port),
            credentials=ydb.AuthTokenCredentials(cls.cluster.config.default_clusteradmin))
        logger.info(f"Connecting to YDB at {cls.cluster.nodes[1].host}:{cls.cluster.nodes[1].port}")
        
        cls.driver = ydb.Driver(driver_config)
        cls.driver.wait(timeout=4)
        logger.info("YDB driver connected successfully")

    @classmethod
    def teardown_class(cls):
        logger.info(f"Tearing down cluster test class: {cls.__name__}")
        cls.cluster.unregister_and_stop_slots(cls.database_nodes)
        cls.cluster.stop()
        logger.info("Cluster test class teardown complete")

    @pytest.fixture(autouse=True, scope='class')
    @classmethod
    def set_test_name(cls, request):
        cls.test_name = request.node.name
        logger.info(f"Setting cluster test name: {cls.test_name}")

    @classmethod
    def create_backup(cls, command, expected_files, output, additional_args=[], token=""):
        logger.info(f"Creating cluster backup with command: {command}")
        logger.debug(f"Expected files: {expected_files}")
        logger.debug(f"Additional args: {additional_args}")
        
        backup_files_dir = output_path(cls.test_name, output)
        
        cmd = [
            backup_bin(),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
        ] + command + ["--output", backup_files_dir] + additional_args
        
        logger.info(f"Executing cluster backup command: {' '.join(cmd)}")
        execution = yatest.common.execute(cmd, env={"YDB_TOKEN": token})

        list_result = fs_recursive_list(backup_files_dir, ListMode.FILES)

        logger.debug("std_out:\n" + execution.std_out.decode('utf-8'))
        logger.debug("fs_recursive_list(backup_files_dir)# " + str(list_result))
        logger.debug("expected_files# " + str(expected_files))

        logger.info("Verifying backup file count")
        assert_that(
            len(list_result),
            equal_to(len(expected_files))
        )

        logger.info("Verifying backup file contents")
        assert_that(
            list_result,
            has_items(*expected_files)
        )
        logger.info("Cluster backup created successfully")

    @classmethod
    def create_cluster_backup(cls, expected_files, output="backup_files_dir", additional_args=[]):
        logger.info("Creating cluster-level backup")
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
        logger.info("Creating database-level backup")
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
        logger.info("Setting up sample data for cluster tests")
        pool = ydb.SessionPool(cls.driver)
        with pool.checkout() as session:
            logger.info("Creating groups and users")
            session.execute_scheme("CREATE GROUP people")
            session.execute_scheme("CREATE USER alice PASSWORD '1234'")
            session.execute_scheme("CREATE USER bob PASSWORD '1234'")
            session.execute_scheme("ALTER GROUP people ADD USER alice")
            session.execute_scheme("ALTER GROUP people ADD USER bob")
            logger.info("Created 'people' group with alice and bob")

            session.execute_scheme("CREATE GROUP dbadmins")
            session.execute_scheme("CREATE USER dbadmin1")
            session.execute_scheme("CREATE USER dbadmin2 PASSWORD '1234'")
            session.execute_scheme("ALTER GROUP dbadmins ADD USER dbadmin1")
            session.execute_scheme("ALTER GROUP dbadmins ADD USER dbadmin2")
            session.execute_scheme(f'GRANT "ydb.generic.use" on `{cls.database}` TO dbadmins')
            logger.info("Created 'dbadmins' group with dbadmin1 and dbadmin2")

            logger.info("Modifying database permissions")
            cls.driver.scheme_client.modify_permissions(
                cls.database,
                ydb.ModifyPermissionsSettings().change_owner("dbadmins")
            )

        logger.info("Creating sample table")
        create_table_with_data(cls.driver.table_client.session().create(), "db1/table")
        logger.info("Sample data setup complete")


class TestClusterBackup(BaseTestClusterBackupInFiles):
    def test_cluster_backup(self):
        logger.info("Starting test_cluster_backup")
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
        logger.info("test_cluster_backup completed successfully")


class TestDatabaseBackup(BaseTestClusterBackupInFiles):
    def test_database_backup(self):
        logger.info("Starting test_database_backup")
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
        logger.info("test_database_backup completed successfully")


class BaseTestMultipleClusterBackupInFiles(BaseTestClusterBackupInFiles):
    @classmethod
    def setup_class(cls):
        logger.info(f"Setting up multiple cluster test class: {cls.__name__}")
        super().setup_class()

        logger.info("Creating restore cluster configuration")
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

        logger.info("Starting restore cluster")
        cls.restore_cluster.start()
        logger.info("Restore cluster started successfully")

        cls.restore_root_dir = "/Root2"
        cls.restore_database = os.path.join(cls.restore_root_dir, "db1")
        logger.info(f"Registering restore database: {cls.restore_database}")
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
        logger.info(f"Connecting to restore cluster at {cls.restore_cluster.nodes[1].host}:{cls.restore_cluster.nodes[1].port}")
        cls.restore_driver = ydb.Driver(restore_driver_config)
        logger.info("Multiple cluster setup complete")

    @classmethod
    def teardown_class(cls):
        logger.info(f"Tearing down multiple cluster test class: {cls.__name__}")
        cls.restore_cluster.unregister_and_stop_slots(cls.restore_database_nodes)
        cls.restore_cluster.stop()
        super().teardown_class()
        logger.info("Multiple cluster teardown complete")

    @classmethod
    def restore(cls, command, input, additional_args=[], token=""):
        logger.info(f"Executing restore with command: {command}")
        logger.debug(f"Input: {input}")
        logger.debug(f"Additional args: {additional_args}")
        
        backup_files_dir = os.path.join(
            yatest.common.output_path(),
            cls.test_name,
            input
        )

        cmd = [
            backup_bin(),
            "--verbose",
            "--assume-yes",
            "--endpoint", f"grpc://localhost:{cls.restore_cluster.nodes[1].grpc_port}",
        ] + command + ["--input", backup_files_dir, "-w", "240s"] + additional_args
        
        logger.info(f"Executing restore command: {' '.join(cmd)}")
        yatest.common.execute(cmd, env={"YDB_TOKEN": token})
        logger.info("Restore command completed successfully")

    @classmethod
    def restore_cluster_backup(cls, input="backup_files_dir", additional_args=[]):
        logger.info("Restoring cluster backup")
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
        logger.info("Restoring database backup")
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
        logger.info("Starting test_cluster_backup_restore")
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

        logger.info("Waiting for restore tenant to come up")
        self.restore_cluster.wait_tenant_up(self.restore_database, self.cluster.config.default_clusteradmin)
        self.restore_driver.wait(timeout=10)
        logger.info("test_cluster_backup_restore completed successfully")


class TestDatabaseBackupRestore(BaseTestMultipleClusterBackupInFiles):
    def test_database_backup_restore(self):
        logger.info("Starting test_database_backup_restore")
        self.setup_sample_data()

        logger.info("Creating cluster backup")
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

        logger.info("Creating database backup")
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

        logger.info("Restoring cluster backup")
        self.restore_cluster_backup(input="cluster_backup")

        logger.info("Waiting for restore tenant to come up")
        self.restore_cluster.wait_tenant_up(self.restore_database, self.cluster.config.default_clusteradmin)
        self.restore_driver.wait(timeout=10)

        logger.info("Restoring database backup")
        self.restore_database_backup(input="database_backup")
        logger.info("test_database_backup_restore completed successfully")


class TestRestoreReplaceOption(BaseTestBackupInFiles):
    def ydb_cli(self, args):
        logger.debug(f"Executing YDB CLI command: {args}")
        cmd = [
            backup_bin(),
            "-vvv",
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", "/Root",
        ] + args
        logger.info(f"YDB CLI command: {' '.join(cmd)}")
        yatest.common.execute(cmd)
        logger.debug("YDB CLI command completed successfully")

    def try_ydb_cli(self, args):
        logger.debug(f"Attempting YDB CLI command: {args}")
        try:
            result = self.ydb_cli(args)
            logger.info("YDB CLI command succeeded")
            return True, result.std_out.decode("utf-8")

        except yatest.common.process.ExecutionError as exception:
            logger.warning(f"YDB CLI command failed with exit code: {exception.execution_result.exit_code}")
            return False, exception.execution_result.std_err.decode("utf-8")

    def delete_some_rows(self, session, table):
        logger.info(f"Deleting rows from table: {table}")
        session.transaction().execute(
            f"""
            DELETE FROM `{table}` WHERE id == 2;
            """,
            commit_tx=True,
        )
        logger.info("Rows deleted successfully")

    def test_table_replacement(self):
        logger.info("Starting test_table_replacement")
        session = self.driver.table_client.session().create()
        logger.info("YDB session created")

        create_table_with_data(session, "table")

        # Backup the table
        backup_files_dir = output_path(self.test_name, "test_table_replacement", "backup_files_dir")
        logger.info("Creating table backup")
        self.ydb_cli(["tools", "dump", "--path", ".", "--output", backup_files_dir])
        assert_that(os.listdir(backup_files_dir), is_(["table"]))

        # Restore the table
        logger.info("Testing dry-run restore (should fail)")
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
        
        logger.info("Performing actual restore")
        self.ydb_cli(["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir])
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/table", "/Root/restoration/point/table"),
            is_(True),
        )

        # Replace the table
        logger.info("Modifying restored table data")
        self.delete_some_rows(session, "/Root/restoration/point/table")
        
        logger.info("Testing replace with dry-run")
        self.ydb_cli(
            ["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir, "--replace", "--dry-run"]
        )
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/table", "/Root/restoration/point/table"),
            is_(False),
        )
        
        logger.info("Performing actual replace")
        self.ydb_cli(["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir, "--replace"])
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/table", "/Root/restoration/point/table"),
            is_(True),
        )

        # Drop the folder
        logger.info("Dropping restoration folder")
        self.ydb_cli(["scheme", "rmdir", "--force", "--recursive", "./restoration/point"])

        # Replace a non-existent table with the --verify-existence option turned on
        logger.info("Testing replace with verify-existence on non-existent table (should fail)")
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

        logger.info("Verifying that failed replace didn't create objects")
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root").children],
            contains_inanyorder("table", "restoration", ".metadata", ".sys"),
        )
        assert_that(
            [child.name for child in self.driver.scheme_client.list_directory("/Root/restoration").children],
            empty()
        )

        # Replace a non-existent table
        logger.info("Testing replace on non-existent table without verify-existence (should fail)")
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
        
        logger.info("Performing replace on non-existent table")
        self.ydb_cli(["tools", "restore", "--path", "./restoration/point", "--input", backup_files_dir, "--replace"])
        assert_that(
            is_tables_the_same(session, self.driver.scheme_client, "/Root/table", "/Root/restoration/point/table"),
            is_(True),
        )
        logger.info("test_table_replacement completed successfully")
