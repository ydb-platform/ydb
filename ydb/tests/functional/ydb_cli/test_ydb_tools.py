# -*- coding: utf-8 -*-

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.canonical import set_canondata_root
from ydb.tests.oss.ydb_sdk_import import ydb

import os
import logging
import pytest

import yatest

logger = logging.getLogger(__name__)


def ydb_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
    raise RuntimeError("YDB_CLI_BINARY environment variable is not specified")


class BaseTestToolsService(object):
    @classmethod
    def setup_class(cls):
        set_canondata_root('ydb/tests/functional/ydb_cli/canondata')

        cls.cluster = KiKiMR()
        cls.cluster.start()
        cls.root_dir = "/Root"
        driver_config = ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port))
        cls.driver = ydb.Driver(driver_config)
        cls.driver.wait(timeout=4)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'driver'):
            cls.driver.stop()
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    @classmethod
    def execute_ydb_cli_command(cls, args, stdin=None):
        execution = yatest.common.execute(
            [
                ydb_bin(),
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
                "--database", cls.root_dir
            ] +
            args, stdin=stdin
        )

        result = execution.std_out
        logger.debug("std_out:\n" + result.decode('utf-8'))
        return result


class TestToolsCopy(BaseTestToolsService):

    @classmethod
    def setup_class(cls):
        BaseTestToolsService.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'session'):
            cls.session.closing()
        BaseTestToolsService.teardown_class()

    def create_table_with_indexes_and_data(self, session, path):
        """Create a table with indexes and populate it with test data."""
        session.create_table(
            path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
                ydb.Column('id', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.String))
            )
            .with_primary_keys('key')
            .with_indexes(
                ydb.TableIndex('by_id').with_index_columns('id'),
                ydb.TableIndex('by_value').with_index_columns('value')
            )
        )

        # Insert test data
        table_name = os.path.basename(path)
        table_dir = os.path.dirname(path)
        session.transaction().execute(
            """
            PRAGMA TablePathPrefix("{0}");
            UPSERT INTO {1} (`key`, `id`, `value`) VALUES (1, 1111, "one");
            UPSERT INTO {1} (`key`, `id`, `value`) VALUES (2, 2222, "two");
            UPSERT INTO {1} (`key`, `id`, `value`) VALUES (3, 3333, "three");
            UPSERT INTO {1} (`key`, `id`, `value`) VALUES (5, 5555, "five");
            UPSERT INTO {1} (`key`, `id`, `value`) VALUES (7, 7777, "seven");
            """.format(table_dir, table_name),
            commit_tx=True,
        )

    def verify_table_data(self, session, path):
        """Verify that table data matches expected values."""
        table_name = os.path.basename(path)
        table_dir = os.path.dirname(path)
        result = session.transaction().execute(
            """
            PRAGMA TablePathPrefix("{0}");
            SELECT `key`, `id`, `value` FROM {1} ORDER BY `key`;
            """.format(table_dir, table_name),
            commit_tx=True,
        )
        
        rows = []
        for row in result[0].rows:
            rows.append((row.key, row.id, row.value))
        
        expected_rows = [
            (1, 1111, "one"),
            (2, 2222, "two"),
            (3, 3333, "three"),
            (5, 5555, "five"),
            (7, 7777, "seven")
        ]
        
        assert rows == expected_rows, f"Data mismatch: expected {expected_rows}, got {rows}"

    def test_copy_table_with_indexes(self):
        """Test copying a table with indexes (default behavior)."""
        source_path = os.path.join(self.root_dir, "test_copy_with_indexes_source")
        dest_path = os.path.join(self.root_dir, "test_copy_with_indexes_dest")

        # Create source table with indexes and data
        self.create_table_with_indexes_and_data(self.session, source_path)

        # Copy table using CLI
        self.execute_ydb_cli_command([
            "tools", "copy",
            "--item", "source=test_copy_with_indexes_source,destination=test_copy_with_indexes_dest"
        ])

        # Verify destination table has data
        self.verify_table_data(self.session, dest_path)

        # Verify destination table has indexes
        desc = self.session.describe_table(dest_path)
        assert len(desc.indexes) == 2, f"Expected 2 indexes, got {len(desc.indexes)}"
        
        index_names = sorted([idx.name for idx in desc.indexes])
        assert index_names == ['by_id', 'by_value'], f"Index names mismatch: {index_names}"

    def test_copy_table_omit_indexes(self):
        """Test copying a table with omit-indexes=true."""
        source_path = os.path.join(self.root_dir, "test_copy_omit_indexes_source")
        dest_path = os.path.join(self.root_dir, "test_copy_omit_indexes_dest")

        # Create source table with indexes and data
        self.create_table_with_indexes_and_data(self.session, source_path)

        # Copy table using CLI with omit-indexes=true
        self.execute_ydb_cli_command([
            "tools", "copy",
            "--item", "source=test_copy_omit_indexes_source,destination=test_copy_omit_indexes_dest,omit-indexes=true"
        ])

        # Verify destination table has data
        self.verify_table_data(self.session, dest_path)

        # Verify destination table has NO indexes
        desc = self.session.describe_table(dest_path)
        assert len(desc.indexes) == 0, f"Expected 0 indexes, got {len(desc.indexes)}"

    def test_copy_multiple_tables_mixed_indexes(self):
        """Test copying multiple tables, some with and some without indexes."""
        source1_path = os.path.join(self.root_dir, "test_multi_copy_with_idx_src")
        dest1_path = os.path.join(self.root_dir, "test_multi_copy_with_idx_dst")
        source2_path = os.path.join(self.root_dir, "test_multi_copy_omit_idx_src")
        dest2_path = os.path.join(self.root_dir, "test_multi_copy_omit_idx_dst")

        # Create source tables with indexes and data
        self.create_table_with_indexes_and_data(self.session, source1_path)
        self.create_table_with_indexes_and_data(self.session, source2_path)

        # Copy both tables in one command: first with indexes, second without
        self.execute_ydb_cli_command([
            "tools", "copy",
            "--item", "source=test_multi_copy_with_idx_src,destination=test_multi_copy_with_idx_dst",
            "--item", "source=test_multi_copy_omit_idx_src,destination=test_multi_copy_omit_idx_dst,omit-indexes=true"
        ])

        # Verify first destination table has data and indexes
        self.verify_table_data(self.session, dest1_path)
        desc1 = self.session.describe_table(dest1_path)
        assert len(desc1.indexes) == 2, f"Expected 2 indexes in first table, got {len(desc1.indexes)}"

        # Verify second destination table has data but NO indexes
        self.verify_table_data(self.session, dest2_path)
        desc2 = self.session.describe_table(dest2_path)
        assert len(desc2.indexes) == 0, f"Expected 0 indexes in second table, got {len(desc2.indexes)}"
