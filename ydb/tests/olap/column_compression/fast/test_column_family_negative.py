import logging
import os
import pytest
import random
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestColumnFamilyNegative(object):
    """Negative test cases for column families (column groups) in column tables.

    Column families are not supported for column (OLAP) tables. These tests verify
    that all DDL paths that attempt to use column families on column tables are
    properly rejected with clear error messages.
    """

    test_name = "column_family_negative"

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags=[
                "enable_olap_compression"
            ]
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

        cls.test_dir = f"{cls.ydb_client.database}/{cls.test_name}"

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    def get_table_path(self, suffix=""):
        return f"{self.test_dir}/table_{suffix}_{random.randrange(99999)}"

    def create_column_table(self, table_path):
        """Helper to create a basic column table for ALTER tests."""
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                val Int64,
                val_str Utf8,
                PRIMARY KEY(id),
            )
            WITH (STORE = COLUMN);
            """
        )

    # =========================================================================
    # CREATE TABLE negative scenarios
    # =========================================================================

    def test_create_with_multiple_families(self):
        """Multiple column families defined in a column table."""
        table_path = self.get_table_path("multiple_families")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val1 Int64 FAMILY fam1,
                    val2 Utf8 FAMILY fam2,
                    PRIMARY KEY(id),
                    FAMILY fam1 (
                        COMPRESSION = "lz4"
                    ),
                    FAMILY fam2 (
                        COMPRESSION = "zstd"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_family_data_setting(self):
        """Column family with DATA storage setting in a column table."""
        table_path = self.get_table_path("family_data")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY fam1,
                    PRIMARY KEY(id),
                    FAMILY fam1 (
                        DATA = "ssd"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_family_compression_off(self):
        """Column family with COMPRESSION = 'off' in a column table."""
        table_path = self.get_table_path("family_compression_off")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY fam1,
                    PRIMARY KEY(id),
                    FAMILY fam1 (
                        COMPRESSION = "off"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_default_family_override(self):
        """Overriding the default family settings in a column table."""
        table_path = self.get_table_path("default_family_override")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64,
                    PRIMARY KEY(id),
                    FAMILY default (
                        DATA = "ssd",
                        COMPRESSION = "lz4"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_family_cache_mode(self):
        """Column family with CACHE_MODE setting in a column table."""
        table_path = self.get_table_path("family_cache_mode")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64,
                    PRIMARY KEY(id),
                    FAMILY fam1 (
                        CACHE_MODE = "regular"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_pk_column_in_non_default_family(self):
        """Primary key column assigned to a non-default family in a column table."""
        table_path = self.get_table_path("pk_in_family")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL FAMILY fam1,
                    val Int64,
                    PRIMARY KEY(id),
                    FAMILY fam1 (
                        COMPRESSION = "lz4"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_column_in_default_family_explicit(self):
        """Column explicitly assigned to FAMILY default in a column table."""
        table_path = self.get_table_path("col_in_default_family")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY default,
                    PRIMARY KEY(id),
                    FAMILY default (
                        COMPRESSION = "lz4"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_orphan_family(self):
        """Non-default FAMILY defined but no column assigned to it in a column table."""
        table_path = self.get_table_path("orphan_family")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64,
                    PRIMARY KEY(id),
                    FAMILY fam1 (
                        COMPRESSION = "lz4"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    # =========================================================================
    # ALTER TABLE negative scenarios
    # =========================================================================

    def test_alter_add_family(self):
        """ALTER TABLE ADD FAMILY on a column table."""
        table_path = self.get_table_path("alter_add_family")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ADD FAMILY fam1 (
                        COMPRESSION = "lz4"
                    );
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_add_family_with_data(self):
        """ALTER TABLE ADD FAMILY with DATA setting on a column table."""
        table_path = self.get_table_path("alter_add_family_data")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ADD FAMILY fam1 (
                        DATA = "ssd",
                        COMPRESSION = "lz4"
                    );
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_family_set_compression(self):
        """ALTER TABLE ALTER FAMILY ... SET COMPRESSION on a column table (non-default family)."""
        table_path = self.get_table_path("alter_family_compression")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ALTER FAMILY nonexistent SET COMPRESSION "zstd";
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_family_default_set_compression(self):
        """ALTER TABLE ALTER FAMILY default SET COMPRESSION on a column table."""
        table_path = self.get_table_path("alter_default_family_compr")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ALTER FAMILY default SET COMPRESSION "lz4";
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_family_set_data(self):
        """ALTER TABLE ALTER FAMILY ... SET DATA on a column table."""
        table_path = self.get_table_path("alter_family_data")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ALTER FAMILY default SET DATA "ssd";
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_column_set_family(self):
        """ALTER TABLE ALTER COLUMN ... SET FAMILY on a column table."""
        table_path = self.get_table_path("alter_column_set_family")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ALTER COLUMN val SET FAMILY fam1;
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_add_column_with_family(self):
        """ALTER TABLE ADD COLUMN with FAMILY on a column table."""
        table_path = self.get_table_path("alter_add_col_family")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ADD COLUMN new_col Utf8 FAMILY fam1;
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_add_family_and_column_together(self):
        """ALTER TABLE ADD FAMILY + ADD COLUMN with that family in one statement on a column table."""
        table_path = self.get_table_path("alter_add_both")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ADD FAMILY fam1 (
                        COMPRESSION = "lz4"
                    ),
                    ADD COLUMN new_col Utf8 FAMILY fam1;
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_family_set_cache_mode(self):
        """ALTER TABLE ALTER FAMILY ... SET CACHE_MODE on a column table."""
        table_path = self.get_table_path("alter_family_cache")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ALTER FAMILY default SET CACHE_MODE "in_memory";
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    # =========================================================================
    # TABLESTORE negative scenarios
    # =========================================================================

    def test_create_tablestore_with_family(self):
        """CREATE TABLESTORE with column families."""
        table_path = self.get_table_path("tablestore_family")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLESTORE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY fam1,
                    PRIMARY KEY(id),
                    FAMILY default (
                        COMPRESSION = "off"
                    ),
                    FAMILY fam1 (
                        COMPRESSION = "lz4"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_tablestore_with_multiple_families(self):
        """CREATE TABLESTORE with multiple column families and different settings."""
        table_path = self.get_table_path("tablestore_multi_family")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLESTORE `{table_path}` (
                    id Uint64 NOT NULL,
                    val1 Int64 FAMILY fam1,
                    val2 Utf8 FAMILY fam2,
                    PRIMARY KEY(id),
                    FAMILY fam1 (
                        COMPRESSION = "lz4"
                    ),
                    FAMILY fam2 (
                        COMPRESSION = "zstd"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    # =========================================================================
    # Combined COMPRESSION + FAMILY negative scenarios
    # =========================================================================

    def test_create_with_both_compression_and_family(self):
        """Column has both COMPRESSION() and FAMILY in a column table — both features conflict."""
        table_path = self.get_table_path("compression_and_family")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY fam1 COMPRESSION(algorithm=lz4),
                    PRIMARY KEY(id),
                    FAMILY fam1 (
                        COMPRESSION = "zstd"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_alter_add_column_with_family_and_compression(self):
        """ALTER TABLE ADD COLUMN with both FAMILY and COMPRESSION on a column table."""
        table_path = self.get_table_path("alter_add_col_family_compr")
        self.create_column_table(table_path)

        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                    ADD COLUMN new_col Utf8 FAMILY fam1 COMPRESSION(algorithm=lz4);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_empty_family_name(self):
        """Column family with empty name in a column table."""
        table_path = self.get_table_path("empty_family_name")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64,
                    PRIMARY KEY(id),
                    FAMILY `` (),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_with_default_and_custom_family_compression(self):
        """CREATE TABLE with default and custom families with COMPRESSION settings.

        Moved from test_obsolete.py::test_family.
        """
        table_path = self.get_table_path("default_and_custom_family")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY fam1,
                    PRIMARY KEY(id),
                    Family default (
                        COMPRESSION = "off"
                    ),
                    Family fam1 (
                        COMPRESSION = "lz4"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message

    def test_create_tablestore_with_family_compression_settings(self):
        """CREATE TABLESTORE with default and custom families with COMPRESSION settings.

        Moved from test_incorrect.py::test_tablestore.
        """
        table_path = self.get_table_path("tablestore_family_compr")
        with pytest.raises(ydb.issues.Error) as ex:
            self.ydb_client.query(
                f"""
                CREATE TABLESTORE `{table_path}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY fam1,
                    PRIMARY KEY(id),
                    Family default (
                        COMPRESSION = "lz4"
                    ),
                    Family fam1 (
                        COMPRESSION = "zstd"
                    ),
                )
                WITH (STORE = COLUMN);
                """
            )
        assert "Column FAMILY is not supported for column tables" in ex.value.message
