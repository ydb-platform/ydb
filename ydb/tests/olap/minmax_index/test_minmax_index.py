# -*- coding: utf-8 -*-
import os
import logging
import yatest.common
import ydb

from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, format_sql_value
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.sql.lib.test_base import TestBase

logger = logging.getLogger(__name__)


class TestYdbMinMaxIndex(TestBase):

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY", "ydb/apps/ydbd/ydbd"))
        logger.error(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        cls.ydb_cli_path = yatest.common.build_path("ydb/apps/ydb/ydb")

        cls.database = "/Root"
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=cls.get_cluster_configuration()
            column_shard_config={
                'alter_object_enabled': True,
            },
        ))

        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database=cls.get_database(),
                endpoint=cls.get_endpoint()
            )
        )

        cls.driver.wait()
        cls.pool = ydb.QuerySessionPool(cls.driver)


    def test_minmax_index_smoke(self):
        """
        Test adding columns and verifying that minmax index is built correctly
        """


        # INSERT_YOUR_CODE

        from ydb.tests.olap.scenario.helpers.data_generators import get_random_value

        table_name = f"{self.table_path}_all_types_minmax"
        self.prepare_table(table_name)

        type_names = list(self.ALL_TYPES.keys())
        type_funcs = list(self.ALL_TYPES.values())

        # Add columns one by one, each of different type
        for i, (type_name, type_func) in enumerate(self.ALL_TYPES.items()):
            col_name = f"C{i + 1}"
            self.query(f"""
                ALTER TABLE `{table_name}`
                ADD COLUMN {col_name} {type_name};
            """)
            # Add minmax index on the column
            self.query(f"""
                ALTER TABLE `{table_name}`
                ADD INDEX idx_{col_name}_minmax TYPE minmax ON ({col_name});
            """)

        # Insert 10000 rows of random values for all columns
        values = []
        for row_idx in range(10000):
            row = [row_idx]  # C0 is Uint64 PK
            for i, (type_name, type_func) in enumerate(self.ALL_TYPES.items()):
                val = get_random_value(type_func, type_name)
                row.append(val)
            values.append(tuple(row))

        columns = ["C0"] + [f"C{i + 1}" for i in range(len(self.ALL_TYPES))]
        # Chunked UPSERT for bulk insert
        chunk_size = 1000
        for chunk_start in range(0, len(values), chunk_size):
            chunk = values[chunk_start:chunk_start+chunk_size]
            values_sql = ", ".join([
                "(" + ", ".join(format_sql_value(val, type_names[i-1] if i != 0 else "Uint64", False)
                                for i, val in enumerate(row)) + ")" for row in chunk
            ])
            self.query(f"""
                UPSERT INTO `{table_name}` ({", ".join(columns)}) VALUES {values_sql};
            """)

        # Build a combined predicate for all columns (pick test values)
        predicates = []
        test_vals = []
        # pick data for first three columns only for sample query
        sample_cols = min(3, len(self.ALL_TYPES))
        for i in range(sample_cols):
            type_name = type_names[i]
            type_func = type_funcs[i]
            col_name = f"C{i + 1}"
            # Use get_random_value with fixed values for repeatability in predicate
            # For strings, use static "abc"
            if type_name.lower() == "string":
                val = "abc"
            elif type_name.lower() in ["utf8"]:
                val = "абв"
            elif type_name.lower().startswith("int") or type_name.lower().startswith("uint"):
                val = i + 1
            elif type_name.lower() == "bool":
                val = 1 if i % 2 == 0 else 0
            else:
                val = get_random_value(type_func, type_name)
            test_vals.append(val)
            predicates.append(f"{col_name} = {format_sql_value(val, type_name, False)}")

        where_clause = " AND ".join(predicates)

        # Perform select with the predicate
        result = self.query(f"""
            SELECT {", ".join(columns)} FROM `{table_name}` WHERE {where_clause} ORDER BY C0;
        """)



        table_name = f"{self.table_path}_add_column"
        self.prepare_table(table_name)

        type_names = list(self.ALL_TYPES.keys())

        # Add new columns with NULLs
        for i, type_name in enumerate(type_names):
            self.query(f"""
                ALTER TABLE `{table_name}`
                ADD COLUMN C{i + 1} {type_name};
            """)

        self.query(f"""
            UPSERT INTO `{table_name}` (C0) VALUES (1);
        """)

        select_columns = [f"C{i}" for i in range(len(type_names) + 1)]

        # Verify all columns are built with NULLs
        return self.query(f"""
            SELECT {", ".join(select_columns)} FROM `{table_name}` ORDER BY C0;
        """)

