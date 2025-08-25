# -*- coding: utf-8 -*-
from ydb.tests.datashard.lib.add_column_base import AddColumnBase
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, format_sql_value


class TestYdbAddColumnWorkload(AddColumnBase):
    # Python SDK returns
    # - decimal.Decimal objects as Decimal(15,0), Decimal(22,9), Decimal(35,10)
    # - uuid.UUID objects as UUID
    # - datetime.timedelta objects as Interval, Interval64
    # which are not serializable by yatest_lib, so need to cast them to String for assertions.
    # Also, need to cast Float and Double to String for assertions to avoid floating point precision issues.
    CAST_TO_STRING_TYPES = {"Decimal(15,0)", "Decimal(22,9)", "Decimal(35,10)", "UUID", "Interval", "Interval64", "Float", "Double"}
    ALL_TYPES = pk_types | non_pk_types

    def prepare_table(self, table_name: str):
        self.query(f"""
            CREATE TABLE `{table_name}` (
                C0 Uint64,
                PRIMARY KEY (C0)
            )
        """)

        self.query(f"""
            UPSERT INTO `{table_name}` (C0) VALUES (0);
        """)

    def try_cast_column(self, idx: int, type_name: str):
        if type_name in self.CAST_TO_STRING_TYPES:
            return f"CAST(C{idx} AS String) AS C{idx}"
        return f"C{idx}"

    def test_add_column(self):
        """
        Test adding columns without default values and verify all columns are present as NULLs
        """

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

    def test_add_column_default(self):
        """
        Test adding columns with default values and verify all columns are present as default values
        """

        table_name = f"{self.table_path}_add_column_default"
        self.prepare_table(table_name)

        # Add new columns with default values
        for i, (type_name, type_func) in enumerate(self.ALL_TYPES.items()):
            self.query(f"""
                ALTER TABLE `{table_name}`
                ADD COLUMN C{i + 1} {type_name} DEFAULT {format_sql_value(type_func(i + 1), type_name, False)};
            """)

        self.query(f"""
            UPSERT INTO `{table_name}` (C0) VALUES (1);
        """)

        select_columns = ["C0"] + [self.try_cast_column(i + 1, type_name) for i, type_name in enumerate(self.ALL_TYPES.keys())]

        # Verify all columns are built with default values
        return self.query(f"""
            SELECT {", ".join(select_columns)} FROM `{table_name}` ORDER BY C0;
        """)

    def test_add_column_default_not_null(self):
        """
        Test adding not null columns with default values and verify all columns are present as default values
        """

        table_name = f"{self.table_path}_add_column_default_not_null"
        self.prepare_table(table_name)

        # Add new columns with default values
        for i, (type_name, type_func) in enumerate(self.ALL_TYPES.items()):
            self.query(f"""
                ALTER TABLE `{table_name}`
                ADD COLUMN C{i + 1} {type_name} NOT NULL DEFAULT {format_sql_value(type_func(i + 1), type_name, False)};
            """)

        self.query(f"""
            UPSERT INTO `{table_name}` (C0) VALUES (1);
        """)

        select_columns = ["C0"] + [self.try_cast_column(i + 1, type_name) for i, type_name in enumerate(self.ALL_TYPES.keys())]

        # Verify all columns are built with default values
        return self.query(f"""
            SELECT {", ".join(select_columns)} FROM `{table_name}` ORDER BY C0;
        """)
