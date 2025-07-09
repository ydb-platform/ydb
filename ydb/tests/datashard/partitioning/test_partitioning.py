import pytest

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.create_table import create_table_sql_request
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types


class TestPartitionong(TestBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index",
        [
            (
                "table_ttl_Date",
                {
                    "Uint64": lambda i: i,
                },
                {**pk_types, **non_pk_types},
                {}
            ),
            (
                "table_ttl_Date",
                {
                    "Uint32": lambda i: i,
                },
                {**pk_types, **non_pk_types},
                {}
            ),
        ],
    )
    def test_uniform_partitiona(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
    ):
        dml = DMLOperations(self)
        count_partition = 10
        set_parametr = f"""WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {count_partition},
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {count_partition},
                UNIFORM_PARTITIONS = {count_partition});"""
        self.create_table(table_name, all_types, pk_types, set_parametr, dml)
        dml.insert(table_name, all_types, pk_types, index, "")
        is_split = wait_for(self.expected_partitions(table_name, 10, dml), timeout_seconds=150)
        assert is_split is True, f"The table {table_name} is not split into partition"
        dml.select_after_insert(table_name, all_types, pk_types, index, "")

    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index",
        [
            ("table_Int64", {"Int64": lambda i: i}, {**pk_types, **non_pk_types}, {}),
            ("table_Int32", {"Int32": lambda i: i}, {**pk_types, **non_pk_types}, {}),
            # ("table_Int16", {"Int16": lambda i: i},
            # {**pk_types, **non_pk_types}, {}), https://github.com/ydb-platform/ydb/issues/15842
            ("table_Int8", {"Int8": lambda i: i}, {**pk_types, **non_pk_types}, {}),
            ("table_Uint64", {"Uint64": lambda i: i}, {**pk_types, **non_pk_types}, {}),
            ("table_Uint32", {"Uint32": lambda i: i}, {**pk_types, **non_pk_types}, {}),
            # ("table_Uint16", {"Uint16": lambda i: i},
            # {**pk_types, **non_pk_types}, {}), https://github.com/ydb-platform/ydb/issues/15842
            ("table_Uint8", {"Uint8": lambda i: i}, {**pk_types, **non_pk_types}, {}),
            ("table_String", {"String": lambda i: f"{i}"}, {**pk_types, **non_pk_types}, {}),
            ("table_Utf8", {"Utf8": lambda i: f"{i}"}, {**pk_types, **non_pk_types}, {}),
        ],
    )
    def test_partition_at_keys(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
    ):
        dml = DMLOperations(self)
        self.create_table(table_name, all_types, pk_types, self.create_partition_at_keys_sql_request(pk_types), dml)
        dml.insert(table_name, all_types, pk_types, index, "")
        is_split = wait_for(self.expected_partitions(table_name, 4, dml), timeout_seconds=150)
        assert is_split is True, f"The table {table_name} is not split into partition"
        dml.select_after_insert(table_name, all_types, pk_types, index, "")

    def create_partition_at_keys_sql_request(self, pk_types):
        statements = []
        for type_name in pk_types.keys():
            for i in range(1, 4):
                if "Uint" in type_name or "Int" in type_name:
                    statements.append(str(i * 3))
                else:
                    statements.append(f"\"{str(pk_types[type_name](i*3))}\"")
        return f"""
                WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                PARTITION_AT_KEYS = ({", ".join(statements)})
                );
                """

    def create_table(self, table_name, all_types, pk_types, set_parametr, dml: DMLOperations):
        columns = {
            "pk_": pk_types.keys(),
            "col_": all_types.keys(),
        }
        pk_columns = {"pk_": pk_types.keys()}
        sql_create_table = create_table_sql_request(table_name, columns, pk_columns, {}, "", "")
        sql_create_table += set_parametr
        dml.query(sql_create_table)

    def expected_partitions(self, table_name, expectation, dml: DMLOperations):
        def predicate():
            rows = dml.query(
                f"""SELECT
                count(*) as count
                FROM `.sys/partition_stats`
                WHERE Path = "{self.get_database()}/{table_name}"
                """
            )
            return rows[0].count == expectation

        return predicate
