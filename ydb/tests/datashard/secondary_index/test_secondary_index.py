import pytest
import ydb

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, format_sql_value, pk_types, index_first, index_second, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync


class TestSecondaryIndex(TestBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            ("table_index_4_UNIQUE_SYNC", pk_types, {},
             index_four_sync, "", "UNIQUE", "SYNC"),
            ("table_index_3_UNIQUE_SYNC", pk_types, {},
             index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            ("table_index_2_UNIQUE_SYNC", pk_types, {},
             index_second_sync, "", "UNIQUE", "SYNC"),
            ("table_index_1_UNIQUE_SYNC", pk_types, {},
             index_first_sync, "", "UNIQUE", "SYNC"),
            ("table_index_0_UNIQUE_SYNC", pk_types, {},
             index_zero_sync, "", "UNIQUE", "SYNC"),
            ("table_index_4__SYNC", pk_types, {},
             index_four_sync, "", "", "SYNC"),
            ("table_index_3__SYNC", pk_types, {},
             index_three_sync, "", "", "SYNC"),
            ("table_index_2__SYNC", pk_types, {},
             index_second_sync, "", "", "SYNC"),
            ("table_index_1__SYNC", pk_types, {},
             index_first_sync, "", "", "SYNC"),
            ("table_index_0__SYNC", pk_types, {},
             index_zero_sync, "", "", "SYNC"),
            ("table_index_1__ASYNC", pk_types, {}, index_second, "", "", "ASYNC"),
            ("table_index_0__ASYNC", pk_types, {}, index_first, "", "", "ASYNC"),
        ]
    )
    def test_secondary_index(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        dml = DMLOperations(self)
        dml.create_table(table_name, pk_types, all_types,
                         index, ttl, unique, sync)
        dml.insert(table_name, all_types, pk_types, index, ttl)
        self.select_index(table_name, index, dml)

    def select_index(self, table_name: str, index: dict[str, str], dml: DMLOperations):
        rows = dml.query(f"select count(*) as count from {table_name}")
        count_col = rows[0].count
        for type_name in index.keys():
            for i in range(1, count_col+1):
                def process(session):
                    rows = self.query(
                        f"""
                            select count(*) as count
                            from {table_name} VIEW idx_col_index_{cleanup_type_name(type_name)}
                            where col_index_{cleanup_type_name(type_name)} > {format_sql_value(pk_types[type_name](i), type_name)};
                            """,
                        tx=session.transaction(
                            tx_mode=ydb.QueryStaleReadOnly())
                    )
                    if type_name != "Bool" and type_name != "String" and type_name != "Utf8":
                        assert len(rows) == 1 and rows[0].count == count_col - \
                            i, f"there were not enough columns by type {type_name}"
                    elif type_name != "Bool":
                        if i < 10:
                            if i*10 > count_col:
                                count = count_col - (9-i)
                            else:
                                count = (i-1)*10+i
                        else:
                            count = i - (9 - i//10)
                        assert len(rows) == 1 and rows[0].count == count_col - \
                            count, f"there were not enough columns by type {type_name}, numb {i}"
                    else:
                        assert len(
                            rows) == 1 and rows[0].count == 0, f"there were not enough columns by type {type_name}"
                self.transactional(process)
