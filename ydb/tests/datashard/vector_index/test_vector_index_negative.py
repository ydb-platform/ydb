import pytest

from ydb.tests.datashard.lib.vector_base import VectorBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.create_table import create_vector_index_sql_request
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    format_sql_value,
)


class TestVectorIndexNegative(VectorBase):
    def test_t(self):
        dml = DMLOperations(self)
        all_types = {"String": lambda i: f"String {i}"}
        index = {"String": lambda i: f"String {i}", "Int64": lambda i: i}
        pk_types = {"Int64": lambda i: i}
        table_name = "table"
        dml.create_table(
            table_name=table_name, pk_types=pk_types, all_types=all_types, index=index, ttl="", unique="", sync=""
        )
