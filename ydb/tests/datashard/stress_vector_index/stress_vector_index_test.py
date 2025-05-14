import pytest

from ydb.tests.datashard.lib.vector_base import VectorBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.create_table import create_vector_index_sql_request
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    format_sql_value,
    pk_types,
    non_pk_types,
)


class TestStressVectorIndex(VectorBase):
    def test_stress_vector_index(self):
        dml = DMLOperations(self)
        all_types = {**pk_types, **non_pk_types}
        dml.create_table("table_vector_index", pk_types, all_types, {}, "", "", "")
        print("aaaaaa")
        
    def test_stress_vector_index_with_covered_columns(self):
        dml = DMLOperations(self)
        all_types = {**pk_types, **non_pk_types}
        dml.create_table("table_vector_index_with_covered_columns", pk_types, all_types, {}, "", "", "")
        print("aaaaaa")
        
    def test_stress_prefixed_vector_index(self):
        dml = DMLOperations(self)
        all_types = {**pk_types, **non_pk_types}
        dml.create_table("table_prefixed_vector_index", pk_types, all_types, {}, "", "", "")
        print("aaaaaa")
        
    def test_stress_prefixed_vector_index_with_covered_columns(self):
        dml = DMLOperations(self)
        all_types = {**pk_types, **non_pk_types}
        dml.create_table("table_prefixed_vector_index_with_covered_columns", pk_types, all_types, {}, "", "", "")
        print("aaaaaa")
        