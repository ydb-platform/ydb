# -*- coding: utf-8 -*-
import pytest
import ydb

from ydb.tests.datashard.lib.types_of_variables import (
    pk_types,
    non_pk_types,
    type_to_literal_lambda,
    cleanup_type_name,
)
from ydb.tests.sql.lib.test_base import TestBase


class TestSetNotNull(TestBase):
    ALL_TYPES = {**pk_types, **non_pk_types}

    @classmethod
    def get_extra_feature_flags(cls):
        return super().get_extra_feature_flags() + ["enable_set_column_constraint"]

    def _create_table(self, table_name: str, value_type: str = "Int64"):
        self.query(f"""
            CREATE TABLE `{table_name}` (
                id Uint64 NOT NULL,
                value {value_type},
                PRIMARY KEY (id)
            )
        """)

    def _count_nulls(self, table_name: str) -> int:
        result = self.query(f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE value IS NULL")
        return result[0]["cnt"]

    def _row_count(self, table_name: str) -> int:
        result = self.query(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
        return result[0]["cnt"]

    def test_set_not_null_success(self):
        table_name = f"{self.table_path}_success"
        self._create_table(table_name)

        for i in range(1, 11):
            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES ({i}, {i * 10})")

        self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value SET NOT NULL")

        assert self._count_nulls(table_name) == 0
        assert self._row_count(table_name) == 10

        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (100, 42)")
        assert self._row_count(table_name) == 11

        with pytest.raises(ydb.issues.Error):
            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (200, NULL)")

        with pytest.raises(ydb.issues.Error):
            self.query(f"UPSERT INTO `{table_name}` (id) VALUES (300)")

        assert self._count_nulls(table_name) == 0

    def test_set_not_null_rejects_existing_nulls(self):
        table_name = f"{self.table_path}_existing_nulls"
        self._create_table(table_name)

        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (1, 10), (2, 20)")
        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (3, NULL)")

        with pytest.raises(ydb.issues.PreconditionFailed):
            self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value SET NOT NULL")

        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (4, NULL)")
        assert self._count_nulls(table_name) == 2

    def test_drop_not_null(self):
        table_name = f"{self.table_path}_drop"
        self._create_table(table_name)

        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (1, 10), (2, 20)")
        self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value SET NOT NULL")

        with pytest.raises(ydb.issues.Error):
            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (3, NULL)")

        self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value DROP NOT NULL")

        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (3, NULL)")
        assert self._count_nulls(table_name) == 1

    def test_set_not_null_idempotent_cycle(self):
        table_name = f"{self.table_path}_cycle"
        self._create_table(table_name)
        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (1, 10)")

        for _ in range(3):
            self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value SET NOT NULL")
            with pytest.raises(ydb.issues.Error):
                self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (2, NULL)")
            self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value DROP NOT NULL")
            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (2, NULL)")
            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (2, 20)")

        assert self._count_nulls(table_name) == 0

    @pytest.mark.parametrize("type_name", sorted(ALL_TYPES.keys()))
    def test_set_not_null_all_types(self, type_name: str):
        table_name = f"{self.table_path}_type_{cleanup_type_name(type_name)}"
        self._create_table(table_name, value_type=type_name)

        make_literal = type_to_literal_lambda[type_name]
        for i in range(1, 4):
            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES ({i}, {make_literal(i)})")

        self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value SET NOT NULL")
        assert self._count_nulls(table_name) == 0

        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (4, {make_literal(4)})")
        assert self._row_count(table_name) == 4

        with pytest.raises(ydb.issues.Error):
            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (5, NULL)")

        assert self._count_nulls(table_name) == 0
