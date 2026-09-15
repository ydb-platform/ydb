# -*- coding: utf-8 -*-
from decimal import Decimal

import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

import ydb


TABLE_KINDS = ("row", "column")
PG_TYPES = ("pgint2", "pgint4", "pgint8", "pgfloat4", "pgfloat8")


def _table_settings(table_kind, *settings):
    values = list(settings)
    if table_kind == "column":
        values.insert(0, "STORE = COLUMN")
    return f" WITH ({', '.join(values)})" if values else ""


def _start_cluster(enable_pg_types):
    feature_flags = [
        "enable_columnshard_bool",
        "enable_columnshard_interval",
        "enable_columnshard_uuid",
        "enable_columnshard_dy_number",
        "enable_parameterized_decimal",
        "enable_table_datetime64",
    ]
    disabled_feature_flags = None
    if enable_pg_types:
        feature_flags.append("enable_table_pg_types")
    else:
        disabled_feature_flags = ["enable_table_pg_types"]

    config = KikimrConfigGenerator(
        erasure=Erasure.NONE,
        extra_feature_flags=feature_flags,
        disabled_feature_flags=disabled_feature_flags,
        table_service_config={"enable_olap_sink": True},
    )
    cluster = KiKiMR(config)
    cluster.start()

    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=cluster.nodes[1].endpoint,
        )
    )
    driver.wait(timeout=60)
    return cluster, driver


class TestTypeSupportConsistency:
    @classmethod
    def setup_class(cls):
        cls.cluster, cls.driver = _start_cluster(enable_pg_types=True)

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    @pytest.mark.parametrize("table_kind", TABLE_KINDS)
    def test_p1_decimal_primary_key_ranges(self, table_kind):
        """A Decimal primary key must use numeric ordering for range reads."""
        table_name = f"p1_decimal_pk_{table_kind}"
        create = f"""
            CREATE TABLE `{table_name}` (
                k Decimal(22, 9) NOT NULL,
                PRIMARY KEY (k)
            ){_table_settings(table_kind)};
        """
        values = """
            (Decimal('-100', 22, 9)),
            (Decimal('-1', 22, 9)),
            (Decimal('0', 22, 9)),
            (Decimal('2', 22, 9)),
            (Decimal('10', 22, 9)),
            (Decimal('100', 22, 9))
        """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(create)
            session_pool.execute_with_retries(
                f"UPSERT INTO `{table_name}` (k) VALUES {values};"
            )

            result_sets = session_pool.execute_with_retries(
                f"""
                    SELECT k
                    FROM `{table_name}`
                    WHERE k >= Decimal('-1', 22, 9)
                    ORDER BY k;
                """
            )
            actual_lower_bound = [row["k"] for row in result_sets[0].rows]
            assert actual_lower_bound == [
                Decimal("-1"),
                Decimal("0"),
                Decimal("2"),
                Decimal("10"),
                Decimal("100"),
            ]

            result_sets = session_pool.execute_with_retries(
                f"""
                    SELECT k
                    FROM `{table_name}`
                    WHERE k < Decimal('0', 22, 9)
                    ORDER BY k;
                """
            )
            actual_upper_bound = [row["k"] for row in result_sets[0].rows]
            assert actual_upper_bound == [Decimal("-100"), Decimal("-1")]

    @pytest.mark.parametrize("table_kind", TABLE_KINDS)
    @pytest.mark.parametrize("pg_type", PG_TYPES)
    def test_p2_pg_value_round_trip(self, table_kind, pg_type):
        """A PG value written through QueryService must retain its value."""
        table_name = f"p2_{pg_type}_{table_kind}"
        create = f"""
            CREATE TABLE `{table_name}` (
                id Uint64 NOT NULL,
                value {pg_type},
                PRIMARY KEY (id)
            ){_table_settings(table_kind)};
        """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(create)
            session_pool.execute_with_retries(
                f"""
                    UPSERT INTO `{table_name}` (id, value)
                    VALUES (1u, {pg_type}('42'));
                """
            )
            result_sets = session_pool.execute_with_retries(
                f"""
                    SELECT value = {pg_type}('42') AS value_matches
                    FROM `{table_name}`
                    WHERE id = 1u;
                """
            )
            assert result_sets[0].rows[0]["value_matches"] is True

    @pytest.mark.parametrize("table_kind", TABLE_KINDS)
    def test_p4_dynumber_ttl(self, table_kind):
        """DyNumber must be accepted as a TTL column for both table kinds."""
        table_name = f"p4_dynumber_ttl_{table_kind}"
        table_settings = _table_settings(
            table_kind,
            "TTL = Interval('P1D') ON ts AS SECONDS",
        )
        create = f"""
            CREATE TABLE `{table_name}` (
                ts DyNumber NOT NULL,
                PRIMARY KEY (ts)
            ){table_settings};
        """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(create)


class TestPgTypeFeatureFlagConsistency:
    @classmethod
    def setup_class(cls):
        cls.cluster, cls.driver = _start_cluster(enable_pg_types=False)

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    @pytest.mark.parametrize("table_kind", TABLE_KINDS)
    def test_p3_pg_type_respects_disabled_feature_flag(self, table_kind):
        """EnableTablePgTypes=false must reject PG columns in every table kind."""
        table_name = f"p3_pg_flag_{table_kind}"
        create = f"""
            CREATE TABLE `{table_name}` (
                id Uint64 NOT NULL,
                value pgint4,
                PRIMARY KEY (id)
            ){_table_settings(table_kind)};
        """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            with pytest.raises(
                ydb.issues.Error,
                match="support for pg types is disabled",
            ):
                session_pool.execute_with_retries(create)
