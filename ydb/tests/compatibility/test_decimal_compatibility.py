import pytest
from decimal import Decimal
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestDecimalMixedCluster(MixedClusterFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
            }
        )

    def test_decimal_mixed_cluster(self):
        table_name = "decimal_mixed_test"

        current_version = self.versions[0] if hasattr(self, 'versions') and self.versions else (25, 1)
        supports_decimal = current_version >= (25, 1)

        if not supports_decimal:
            pytest.skip("Decimal types are not supported in this version")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    decimal_12_2 Decimal(12,2) NOT NULL,
                    decimal_22_9 Decimal(22,9) NOT NULL,
                    decimal_35_10 Decimal(35,10) NOT NULL,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                )
            """

            session_pool.execute_with_retries(query)

            test_data = [
                (1, "12345678.90", "123456.123456789", "123456789.1234567890"),
                (2, "0.00", "0.000000001", "0.0000000001"),
                (3, "9999999999.99", "999999999.999999999", "999999999999999999999999.123"),
                (4, "-12345678.90", "-123456.123456789", "-123456789.1234567890"),
            ]

            for id_val, dec12, dec22, dec35 in test_data:
                query = f"""
                    UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10)
                    VALUES ($id, $dec12, $dec22, $dec35)
                """

                params = {
                    '$id': id_val,
                    '$dec12': ydb.TypedValue(Decimal(dec12), ydb.DecimalType(12, 2)),
                    '$dec22': ydb.TypedValue(Decimal(dec22), ydb.DecimalType(22, 9)),
                    '$dec35': ydb.TypedValue(Decimal(dec35), ydb.DecimalType(35, 10)),
                }

                session_pool.execute_with_retries(query, params)

            for id_val, dec12, dec22, dec35 in test_data:
                query = f"SELECT * FROM `{table_name}` WHERE id = {id_val}"
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1
                row = result_sets[0].rows[0]
                assert row["decimal_12_2"] == Decimal(dec12)
                assert row["decimal_22_9"] == Decimal(dec22)
                assert row["decimal_35_10"] == Decimal(dec35)


class TestDecimalRestartToAnotherVersion(RestartToAnotherVersionFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
            }
        )

    def test_decimal_version_change(self):
        table_name = "decimal_version_test"

        current_version = self.versions[0] if hasattr(self, 'versions') and self.versions else (25, 1)
        supports_decimal = current_version >= (25, 1)

        if not supports_decimal:
            pytest.skip("Decimal types are not supported in this version")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    decimal_12_2 Decimal(12,2) NOT NULL,
                    decimal_22_9 Decimal(22,9) NOT NULL,
                    decimal_35_10 Decimal(35,10) NOT NULL,
                    decimal_nullable Decimal(22,9),
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                )
            """

            session_pool.execute_with_retries(query)

            test_data = [
                (1, "12345678.90", "123456.123456789", "123456789.1234567890", "999.999999999"),
                (2, "0.00", "0.000000001", "0.0000000001", None),
                (3, "-9999999999.99", "-999999999.999999999", "-999999999999999999999999.123", "0.000000000"),
            ]

            for id_val, dec12, dec22, dec35, dec_null in test_data:
                if dec_null is None:
                    query = f"""
                        UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10, decimal_nullable)
                        VALUES ($id, $dec12, $dec22, $dec35, NULL)
                    """

                    params = {
                        '$id': id_val,
                        '$dec12': ydb.TypedValue(Decimal(dec12), ydb.DecimalType(12, 2)),
                        '$dec22': ydb.TypedValue(Decimal(dec22), ydb.DecimalType(22, 9)),
                        '$dec35': ydb.TypedValue(Decimal(dec35), ydb.DecimalType(35, 10))
                    }

                    session_pool.execute_with_retries(query, params)
                else:
                    query = f"""
                        UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10, decimal_nullable)
                        VALUES ($id, $dec12, $dec22, $dec35, $dec_null)
                    """

                    params = {
                        '$id': id_val,
                        '$dec12': ydb.TypedValue(Decimal(dec12), ydb.DecimalType(12, 2)),
                        '$dec22': ydb.TypedValue(Decimal(dec22), ydb.DecimalType(22, 9)),
                        '$dec35': ydb.TypedValue(Decimal(dec35), ydb.DecimalType(35, 10)),
                        '$dec_null': ydb.TypedValue(Decimal(dec_null), ydb.DecimalType(22, 9))
                    }

                    session_pool.execute_with_retries(query, params)

            self.check_decimal_data(table_name, test_data)

        self.change_cluster_version()
        self.check_decimal_data(table_name, test_data)

        with ydb.QuerySessionPool(self.driver) as session_pool:
            new_data = [
                (4, "98765432.10", "987654.987654321", "987654321.9876543210", "123.456789012"),
                (5, "1.00", "1.000000000", "1.0000000000", None),
            ]

            for id_val, dec12, dec22, dec35, dec_null in new_data:
                if dec_null is None:
                    query = f"""
                        UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10, decimal_nullable)
                        VALUES ($id, $dec12, $dec22, $dec35, NULL)
                    """

                    params = {
                        '$id': id_val,
                        '$dec12': ydb.TypedValue(Decimal(dec12), ydb.DecimalType(12, 2)),
                        '$dec22': ydb.TypedValue(Decimal(dec22), ydb.DecimalType(22, 9)),
                        '$dec35': ydb.TypedValue(Decimal(dec35), ydb.DecimalType(35, 10))
                    }

                    session_pool.execute_with_retries(query, params)
                else:
                    query = f"""
                        UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10, decimal_nullable)
                        VALUES ($id, $dec12, $dec22, $dec35, $dec_null)
                    """

                    params = {
                        '$id': id_val,
                        '$dec12': ydb.TypedValue(Decimal(dec12), ydb.DecimalType(12, 2)),
                        '$dec22': ydb.TypedValue(Decimal(dec22), ydb.DecimalType(22, 9)),
                        '$dec35': ydb.TypedValue(Decimal(dec35), ydb.DecimalType(35, 10)),
                        '$dec_null': ydb.TypedValue(Decimal(dec_null), ydb.DecimalType(22, 9))
                    }

                    session_pool.execute_with_retries(query, params)

            all_data = test_data + new_data
            self.check_decimal_data(table_name, all_data)

    def check_decimal_data(self, table_name, expected_data):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for id_val, dec12, dec22, dec35, dec_null in expected_data:
                query = f"SELECT * FROM `{table_name}` WHERE id = {id_val}"
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1
                row = result_sets[0].rows[0]
                assert row["decimal_12_2"] == Decimal(dec12)
                assert row["decimal_22_9"] == Decimal(dec22)
                assert row["decimal_35_10"] == Decimal(dec35)

                if dec_null is None:
                    assert row["decimal_nullable"] is None
                else:
                    assert row["decimal_nullable"] == Decimal(dec_null)


class TestDecimalRollingUpgrade(RollingUpgradeAndDowngradeFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
            }
        )

    def test_decimal_rolling_upgrade(self):
        table_name = "decimal_rolling_test"

        current_version = self.versions[0] if hasattr(self, 'versions') and self.versions else (25, 1)
        supports_decimal = current_version >= (25, 1)

        if not supports_decimal:
            pytest.skip("Decimal types are not supported in this version")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    decimal_12_2 Decimal(12,2) NOT NULL,
                    decimal_22_9 Decimal(22,9) NOT NULL,
                    decimal_35_10 Decimal(35,10) NOT NULL,
                    ts Timestamp,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                )
            """

            session_pool.execute_with_retries(query)

            initial_data = [
                (1, "12345678.90", "123456.123456789", "123456789.1234567890"),
                (2, "0.00", "0.000000001", "0.0000000001"),
                (3, "-9999999999.99", "-999999999.999999999", "-999999999999999999999999.123"),
            ]

            for id_val, dec12, dec22, dec35 in initial_data:
                query = f"""
                    UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10)
                    VALUES ($id, $dec12, $dec22, $dec35)
                """

                params = {
                    '$id': id_val,
                    '$dec12': ydb.TypedValue(Decimal(dec12), ydb.DecimalType(12, 2)),
                    '$dec22': ydb.TypedValue(Decimal(dec22), ydb.DecimalType(22, 9)),
                    '$dec35': ydb.TypedValue(Decimal(dec35), ydb.DecimalType(35, 10)),
                }

                session_pool.execute_with_retries(query, params)

        for step in self.roll():
            self.check_decimal_data_rolling(table_name, initial_data)
            with ydb.QuerySessionPool(self.driver) as session_pool:
                step_id = len(initial_data) + (step or 0) + 1
                new_dec12 = f"{step_id * 1000000}.{step_id % 100:02d}"
                new_dec22 = f"{step_id}.{step_id:09d}"
                new_dec35 = f"{step_id * 1000000000}.{step_id:010d}"

                query = f"""
                    UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10)
                    VALUES ($id, $dec12, $dec22, $dec35)
                """

                params = {
                    '$id': step_id,
                    '$dec12': ydb.TypedValue(Decimal(new_dec12), ydb.DecimalType(12, 2)),
                    '$dec22': ydb.TypedValue(Decimal(new_dec22), ydb.DecimalType(22, 9)),
                    '$dec35': ydb.TypedValue(Decimal(new_dec35), ydb.DecimalType(35, 10)),
                }

                session_pool.execute_with_retries(query, params)
                query = f"SELECT * FROM `{table_name}` WHERE id = {step_id}"
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1
                row = result_sets[0].rows[0]
                assert row["decimal_12_2"] == Decimal(new_dec12)
                assert row["decimal_22_9"] == Decimal(new_dec22)
                assert row["decimal_35_10"] == Decimal(new_dec35)

    def check_decimal_data_rolling(self, table_name, expected_data):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for id_val, dec12, dec22, dec35 in expected_data:
                query = f"SELECT * FROM `{table_name}` WHERE id = {id_val}"
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1
                row = result_sets[0].rows[0]
                assert row["decimal_12_2"] == Decimal(dec12)
                assert row["decimal_22_9"] == Decimal(dec22)
                assert row["decimal_35_10"] == Decimal(dec35)


class TestDecimalOperations(RestartToAnotherVersionFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
            }
        )

    def test_decimal_operations(self):
        table_name = "decimal_operations_test"

        current_version = self.versions[0] if hasattr(self, 'versions') and self.versions else (25, 1)
        supports_decimal = current_version >= (25, 1)

        if not supports_decimal:
            pytest.skip("Decimal types are not supported in this version")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    decimal_12_2 Decimal(12,2) NOT NULL,
                    decimal_22_9 Decimal(22,9) NOT NULL,
                    decimal_35_10 Decimal(35,10) NOT NULL,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                )
            """

            session_pool.execute_with_retries(query)

            test_data = [
                (1, "100.00", "100.5", "100.1234567890"),
                (2, "200.00", "200.5", "200.1234567890"),
                (3, "300.00", "300.5", "300.1234567890"),
            ]

            for id_val, dec12, dec22, dec35 in test_data:
                query = f"""
                    UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10)
                    VALUES ($id, $dec12, $dec22, $dec35)
                """

                params = {
                    '$id': id_val,
                    '$dec12': ydb.TypedValue(Decimal(dec12), ydb.DecimalType(12, 2)),
                    '$dec22': ydb.TypedValue(Decimal(dec22), ydb.DecimalType(22, 9)),
                    '$dec35': ydb.TypedValue(Decimal(dec35), ydb.DecimalType(35, 10)),
                }

                session_pool.execute_with_retries(query, params)

            query = f"""
                SELECT
                    SUM(decimal_12_2) as sum_12_2,
                    SUM(decimal_22_9) as sum_22_9,
                    SUM(decimal_35_10) as sum_35_10,
                    AVG(decimal_12_2) as avg_12_2,
                    AVG(decimal_22_9) as avg_22_9,
                    AVG(decimal_35_10) as avg_35_10,
                    MIN(decimal_12_2) as min_12_2,
                    MAX(decimal_12_2) as max_12_2
                FROM `{table_name}`
            """

            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 1
            row = result_sets[0].rows[0]
            assert row["sum_12_2"] == Decimal("600.00")
            assert row["min_12_2"] == Decimal("100.00")
            assert row["max_12_2"] == Decimal("300.00")

        self.change_cluster_version()

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                SELECT
                    SUM(decimal_12_2) as sum_12_2,
                    SUM(decimal_22_9) as sum_22_9,
                    SUM(decimal_35_10) as sum_35_10,
                    AVG(decimal_12_2) as avg_12_2,
                    AVG(decimal_22_9) as avg_22_9,
                    AVG(decimal_35_10) as avg_35_10,
                    MIN(decimal_12_2) as min_12_2,
                    MAX(decimal_12_2) as max_12_2
                FROM `{table_name}`
            """

            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 1
            row = result_sets[0].rows[0]
            assert row["sum_12_2"] == Decimal("600.00")
            assert row["min_12_2"] == Decimal("100.00")
            assert row["max_12_2"] == Decimal("300.00")
