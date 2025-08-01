import pytest
from decimal import Decimal
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class DecimalCompatibilityBase:

    def skip_if_unsupported_version(self):
        if (min(self.versions) < (25, 1)):
            pytest.skip("Decimal types are not supported in this version")

    def create_decimal_table(self, session_pool, table_name, columns_schema):
        query = f"""
            CREATE TABLE `{table_name}` (
                {columns_schema}
            ) WITH (
                STORE = COLUMN
            )
        """

        session_pool.execute_with_retries(query)

    def create_decimal_params(self, dec12, dec22, dec35, dec_null=None):
        params = {
            '$dec12': ydb.TypedValue(Decimal(dec12), ydb.DecimalType(12, 2)),
            '$dec22': ydb.TypedValue(Decimal(dec22), ydb.DecimalType(22, 9)),
            '$dec35': ydb.TypedValue(Decimal(dec35), ydb.DecimalType(35, 10)),
        }

        if dec_null is not None:
            params['$dec_null'] = ydb.TypedValue(Decimal(dec_null), ydb.DecimalType(22, 9))

        return params

    def insert_decimal_data(self, session_pool, table_name, data, has_nullable=False):
        for row_data in data:
            if has_nullable:
                id_val, dec12, dec22, dec35, dec_null = row_data
                if dec_null is None:
                    query = f"""
                        UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10, decimal_nullable)
                        VALUES ($id, $dec12, $dec22, $dec35, NULL)
                    """

                    params = {'$id': id_val}
                    params.update(self.create_decimal_params(dec12, dec22, dec35))
                else:
                    query = f"""
                        UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10, decimal_nullable)
                        VALUES ($id, $dec12, $dec22, $dec35, $dec_null)
                    """

                    params = {'$id': id_val}
                    params.update(self.create_decimal_params(dec12, dec22, dec35, dec_null))
            else:
                id_val, dec12, dec22, dec35 = row_data
                query = f"""
                    UPSERT INTO `{table_name}` (id, decimal_12_2, decimal_22_9, decimal_35_10)
                    VALUES ($id, $dec12, $dec22, $dec35)
                """

                params = {'$id': id_val}
                params.update(self.create_decimal_params(dec12, dec22, dec35))

            session_pool.execute_with_retries(query, params)

    def check_decimal_data(self, table_name, expected_data, has_nullable=False):
        raise NotImplementedError("Not implemented")

    def generate_test_data(self, count, has_nullable=False):
        test_data = []
        for i in range(1, count + 1):
            if i % 4 == 0:
                dec12 = f"{i * 1000000}.{i % 100:02d}"
                dec22 = f"{i * 1000}.{i:09d}"
                dec35 = f"{i * 1000000000}.{i:010d}"
                dec_null = f"{i * 100}.{i:09d}" if has_nullable and i % 2 == 0 else None
            elif i % 4 == 1:
                dec12 = f"{i}.{i % 100:02d}"
                dec22 = f"{i * 1000}.{i:09d}"
                dec35 = f"{i * 1000000}.{i:010d}"
                dec_null = f"{i * 100}.{i:09d}" if has_nullable and i % 2 == 0 else None
            elif i % 4 == 2:
                dec12 = f"-{i * 100000}.{i % 100:02d}"
                dec22 = f"-{i * 100}.{i:09d}"
                dec35 = f"-{i * 100000000}.{i:010d}"
                dec_null = f"-{i * 10}.{i:09d}" if has_nullable and i % 2 == 0 else None
            else:
                dec12 = f"0.{i % 100:02d}"
                dec22 = f"0.{i:09d}"
                dec35 = f"0.{i:010d}"
                dec_null = None if has_nullable and i % 2 == 0 else f"0.{i:09d}"

            if has_nullable:
                test_data.append((i, dec12, dec22, dec35, dec_null))
            else:
                test_data.append((i, dec12, dec22, dec35))

        return test_data


class TestDecimalMixedCluster(MixedClusterFixture, DecimalCompatibilityBase):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
            }
        )

    def test_decimal_mixed_cluster(self):
        table_name = "decimal_mixed_test"

        self.skip_if_unsupported_version()

        with ydb.QuerySessionPool(self.driver) as session_pool:
            columns_schema = """
                id Int64 NOT NULL,
                decimal_12_2 Decimal(12,2) NOT NULL,
                decimal_22_9 Decimal(22,9) NOT NULL,
                decimal_35_10 Decimal(35,10) NOT NULL,
                PRIMARY KEY (id)
            """

            self.create_decimal_table(session_pool, table_name, columns_schema)

            test_data = self.generate_test_data(1000)

            self.insert_decimal_data(session_pool, table_name, test_data)
            self.check_decimal_data(table_name, test_data)

    def check_decimal_data(self, table_name, expected_data, has_nullable=False):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for id_val, dec12, dec22, dec35 in expected_data:
                query = f"SELECT * FROM `{table_name}` WHERE id = {id_val}"
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1
                row = result_sets[0].rows[0]
                assert row["id"] == id_val
                assert row["decimal_12_2"] == Decimal(dec12)
                assert row["decimal_22_9"] == Decimal(dec22)
                assert row["decimal_35_10"] == Decimal(dec35)


class TestDecimalRestartToAnotherVersion(RestartToAnotherVersionFixture, DecimalCompatibilityBase):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
            }
        )

    def test_decimal_version_change(self):
        table_name = "decimal_version_test"

        self.skip_if_unsupported_version()

        with ydb.QuerySessionPool(self.driver) as session_pool:
            columns_schema = """
                id Int64 NOT NULL,
                decimal_12_2 Decimal(12,2) NOT NULL,
                decimal_22_9 Decimal(22,9) NOT NULL,
                decimal_35_10 Decimal(35,10) NOT NULL,
                decimal_nullable Decimal(22,9),
                PRIMARY KEY (id)
            """

            self.create_decimal_table(session_pool, table_name, columns_schema)

            test_data = self.generate_test_data(500, has_nullable=True)

            self.insert_decimal_data(session_pool, table_name, test_data, has_nullable=True)
            self.check_decimal_data(table_name, test_data, has_nullable=True)

        self.change_cluster_version()
        self.check_decimal_data(table_name, test_data, has_nullable=True)

        with ydb.QuerySessionPool(self.driver) as session_pool:
            new_data = [
                (501, "98765432.10", "987654.987654321", "987654321.9876543210", "123.456789012"),
                (502, "1.00", "1.000000000", "1.0000000000", None),
            ]

            self.insert_decimal_data(session_pool, table_name, new_data, has_nullable=True)
            all_data = test_data + new_data
            self.check_decimal_data(table_name, all_data, has_nullable=True)

    def check_decimal_data(self, table_name, expected_data, has_nullable=False):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for row_data in expected_data:
                if has_nullable:
                    id_val, dec12, dec22, dec35, dec_null = row_data
                else:
                    id_val, dec12, dec22, dec35 = row_data

                query = f"SELECT * FROM `{table_name}` WHERE id = {id_val}"
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) == 1
                row = result_sets[0].rows[0]
                assert row["id"] == id_val
                assert row["decimal_12_2"] == Decimal(dec12)
                assert row["decimal_22_9"] == Decimal(dec22)
                assert row["decimal_35_10"] == Decimal(dec35)

                if has_nullable:
                    if dec_null is None:
                        assert row["decimal_nullable"] is None
                    else:
                        assert row["decimal_nullable"] == Decimal(dec_null)


class TestDecimalRollingUpgrade(RollingUpgradeAndDowngradeFixture, DecimalCompatibilityBase):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
            }
        )

    def test_decimal_rolling_upgrade(self):
        table_name = "decimal_rolling_test"

        self.skip_if_unsupported_version()

        with ydb.QuerySessionPool(self.driver) as session_pool:
            columns_schema = """
                id Int64 NOT NULL,
                decimal_12_2 Decimal(12,2) NOT NULL,
                decimal_22_9 Decimal(22,9) NOT NULL,
                decimal_35_10 Decimal(35,10) NOT NULL,
                ts Timestamp,
                PRIMARY KEY (id)
            """

            self.create_decimal_table(session_pool, table_name, columns_schema)
            initial_data = self.generate_test_data(300)
            self.insert_decimal_data(session_pool, table_name, initial_data)

        step_counter = 0
        for step in self.roll():
            self.check_decimal_data_rolling(table_name, initial_data)
            with ydb.QuerySessionPool(self.driver) as session_pool:
                step_id = 1000 + step_counter
                step_counter += 1
                new_dec12 = f"{step_id * 1000000}.{step_id % 100:02d}"
                new_dec22 = f"{step_id}.{step_id:09d}"
                new_dec35 = f"{step_id * 1000000000}.{step_id:010d}"
                new_data = [(step_id, new_dec12, new_dec22, new_dec35)]
                self.insert_decimal_data(session_pool, table_name, new_data)

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
                assert row["id"] == id_val
                assert row["decimal_12_2"] == Decimal(dec12)
                assert row["decimal_22_9"] == Decimal(dec22)
                assert row["decimal_35_10"] == Decimal(dec35)


class TestDecimalOperations(RestartToAnotherVersionFixture, DecimalCompatibilityBase):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
            }
        )

    def test_decimal_operations(self):
        table_name = "decimal_operations_test"

        self.skip_if_unsupported_version()

        with ydb.QuerySessionPool(self.driver) as session_pool:
            columns_schema = """
                id Int64 NOT NULL,
                decimal_12_2 Decimal(12,2) NOT NULL,
                decimal_22_9 Decimal(22,9) NOT NULL,
                decimal_35_10 Decimal(35,10) NOT NULL,
                PRIMARY KEY (id)
            """

            self.create_decimal_table(session_pool, table_name, columns_schema)
            test_data = self.generate_test_data(200)
            self.insert_decimal_data(session_pool, table_name, test_data)

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
            assert row["sum_12_2"] is not None
            assert row["min_12_2"] is not None
            assert row["max_12_2"] is not None

        self.change_cluster_version()

        with ydb.QuerySessionPool(self.driver) as session_pool:
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 1
            row = result_sets[0].rows[0]
            assert row["sum_12_2"] is not None
            assert row["min_12_2"] is not None
            assert row["max_12_2"] is not None
