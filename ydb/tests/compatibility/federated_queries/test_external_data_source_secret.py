import pytest

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.compatibility.federated_queries.test_external_data_source import ExternalDataTableTestBase


class ExternalDataSourceSecretCompatibilityBase(ExternalDataTableTestBase):
    def create_external_data_source_with_secret_kind(self, kind: str):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = """
                DROP OBJECT IF EXISTS s3_access_key (TYPE SECRET);
                DROP OBJECT IF EXISTS s3_secret_key (TYPE SECRET);
                DROP SECRET IF EXISTS `s3_access_key`;
                DROP SECRET IF EXISTS `s3_secret_key`;
            """
            session_pool.execute_with_retries(query)

            if kind == "object":
                query = f"""
                    CREATE OBJECT s3_access_key (TYPE SECRET) WITH value="{s3_access_key}";
                    CREATE OBJECT s3_secret_key (TYPE SECRET) WITH value="{s3_secret_key}";
                """
            elif kind == "schema":
                query = f"""
                    CREATE SECRET `s3_access_key` WITH (value="{s3_access_key}");
                    CREATE SECRET `s3_secret_key` WITH (value="{s3_secret_key}");
                """
            else:
                raise ValueError(f"Unknown secret kind: {kind}")

            session_pool.execute_with_retries(query)

            query = """
                DROP EXTERNAL DATA SOURCE IF EXISTS s3_source;
            """

            session_pool.execute_with_retries(query)

            query = f"""
                CREATE EXTERNAL DATA SOURCE s3_source WITH (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION = "{s3_endpoint}/{s3_bucket}",
                    AUTH_METHOD="AWS",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="s3_access_key",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="s3_secret_key",
                    AWS_REGION="us-east-1"
                );
            """

            session_pool.execute_with_retries(query)


class TestExternalDataSourceSecretCompatibilityMixedCluster(ExternalDataSourceSecretCompatibilityBase, MixedClusterFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source_secret_object_schema_compatibility(self):
        self.create_external_data_source_with_secret_kind("object")
        self.do_test_external_data()
        self.create_external_data_source_with_secret_kind("schema")
        self.do_test_external_data()


class TestExternalDataSourceSecretCompatibilityRestartToAnotherVersion(
    ExternalDataSourceSecretCompatibilityBase,
    RestartToAnotherVersionFixture,
):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source_secret_object_schema_compatibility(self):
        self.create_external_data_source_with_secret_kind("object")
        self.do_test_external_data()

        self.change_cluster_version()

        self.create_external_data_source_with_secret_kind("schema")
        self.do_test_external_data()


class TestExternalDataSourceSecretCompatibilityRollingUpgradeAndDowngrade(
    ExternalDataSourceSecretCompatibilityBase,
    RollingUpgradeAndDowngradeFixture,
):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source_secret_object_schema_compatibility(self):
        self.create_external_data_source_with_secret_kind("object")
        self.do_test_external_data()

        switched_to_schema = False
        for _ in self.roll():
            if not switched_to_schema:
                self.create_external_data_source_with_secret_kind("schema")
                switched_to_schema = True

            self.do_test_external_data()
