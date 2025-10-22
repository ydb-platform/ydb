import pytest

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    MixedClusterFixture,
    RollingUpgradeAndDowngradeFixture,
)
from ydb.tests.oss.ydb_sdk_import import ydb


class WorkloadManagerWorkload:
    def __init__(self, driver, endpoint):
        self.driver = driver
        self.endpoint = endpoint
        self.table_name = "/Root/simple_reader_table"
        self.batch_size = 1000

    def create_resource_pool(self):
        self.execute_query("""
            CREATE RESOURCE POOL TestResourcePool WITH (
                CONCURRENT_QUERY_LIMIT=20,
                QUEUE_SIZE=1000
            );
        """)

    def alter_resource_pool(self):
        self.execute_query("""
            ALTER RESOURCE POOL TestResourcePool
                SET (CONCURRENT_QUERY_LIMIT = 30, QUEUE_SIZE = 100),
                RESET (QUERY_MEMORY_LIMIT_PERCENT_PER_NODE);
        """)

    def create_resource_pool_classifier(self):
        self.execute_query("""
            CREATE RESOURCE POOL CLASSIFIER TestResourcePoolClassifier WITH (
                RANK=20,
                RESOURCE_POOL="TestResourcePool"
            );
        """)

    def alter_resource_pool_classifier(self):
        self.execute_query("""
            ALTER RESOURCE POOL CLASSIFIER TestResourcePoolClassifier
                SET (RANK = 1, RESOURCE_POOL = "TestResourcePool"),
                RESET (MEMBER_NAME);
        """)

    def wait_for_connection(self, timeout_seconds=240):
        def predicate():
            try:
                with ydb.QuerySessionPool(self.driver) as session_pool:
                    session_pool.execute_with_retries("SELECT 1")
                return True
            except Exception:
                return False

        return wait_for(predicate, timeout_seconds=timeout_seconds, step_seconds=1)

    def execute_query(self, query_body):
        try:
            with ydb.QuerySessionPool(self.driver) as session_pool:
                result = session_pool.execute_with_retries(query_body)
                return result[0].rows if result else None
        except Exception:
            assert self.wait_for_connection(), "Failed to restore connection in execute_query"
            with ydb.QuerySessionPool(self.driver) as session_pool:
                result = session_pool.execute_with_retries(query_body)
                return result[0].rows if result else None

    def get_resource_pool(self, throw_exception):
        query = "SELECT * FROM `.sys/resource_pools` WHERE Name = 'TestResourcePool'"
        try:
            return (self.execute_query(query)[0], True)
        except Exception as e:
            if throw_exception:
                raise e
            return (str(e), False)

    def get_resource_pool_classifier(self, throw_exception):
        query = "SELECT * FROM `.sys/resource_pool_classifiers` WHERE Name = 'TestResourcePoolClassifier'"
        try:
            return (self.execute_query(query)[0], True)
        except Exception as e:
            if throw_exception:
                raise e
            return (str(e), False)

    def validate_resource_pool(self, throw_exception=False):
        value, result = self.get_resource_pool(throw_exception)
        assert result, f"Failed to get resource pool: {value}"
        assert value["ConcurrentQueryLimit"] == 20
        assert value["QueueSize"] == 1000

    def validate_altered_resource_pool(self, throw_exception=False):
        value, result = self.get_resource_pool(throw_exception)
        assert result, f"Failed to get resource pool: {value}"
        assert value["ConcurrentQueryLimit"] == 30
        assert value["QueueSize"] == 100

    def validate_resource_pool_classifier(self, throw_exception=False):
        value, result = self.get_resource_pool_classifier(throw_exception)
        assert result, f"Failed to get resource pool classifier: {value}"
        assert value["ResourcePool"] == "TestResourcePool"
        assert value["Rank"] == 20

    def validate_altered_resource_pool_classifier(self, throw_exception=False):
        value, result = self.get_resource_pool_classifier(throw_exception)
        assert result, f"Failed to get resource pool classifier: {value}"
        assert value["ResourcePool"] == "TestResourcePool"
        assert value["Rank"] == 1


class TestWorkloadManagerMixedCluster(MixedClusterFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(extra_feature_flags={"enable_resource_pools": True})

    def test_workload_manager_mixed_cluster(self):
        if min(self.versions) < (25, 1, 4):
            pytest.skip("Test is not supported for this cluster version")

        workload = WorkloadManagerWorkload(self.driver, self.endpoint)
        workload.create_resource_pool()
        workload.validate_resource_pool()
        workload.alter_resource_pool()
        workload.validate_altered_resource_pool()
        workload.create_resource_pool_classifier()
        workload.validate_resource_pool_classifier()
        workload.alter_resource_pool_classifier()
        workload.validate_altered_resource_pool_classifier()


class TestWorkloadManagerRestartToAnotherVersion(RestartToAnotherVersionFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(extra_feature_flags={"enable_resource_pools": True})

    def test_workload_manager_version_upgrade(self):
        if min(self.versions) < (25, 1, 4):
            pytest.skip("Test is not supported for this cluster version")

        workload = WorkloadManagerWorkload(self.driver, self.endpoint)
        workload.create_resource_pool()
        workload.validate_resource_pool()
        workload.create_resource_pool_classifier()
        workload.validate_resource_pool_classifier()
        self.change_cluster_version()
        workload.validate_resource_pool()
        workload.validate_resource_pool_classifier()
        workload.alter_resource_pool()
        workload.validate_altered_resource_pool()
        workload.alter_resource_pool_classifier()
        workload.validate_altered_resource_pool_classifier()


class TestWorkloadManagerTabletTransfer(RollingUpgradeAndDowngradeFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(extra_feature_flags={"enable_resource_pools": True})

    def test_workload_manager_tablet_transfer(self):
        if min(self.versions) < (25, 1, 4):
            pytest.skip("Test is not supported for this cluster version")

        workload = WorkloadManagerWorkload(self.driver, self.endpoint)
        workload.create_resource_pool()
        workload.validate_resource_pool()
        workload.create_resource_pool_classifier()
        workload.validate_resource_pool_classifier()
        step_count = 0
        for _ in self.roll():
            if step_count >= 8:
                break

            try:
                workload.validate_resource_pool(True)
                workload.validate_resource_pool_classifier(True)
            except Exception:
                assert workload.wait_for_connection(), "Failed to restore connection after rolling upgrade"
            step_count += 1

        workload.validate_resource_pool()
        workload.validate_resource_pool_classifier()
