import allure
import pytest

import ydb

from ydb.tests.workload_manager.common.workload_manager import WorkloadManagerBase, ResourcePool
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class WorkloadManagerExplicitResourcePool(WorkloadManagerBase):

    pool_a_name = 'test_pool_explicit_a'
    pool_b_name = 'test_pool_explicit_b'

    @classmethod
    def get_resource_pools(cls) -> list[ResourcePool]:
        # No users are passed, so no classifiers are created. The pools are addressed
        # explicitly through the SDK ``pool_id`` parameter.
        return [
            ResourcePool(cls.pool_a_name, [], concurrent_query_limit=2),
            ResourcePool(cls.pool_b_name, [], concurrent_query_limit=2),
        ]

    @classmethod
    def benchmark_setup(cls) -> None:
        pass

    @classmethod
    def get_path(cls) -> str:
        return ''

    @classmethod
    def get_query_list(cls) -> list[str]:
        return []

    # These tests do not run the workload via the `workload run` CLI command, so override the
    # inherited `test` method to avoid running the default workload.
    def test(self):
        pytest.skip('Explicit resource pool tests do not run the default workload')

    def test_explicit_resource_pool(self):
        """Verify that the SDK ``pool_id`` parameter routes the query to the specified pool."""
        query = 'SELECT 1'

        # Route the query explicitly to each pool through the SDK pool_id parameter.
        used = self.execute_query_in_pool(query, pool_id=self.pool_a_name)
        assert used == self.pool_a_name, \
            f'Query with pool_id={self.pool_a_name} is expected to use pool {self.pool_a_name}, got {used}'

        used = self.execute_query_in_pool(query, pool_id=self.pool_b_name)
        allure.attach(str(used), 'used pool (b)', allure.attachment_type.TEXT)
        assert used == self.pool_b_name, \
            f'Query with pool_id={self.pool_b_name} is expected to use pool {self.pool_b_name}, got {used}'

    def test_explicit_resource_pool_not_found(self):
        """Verify that specifying a non-existent resource pool makes the query fail with NOT_FOUND."""
        query = 'SELECT 1'
        with pytest.raises(ydb.issues.NotFound) as exc_info:
            self.execute_query_in_pool(query, pool_id='nonexistent_pool')
        assert exc_info.value.status == ydb.issues.StatusCode.NOT_FOUND
        assert 'Resource pool nonexistent_pool not found' in str(exc_info.value)


class TestExplicitResourcePool(WorkloadManagerExplicitResourcePool, FunctionalTestBase):
    """Test that a query can be assigned to a concrete resource pool through the SDK ``pool_id`` parameter."""

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        super().setup_class()
