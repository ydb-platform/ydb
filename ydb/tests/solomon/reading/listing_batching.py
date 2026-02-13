import logging

from .base import SolomonReadingTestBase
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestListingBatching(SolomonReadingTestBase):
    def check_full_listing_result(self, result_set, error):
        if error is not None:
            return False, error

        rows = []
        for result in result_set:
            rows.extend(result.rows)

        total_size, first_label_size = self.listing_batching_metrics_sizes[0], self.listing_batching_metrics_sizes[1]
        if (len(rows) != total_size):
            return False, f"Result size differs from expected: have {len(rows)}, should be {total_size}"

        test_labels = []
        for row in rows:
            test_labels.append(row["test_label"])

        test_labels.sort(key=lambda x: int(x))

        for i in range(total_size - first_label_size):
            if int(test_labels[i]) != 0:
                return False, f"Invalid test_label = \"0\" values, should contain {total_size - first_label_size} zeros, have: {i}"
        for i in range(total_size - first_label_size, total_size):
            if int(test_labels[i]) != i - total_size + first_label_size:
                return False, f"Missing test_label = {i - total_size + first_label_size}"

        return True, None

    @link_test_case("#16395")
    def test_listing_batching_solomon(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_solomon WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        query = """
            SELECT test_label FROM local_solomon.listing_batching WITH (
                selectors = @@{cluster="listing_batching", service="my_service", test_type="listing_batching_test", test_label="*"}@@,

                labels = "test_label",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        success, error = self.check_full_listing_result(*self.execute_query(query))
        assert success, error

    @link_test_case("#23190")
    def test_listing_batching_monitoring(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "listing_batching",
                CLUSTER         = "listing_batching",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        query = """
            SELECT test_label FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="listing_batching_test", test_label="*"}@@,

                labels = "test_label",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        success, error = self.check_full_listing_result(*self.execute_query(query))
        assert success, error
