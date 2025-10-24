import logging

from .base import SolomonReadingTestBase
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestListingPaging(SolomonReadingTestBase):
    def check_full_listing_result(self, result_set, error):
        if error is not None:
            return False, error

        rows = []
        for result in result_set:
            rows.extend(result.rows)

        if (len(rows) != self.listing_paging_metrics_size):
            return False, "Result size differs from expected: have {}, should be {}".format(len(rows), self.listing_paging_metrics_size)

        test_labels = []
        for row in rows:
            test_labels.append(row["test_label"])

        test_labels.sort(key=lambda x: int(x))
        for i in range(len(test_labels)):
            if test_labels[i].decode("utf-8") != str(i):
                return False, "Missing test_label = {}".format(str(i))

        return True, None

    def check_listing_size(self, result_set, error):
        if error is not None:
            return False, error

        rows = []
        for result in result_set:
            rows.extend(result.rows)

        if (len(rows) != self.listing_paging_metrics_size + 1):
            return False, "Result size differs from expected: have {}, should be {}".format(len(rows), self.listing_paging_metrics_size)

        return True, None

    @link_test_case("#16395")
    def test_listing_paging_solomon(self):
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
            SELECT test_label FROM local_solomon.listing_paging WITH (
                selectors = @@{cluster="listing_paging", service="my_service", test_type="listing_paging_test", test_label="*"}@@,

                labels = "test_label",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        success, error = self.check_full_listing_result(*self.execute_query(query))
        assert success, error

        query = """
            SELECT * FROM local_solomon.listing_paging WITH (
                selectors = @@{cluster="listing_paging", service="my_service", test_type="listing_paging_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        success, error = self.check_listing_size(*self.execute_query(query))
        assert success, error

    @link_test_case("#23190")
    def test_listing_paging_monitoring(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "listing_paging",
                CLUSTER         = "listing_paging",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        query = """
            SELECT test_label FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="listing_paging_test", test_label="*"}@@,

                labels = "test_label",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        success, error = self.check_full_listing_result(*self.execute_query(query))
        assert success, error

        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="listing_paging_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        success, error = self.check_listing_size(*self.execute_query(query))
        assert success, error
