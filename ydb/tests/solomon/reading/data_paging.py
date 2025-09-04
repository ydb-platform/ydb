import logging

from .base import SolomonReadingTestBase
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestDataPaging(SolomonReadingTestBase):
    def check_data_paging_result(self, result_set, error):
        if error is not None:
            return False, error

        rows = []
        for result in result_set:
            rows.extend(result.rows)

        if (len(rows) != self.data_paging_timeseries_size):
            return False, "Result size differs from expected: have {}, should be {}".format(len(rows), self.data_paging_timeseries_size)

        values = []
        for row in rows:
            values.append(row["value"])

        values.sort()
        for i in range(len(values)):
            if values[i] != i:
                return False, "Missing value = {}".format(i)

        return True, None

    @link_test_case("#16396")
    def test_data_paging_solomon(self):
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

        # simplest query with default downsampling settings
        query = """
            SELECT value FROM local_solomon.data_paging WITH (
                selectors = @@{cluster="data_paging", service="my_service", test_type="data_paging_test"}@@,

                `downsampling.grid_interval` = "5",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-03T00:00:00Z"
            )
        """
        success, error = self.check_data_paging_result(*self.execute_query(query))
        assert success, error

    @link_test_case("#23191")
    def test_listing_paging_monitoring(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "data_paging",
                CLUSTER         = "data_paging",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        # simplest query with default downsampling settings
        query = """
            SELECT value FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="data_paging_test"}@@,

                `downsampling.grid_interval` = "5",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-03T00:00:00Z"
            )
        """
        success, error = self.check_data_paging_result(*self.execute_query(query))
        assert success, error
