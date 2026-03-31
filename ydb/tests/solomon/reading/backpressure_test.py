import logging

from ydb.library.yql.tools.solomon_emulator.client.client import get_api_calls_count, cleanup_api_calls
from ydb.tests.library.test_meta import link_test_case

from .base import SolomonReadingTestBase

logger = logging.getLogger(__name__)


class TestBackpressure(SolomonReadingTestBase):
    @classmethod
    def setup_class(cls):
        super().setup_class("backpressure_test")

    def check_backpressure_test_result(self, result_set, error):
        if error is not None:
            return False, error

        rows = []
        for result in result_set:
            rows.extend(result.rows)

        if len(rows) != 1:
            return False, "Result size differs from expected: have {}, should be 1".format(len(rows))

        api_call_count = get_api_calls_count()
        if api_call_count >= self.backpressure_test_max_inflight + 3:
            return False, "Solomon emulator received too many api calls, shouldnt be hugher then {}, have {}".format(self.backpressure_test_max_inflight + 3, api_call_count)

        return True, None

    @link_test_case("#16396")
    def test_backpressure_solomon(self):
        cleanup_api_calls()

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
            SELECT value FROM local_solomon.backpressure_test WITH (
                selectors = @@{cluster="backpressure_test", service="my_service", test_type="backpressure_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-03T00:00:00Z"
            )
            LIMIT 1
        """
        success, error = self.check_backpressure_test_result(*self.execute_query(query))
        assert success, error

    @link_test_case("#23191")
    def test_backpressure_monitoring(self):
        cleanup_api_calls()

        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "backpressure_test",
                CLUSTER         = "backpressure_test",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        query = """
            SELECT value FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="backpressure_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-03T00:00:00Z"
            )
            LIMIT 1
        """
        success, error = self.check_backpressure_test_result(*self.execute_query(query))
        assert success, error
