import logging

from .base import SolomonReadingTestBase
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestBasicReading(SolomonReadingTestBase):
    def check_query_result(self, result, error, downsampling_disabled):
        if error is not None:
            return False

        timestamps = [int(row["ts"].timestamp()) for row in result[0].rows]
        values = [int(row["value"]) for row in result[0].rows]

        if not downsampling_disabled:
            return \
                timestamps == [ts for ts in self.basic_reading_timestamps if ts % 15 == 0] and \
                values == [value for value in self.basic_reading_values if value % 3 == 0]
        else:
            return \
                timestamps == self.basic_reading_timestamps and \
                values == self.basic_reading_values

    @link_test_case("#16398")
    def test_basic_reading(self):
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
            SELECT * FROM local_solomon.basic_reading WITH (
                program=@@{cluster="my_cluster", service="my_service", test_type="basic_reading_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        assert self.check_query_result(*self.execute_query(query), False)

        # query using `program` param with enabled downsampling
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                program = @@{cluster="my_cluster", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "false",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        assert self.check_query_result(*self.execute_query(query), False)

        # query using `program` param with disabled downsampling
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                program = @@{cluster="my_cluster", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "true",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        assert self.check_query_result(*self.execute_query(query), True)

        # query using `selectors` param with enabled downsampling
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                selectors = @@{cluster="my_cluster", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "false",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        assert self.check_query_result(*self.execute_query(query), False)

        # query using `selectors` param with disabled downsampling
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                selectors = @@{cluster="my_cluster", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "true",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        assert self.check_query_result(*self.execute_query(query), True)
