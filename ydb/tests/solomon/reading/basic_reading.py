from datetime import timezone
import logging

from .base import SolomonReadingTestBase
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestBasicReading(SolomonReadingTestBase):
    def check_query_result(self, result, error, downsampling_disabled):
        if error is not None:
            return False, error

        timestamps = [int(row["ts"].replace(tzinfo=timezone.utc).timestamp()) for row in result[0].rows]
        values = [int(row["value"]) for row in result[0].rows]

        downsampled_timestamps = [ts for ts in self.basic_reading_timestamps if ts % 15 == 0]
        downsampled_values = [value for value in self.basic_reading_values if value % 3 == 0]

        canon_timestamps = self.basic_reading_timestamps if downsampling_disabled else downsampled_timestamps
        canon_values = self.basic_reading_values if downsampling_disabled else downsampled_values

        if timestamps != canon_timestamps:
            return False, "timstamps differ from canonical, have {}, should be {}".format(timestamps, canon_timestamps)
        elif values != canon_values:
            return False, "values differ from canonical, have {}, should be {}".format(values, canon_values)
        return True, None

    def check_query_result_size(self, result, error):
        if error is not None:
            return False, error

        result_size = len(result[0].rows)

        if (result_size != 1):
            return False, "should only have a single return row, have: {}".format(result_size)

        return True, None

    @link_test_case("#16398")
    def test_basic_reading_solomon(self):
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
                program = @@{cluster="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), False)
        assert succes, error

        # query using `program` param with enabled downsampling
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                program = @@{cluster="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "false",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), False)
        assert succes, error

        # query using `program` param with disabled downsampling
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                program = @@{cluster="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "true",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), True)
        assert succes, error

        # query using `selectors` param with enabled downsampling
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                selectors = @@{cluster="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "false",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), False)
        assert succes, error

        # query using `selectors` param with disabled downsampling
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                selectors = @@{cluster="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "true",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), True)
        assert succes, error

        # query using `labels` param
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                selectors = @@{cluster="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                labels = "test_type",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        result, error = self.execute_query(query)
        assert error is None, error
        assert any(column.name == "test_type" for column in result[0].columns)

        # query using `labels` param
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                selectors = @@{cluster="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                labels = "test_type as tt",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        result, error = self.execute_query(query)
        assert error is None, error
        assert any(column.name == "tt" for column in result[0].columns)

        # query with a single second interval
        query = """
            SELECT * FROM local_solomon.basic_reading WITH (
                selectors = @@{cluster="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:00:01Z"
            )
        """
        succes, error = self.check_query_result_size(*self.execute_query(query))
        assert succes, error

    @link_test_case("#23192")
    def test_basic_reading_monitoring(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "basic_reading",
                CLUSTER         = "basic_reading",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )"""
        result, error = self.execute_query(data_source_query)
        assert error is None

        # simplest query with default downsampling settings
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{folderId="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), False)
        assert succes, error

        # query using `program` param with enabled downsampling
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{folderId="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "false",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), False)
        assert succes, error

        # query using `program` param with disabled downsampling
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{folderId="basic_reading", service="my_service", test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "true",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), True)
        assert succes, error

        # query using `selectors` param with enabled downsampling
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "false",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), False)
        assert succes, error

        # query using `selectors` param with disabled downsampling
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="basic_reading_test"}@@,

                `downsampling.disabled` = "true",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        succes, error = self.check_query_result(*self.execute_query(query), True)
        assert succes, error

        # query using `labels` param
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="basic_reading_test"}@@,

                labels = "test_type",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        result, error = self.execute_query(query)
        assert error is None, error
        assert any(column.name == "test_type" for column in result[0].columns)

        # query using `labels` param
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="basic_reading_test"}@@,

                labels = "test_type as tt",

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            )
        """
        result, error = self.execute_query(query)
        assert error is None, error
        assert any(column.name == "tt" for column in result[0].columns)

        # query with a single second interval
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="basic_reading_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:00:01Z"
            )
        """
        succes, error = self.check_query_result_size(*self.execute_query(query))
        assert succes, error
