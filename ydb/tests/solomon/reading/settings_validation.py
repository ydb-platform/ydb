import logging

from .base import SolomonReadingTestBase
from ydb.tests.library.test_meta import link_test_case

import ydb
from ydb.issues import GenericError

logger = logging.getLogger(__name__)


def extract_issue_messages(issue):
    result = issue.message
    for subissue in issue.issues:
        result += extract_issue_messages(subissue)
    return result


class TestSettingsValidation(SolomonReadingTestBase):
    def check_query_error(self, query, error_msg):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            error = None
            try:
                session_pool.execute_with_retries(query)
            except GenericError as generic_error:
                error = generic_error

            assert error is not None
            assert error_msg in extract_issue_messages(error)

    @link_test_case("#16385")
    def test_settings_validation(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            data_source_query = f"""
                CREATE EXTERNAL DATA SOURCE local_solomon WITH (
                    SOURCE_TYPE     = "Solomon",
                    LOCATION        = "{self.solomon_http_endpoint}",
                    GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                    AUTH_METHOD     = "NONE",
                    USE_TLS         = "false"
                )"""
            session_pool.execute_with_retries(data_source_query)

        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program=@@{}@@,
                selectors=@@{}@@
            );
        """
        self.check_query_error(query, "either program or selectors must be specified")

        # check `selectors` validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                selectors=@@invalid selectors@@
            );
        """
        self.check_query_error(query, "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format")

        # check `from` setting validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program=@@{}@@,
                from="invalid time"
            );
        """
        self.check_query_error(query, "couldn\'t parse `from`, use Iso8601 format")

        # check `to` setting validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program=@@{}@@,
                to="invalid time"
            );
        """
        self.check_query_error(query, "couldn\'t parse `to`, use Iso8601 format")

        # check `downsampling.disabled` setting validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program = @@{}@@,
                `downsampling.disabled` = "ABC"
            );
        """
        self.check_query_error(query, "downsampling.disabled must be true or false, but has ABC")

        # check `downsampling.aggregation` setting validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program = @@{}@@,
                `downsampling.aggregation` = "ABC"
            );
        """
        self.check_query_error(query, "downsampling.aggregation must be one of AVG, COUNT, DEFAULT_AGGREGATION, LAST, MAX, MIN, SUM, but has ABC")

        # check `downsampling.fill` setting validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program = @@{}@@,
                `downsampling.fill` = "ABC"
            );
        """
        self.check_query_error(query, "downsampling.fill must be one of NONE, NULL, PREVIOUS, but has ABC")

        # check `downsampling.grid_interval` setting validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program = @@{}@@,
                `downsampling.grid_interval` = "ABC"
            );
        """
        self.check_query_error(query, "downsampling.grid_interval must be positive number, but has ABC")

        # check unknown setting validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program = @@{}@@,
                unk = "ABC"
            );
        """
        self.check_query_error(query, "Unknown setting unk")

        # check additional downsampling settings validation
        query = """
            SELECT * FROM local_solomon.my_project WITH (
                program = @@{}@@,
                `downsampling.disabled` = "true",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15"
            );
        """
        self.check_query_error(query, "downsampling.disabled must be false if downsampling.aggregation, downsampling.fill or downsamplig.grid_interval is specified")
