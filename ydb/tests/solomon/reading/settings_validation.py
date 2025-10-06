import logging

from .base import SolomonReadingTestBase
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


def extract_issue_messages(issue):
    result = issue.message
    for subissue in issue.issues:
        result += extract_issue_messages(subissue)
    return result


class TestSettingsValidation(SolomonReadingTestBase):
    def check_query_error(self, query, error_msg):
        result, error = self.execute_query(query)

        assert error is not None, "Query executed without errors, expecting to have at least one"
        assert error_msg in extract_issue_messages(error), "Expected to find specific error: {}, have errors: {}".format(error_msg, error)

    @link_test_case("#16385")
    def test_settings_validation_solomon_selectors(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_solomon WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )
        """
        result, error = self.execute_query(data_source_query)
        assert error is None

        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@
            )
        """
        self.check_query_error(query, "either program or selectors must be specified")

        # check `selectors` validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster=settings_validation, service=my_service, test_type=setting_validation}@@
            )
        """
        self.check_query_error(query, "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format")

        # check `selectors` validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@cluster="settings_validation", service="my_service", test_type="setting_validation"@@
            )
        """
        self.check_query_error(query, "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format")

        # check `selectors` validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@series_avg{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@
            )
        """
        self.check_query_error(query, "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format")

        # check `labels` validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                labels = "[test_type]"
            )
        """
        self.check_query_error(query, "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format")

        # check `labels` validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                labels = @@"test_type"@@
            )
        """
        self.check_query_error(query, "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format")

        # check `from` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                from = "invalid time"
            )
        """
        self.check_query_error(query, "couldn\'t parse `from`, use ISO8601 format")

        # check `to` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                to = "invalid time"
            )
        """
        self.check_query_error(query, "couldn\'t parse `to`, use ISO8601 format")

        # check `downsampling.disabled` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.disabled` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.disabled must be true or false, but has ABC")

        # check `downsampling.aggregation` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.aggregation` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.aggregation must be one of AVG, COUNT, DEFAULT_AGGREGATION, LAST, MAX, MIN, SUM, but has ABC")

        # check `downsampling.fill` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.fill` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.fill must be one of NONE, NULL, PREVIOUS, but has ABC")

        # check `downsampling.grid_interval` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.grid_interval` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.grid_interval must be positive number, but has ABC")

        # check unknown setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                unk = "ABC"
            )
        """
        self.check_query_error(query, "Unknown setting unk")

        # check additional downsampling settings validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.disabled` = "true",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15"
            )
        """
        self.check_query_error(query, "downsampling.disabled must be false if downsampling.aggregation, downsampling.fill or downsampling.grid_interval is specified")

        drop_source_query = """
            DROP EXTERNAL DATA SOURCE local_solomon;
        """
        result, error = self.execute_query(drop_source_query)
        assert error is None

    @link_test_case("#16385")
    def test_settings_validation_solomon_program(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_solomon WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )
        """
        result, error = self.execute_query(data_source_query)
        assert error is None

        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                selectors = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@
            )
        """
        self.check_query_error(query, "either program or selectors must be specified")

        # check `labels` validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                labels = "[test_type]"
            )
        """
        self.check_query_error(query, "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format")

        # check `labels` validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                labels = @@"test_type"@@
            )
        """
        self.check_query_error(query, "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format")

        # check `from` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                from = "invalid time"
            )
        """
        self.check_query_error(query, "couldn\'t parse `from`, use ISO8601 format")

        # check `to` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                to = "invalid time"
            )
        """
        self.check_query_error(query, "couldn\'t parse `to`, use ISO8601 format")

        # check `downsampling.disabled` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.disabled` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.disabled must be true or false, but has ABC")

        # check `downsampling.aggregation` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.aggregation` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.aggregation must be one of AVG, COUNT, DEFAULT_AGGREGATION, LAST, MAX, MIN, SUM, but has ABC")

        # check `downsampling.fill` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.fill` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.fill must be one of NONE, NULL, PREVIOUS, but has ABC")

        # check `downsampling.grid_interval` setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.grid_interval` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.grid_interval must be positive number, but has ABC")

        # check unknown setting validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                unk = "ABC"
            )
        """
        self.check_query_error(query, "Unknown setting unk")

        # check additional downsampling settings validation
        query = """
            SELECT * FROM local_solomon.settings_validation WITH (
                program = @@{cluster="settings_validation", service="my_service", test_type="setting_validation"}@@,
                `downsampling.disabled` = "true",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15"
            )
        """
        self.check_query_error(query, "downsampling.disabled must be false if downsampling.aggregation, downsampling.fill or downsampling.grid_interval is specified")

        drop_source_query = """
            DROP EXTERNAL DATA SOURCE local_solomon;
        """
        result, error = self.execute_query(drop_source_query)
        assert error is None

    @link_test_case("#23189")
    def test_settings_validation_monitoring_selectors(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "settings_validation",
                CLUSTER         = "settings_validation",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )
        """
        result, error = self.execute_query(data_source_query)
        assert error is None

        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                selectors = @@{test_type="setting_validation"}@@
            )
        """
        self.check_query_error(query, "either program or selectors must be specified")

        # check `selectors` validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type=setting_validation}@@
            )
        """
        self.check_query_error(query, "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format")

        # check `selectors` validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@test_type="setting_validation"@@
            )
        """
        self.check_query_error(query, "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format")

        # check `selectors` validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@series_avg{test_type="setting_validation"}@@
            )
        """
        self.check_query_error(query, "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format")

        # check `labels` validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                labels = "[test_type]"
            )
        """
        self.check_query_error(query, "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format")

        # check `labels` validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                labels = @@"test_type"@@
            )
        """
        self.check_query_error(query, "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format")

        # check `from` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                from = "invalid time"
            )
        """
        self.check_query_error(query, "couldn\'t parse `from`, use ISO8601 format")

        # check `to` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                to = "invalid time"
            )
        """
        self.check_query_error(query, "couldn\'t parse `to`, use ISO8601 format")

        # check `downsampling.disabled` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                `downsampling.disabled` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.disabled must be true or false, but has ABC")

        # check `downsampling.aggregation` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                `downsampling.aggregation` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.aggregation must be one of AVG, COUNT, DEFAULT_AGGREGATION, LAST, MAX, MIN, SUM, but has ABC")

        # check `downsampling.fill` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                `downsampling.fill` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.fill must be one of NONE, NULL, PREVIOUS, but has ABC")

        # check `downsampling.grid_interval` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                `downsampling.grid_interval` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.grid_interval must be positive number, but has ABC")

        # check unknown setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                unk = "ABC"
            )
        """
        self.check_query_error(query, "Unknown setting unk")

        # check additional downsampling settings validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                selectors = @@{test_type="setting_validation"}@@,
                `downsampling.disabled` = "true",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15"
            )
        """
        self.check_query_error(query, "downsampling.disabled must be false if downsampling.aggregation, downsampling.fill or downsampling.grid_interval is specified")

        drop_source_query = """
            DROP EXTERNAL DATA SOURCE local_monitoring;
        """
        result, error = self.execute_query(drop_source_query)
        assert error is None

    @link_test_case("#23189")
    def test_settings_validation_monitoring_program(self):
        data_source_query = f"""
            CREATE EXTERNAL DATA SOURCE local_monitoring WITH (
                SOURCE_TYPE     = "Solomon",
                LOCATION        = "{self.solomon_http_endpoint}",
                GRPC_LOCATION   = "{self.solomon_grpc_endpoint}",
                PROJECT         = "settings_validation",
                CLUSTER         = "settings_validation",
                AUTH_METHOD     = "NONE",
                USE_TLS         = "false"
            )
        """
        result, error = self.execute_query(data_source_query)
        assert error is None

        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                selectors = @@{test_type="setting_validation"}@@
            )
        """
        self.check_query_error(query, "either program or selectors must be specified")

        # check `labels` validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                labels = "[test_type]"
            )
        """
        self.check_query_error(query, "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format")

        # check `labels` validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                labels = @@"test_type"@@
            )
        """
        self.check_query_error(query, "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format")

        # check `from` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                from = "invalid time"
            )
        """
        self.check_query_error(query, "couldn\'t parse `from`, use ISO8601 format")

        # check `to` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                to = "invalid time"
            )
        """
        self.check_query_error(query, "couldn\'t parse `to`, use ISO8601 format")

        # check `downsampling.disabled` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                `downsampling.disabled` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.disabled must be true or false, but has ABC")

        # check `downsampling.aggregation` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                `downsampling.aggregation` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.aggregation must be one of AVG, COUNT, DEFAULT_AGGREGATION, LAST, MAX, MIN, SUM, but has ABC")

        # check `downsampling.fill` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                `downsampling.fill` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.fill must be one of NONE, NULL, PREVIOUS, but has ABC")

        # check `downsampling.grid_interval` setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                `downsampling.grid_interval` = "ABC"
            )
        """
        self.check_query_error(query, "downsampling.grid_interval must be positive number, but has ABC")

        # check unknown setting validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                unk = "ABC"
            )
        """
        self.check_query_error(query, "Unknown setting unk")

        # check additional downsampling settings validation
        query = """
            SELECT * FROM local_monitoring.my_service WITH (
                program = @@{test_type="setting_validation"}@@,
                `downsampling.disabled` = "true",
                `downsampling.aggregation` = "AVG",
                `downsampling.fill` = "PREVIOUS",
                `downsampling.grid_interval` = "15"
            )
        """
        self.check_query_error(query, "downsampling.disabled must be false if downsampling.aggregation, downsampling.fill or downsampling.grid_interval is specified")

        drop_source_query = """
            DROP EXTERNAL DATA SOURCE local_monitoring;
        """
        result, error = self.execute_query(drop_source_query)
        assert error is None
