# -*- coding: utf-8 -*-
import os

from ydb.library.yql.tools.solomon_emulator.client.client import cleanup_solomon, add_solomon_metrics

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import ydb
from ydb.issues import GenericError


class SolomonReadingTestBase(object):
    @classmethod
    def setup_class(cls):
        cls.basic_reading_timestamps = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
        cls.basic_reading_values = [0, 1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12]

        cleanup_solomon("basic_reading", "my_cluster", "my_service")
        add_solomon_metrics("basic_reading", "my_cluster", "my_service", {"metrics": [
            {
                "labels"        : {"test_type": "basic_reading_test"},
                "type"          : "DGAUGE",
                "timestamps"    : cls.basic_reading_timestamps,
                "values"        : cls.basic_reading_values
            }
        ]})

        cls.solomon_http_endpoint = os.environ.get("SOLOMON_HTTP_ENDPOINT")
        cls.solomon_grpc_endpoint = os.environ.get("SOLOMON_GRPC_ENDPOINT")

        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_external_data_sources": True}
        )
        config.yaml_config["query_service_config"] = {}
        config.yaml_config["query_service_config"]["available_external_data_sources"] = ["Solomon"]
        config.yaml_config["query_service_config"]["solomon"] = {
            "default_settings": [
                {
                    "name": "_EnableReading",
                    "value": "true"
                },
                {
                    "name": "_EnableRuntimeListing",
                    "value": "true"
                }
            ]
        }

        cls.cluster = KiKiMR(config)
        cls.cluster.start()

        cls.endpoint = "%s:%s" % (
            cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
        )
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=cls.endpoint
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    def execute_query(self, query):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            try:
                res = session_pool.execute_with_retries(query)
                return (res, None)
            except GenericError as generic_error:
                return (None, generic_error)
