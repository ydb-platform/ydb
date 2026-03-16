#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import os
from contextlib import contextmanager
from logging import getLogger
from pathlib import Path
from typing import Generator

import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.connector.compat import IS_LINUX
from snowflake.connector.telemetry import TelemetryClient, TelemetryData

from . import (
    CLOUD_PROVIDERS,
    EXTERNAL_SKIP_TAGS,
    INTERNAL_SKIP_TAGS,
    running_on_public_ci,
)


class TelemetryCaptureHandler(TelemetryClient):
    def __init__(
        self,
        real_telemetry: TelemetryClient,
        propagate: bool = True,
    ):
        super().__init__(real_telemetry._rest)
        self.records: list[TelemetryData] = []
        self._real_telemetry = real_telemetry
        self._propagate = propagate

    def add_log_to_batch(self, telemetry_data):
        self.records.append(telemetry_data)
        if self._propagate:
            super().add_log_to_batch(telemetry_data)

    def send_batch(self):
        self.records = []
        if self._propagate:
            super().send_batch()


class TelemetryCaptureFixture:
    """Provides a way to capture Snowflake telemetry messages."""

    @contextmanager
    def patch_connection(
        self,
        con: SnowflakeConnection,
        propagate: bool = True,
    ) -> Generator[TelemetryCaptureHandler, None, None]:
        original_telemetry = con._telemetry
        new_telemetry = TelemetryCaptureHandler(
            original_telemetry,
            propagate,
        )
        con._telemetry = new_telemetry
        try:
            yield new_telemetry
        finally:
            con._telemetry = original_telemetry


@pytest.fixture(scope="session")
def capture_sf_telemetry() -> TelemetryCaptureFixture:
    return TelemetryCaptureFixture()


def pytest_collection_modifyitems(items) -> None:
    """Applies tags to tests based on folders that they are in."""
    for item in items:
        item.add_marker("skipolddriver")


@pytest.fixture(scope="session", autouse=True)
def filter_log() -> None:
    """Sets up our SecretDetector as a logging formatter.

    A workaround to use our custom Formatter in pytest based on the discussion at
    https://github.com/pytest-dev/pytest/issues/2987
    """
    import logging
    import pathlib

    from snowflake.connector.secret_detector import SecretDetector

    if not isinstance(SecretDetector, logging.Formatter):
        # Override it if SecretDetector is not an instance of logging.Formatter
        class SecretDetector(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                return super().format(record)

    log_dir = os.getenv(
        "CLIENT_LOG_DIR_PATH_DOCKER", str(pathlib.Path(__file__).parent.absolute())
    )

    _logger = getLogger("snowflake.connector")
    original_log_level = _logger.getEffectiveLevel()
    # Make sure that the old handlers are unaffected by the DEBUG level set for the new handler
    for handler in _logger.handlers:
        handler.setLevel(original_log_level)
    _logger.setLevel(logging.DEBUG)
    sd = logging.FileHandler(os.path.join(log_dir, "", "..", "snowflake_ssm_rt.log"))
    sd.setLevel(logging.DEBUG)
    sd.setFormatter(
        SecretDetector(
            "%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s"
        )
    )
    _logger.addHandler(sd)


def pytest_runtest_setup(item) -> None:
    """Ran before calling each test, used to decide whether a test should be skipped."""
    test_tags = [mark.name for mark in item.iter_markers()]

    # Get what cloud providers the test is marked for if any
    test_supported_providers = CLOUD_PROVIDERS.intersection(test_tags)
    # Default value means that we are probably running on a developer's machine, allow everything in this case
    current_provider = os.getenv("cloud_provider", "dev")
    if test_supported_providers:
        # If test is tagged for specific cloud providers add the default cloud_provider as supported too
        test_supported_providers.add("dev")
        if current_provider not in test_supported_providers:
            pytest.skip(
                "cannot run unit test against cloud provider {}".format(
                    current_provider
                )
            )
    if EXTERNAL_SKIP_TAGS.intersection(test_tags) and (
        not IS_LINUX or running_on_public_ci()
    ):
        pytest.skip("cannot run this test on public Snowflake deployment")
    elif INTERNAL_SKIP_TAGS.intersection(test_tags) and not running_on_public_ci():
        pytest.skip("cannot run this test on private Snowflake deployment")


def pytest_configure(config):
    from_tox_ini = """# Optional dependency groups markers
    lambda: AWS lambda tests
    pandas: tests for pandas integration
    sso: tests for sso optional dependency integration
    # Cloud provider markers
    aws: tests for Amazon Cloud storage
    azure: tests for Azure Cloud storage
    gcp: tests for Google Cloud storage
    # Test type markers
    integ: integration tests
    unit: unit tests
    skipolddriver: skip for old driver tests
    # Other markers
    timeout: tests that need a timeout time
    internal: tests that could but should only run on our internal CI
    external: tests that could but should only run on our external CI
    """
    for line in from_tox_ini.split("\n"):
        line = line.strip()
        if len(line) == 0 or line.startswith("#"):
            continue
        if ": " not in line:
            continue

        config.addinivalue_line("markers", line)

    config.addinivalue_line("markers", "flaky: flaky")
