#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pytest
import requests
import yatest.common

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.custom_hooks import *  # noqa: F401,F403 Adding custom hooks for YQv2 support
from ydb.tests.tools.fq_runner.kikimr_utils import AddInflightExtension
from ydb.tests.tools.fq_runner.kikimr_utils import AddDataInflightExtension
from ydb.tests.tools.fq_runner.kikimr_utils import AddFormatSizeLimitExtension
from ydb.tests.tools.fq_runner.kikimr_utils import DefaultConfigExtension
from ydb.tests.tools.fq_runner.kikimr_utils import YQv2Extension
from ydb.tests.tools.fq_runner.kikimr_utils import ComputeExtension
from ydb.tests.tools.fq_runner.kikimr_utils import StatsModeExtension
from ydb.tests.tools.fq_runner.kikimr_utils import BindingsModeExtension
from ydb.tests.tools.fq_runner.kikimr_utils import YdbMvpExtension
from ydb.tests.tools.fq_runner.kikimr_utils import start_kikimr
from ydb.tests.tools.fq_runner.kikimr_utils import YQV1_VERSION_NAME, YQV2_VERSION_NAME
from ydb.tests.fq.s3.s3_helpers import S3
from library.recipes import common as recipes_common


MOTO_SERVER_PATH = "contrib/python/moto/bin/moto_server"
S3_PID_FILE = "s3.pid"


class TestCounter:
    def __init__(self, tests_count_limit, error_string):
        self.tests_count_limit = tests_count_limit
        self.error_string = error_string
        self.number_tests = 0

    def on_test_start(self):
        self.number_tests += 1
        assert self.number_tests <= self.tests_count_limit, (
            f"{self.error_string} exceeded limit {self.number_tests} vs {self.tests_count_limit}, "
            "this may lead timeouts on CI, please split this file"
        )


@pytest.fixture(scope="module")
def mvp_external_ydb_endpoint(request) -> str:
    return request.param["endpoint"] if request is not None and hasattr(request, 'param') else None


@pytest.fixture(scope="module")
def s3(request) -> S3:
    port_manager = yatest.common.network.PortManager()
    s3_port = port_manager.get_port()
    s3_url = "http://localhost:{port}".format(port=s3_port)

    command = [yatest.common.binary_path(MOTO_SERVER_PATH), "s3", "--host", "::1", "--port", str(s3_port)]

    def is_s3_ready():
        try:
            response = requests.get(s3_url)
            response.raise_for_status()
            return True
        except requests.RequestException as err:
            logging.debug(err)
            return False

    recipes_common.start_daemon(
        command=command, environment=None, is_alive_check=is_s3_ready, pid_file_name=S3_PID_FILE
    )

    try:
        yield S3(s3_url)
    finally:
        with open(S3_PID_FILE, 'r') as f:
            pid = int(f.read())
        recipes_common.stop_daemon(pid)


@pytest.fixture(scope="module")
def kikimr_settings(request: pytest.FixtureRequest):
    return getattr(request, "param", dict())


@pytest.fixture(scope="module")
def kikimr_params(request: pytest.FixtureRequest):
    return request


def get_kikimr_extensions(s3: S3, yq_version: str, kikimr_settings, mvp_external_ydb_endpoint):
    return [
        AddInflightExtension(),
        AddDataInflightExtension(),
        AddFormatSizeLimitExtension(),
        DefaultConfigExtension(s3.s3_url),
        YQv2Extension(yq_version, kikimr_settings.get("is_replace_if_exists", False)),
        ComputeExtension(),
        YdbMvpExtension(mvp_external_ydb_endpoint),
        StatsModeExtension(kikimr_settings.get("stats_mode", "")),
        BindingsModeExtension(kikimr_settings.get("bindings_mode", ""), yq_version),
    ]


@pytest.fixture(scope="module")
def kikimr_starts_counter():
    return TestCounter(10, "Number kikimr restarts in one module")


@pytest.fixture(scope="module")
def kikimr_yqv1(
    kikimr_params: pytest.FixtureRequest, s3: S3, kikimr_settings, mvp_external_ydb_endpoint, kikimr_starts_counter
):
    kikimr_starts_counter.on_test_start()
    kikimr_extensions = get_kikimr_extensions(s3, YQV1_VERSION_NAME, kikimr_settings, mvp_external_ydb_endpoint)
    with start_kikimr(kikimr_params, kikimr_extensions) as kikimr:
        yield kikimr


@pytest.fixture(scope="module")
def kikimr_yqv2(
    kikimr_params: pytest.FixtureRequest, s3: S3, kikimr_settings, mvp_external_ydb_endpoint, kikimr_starts_counter
):
    kikimr_starts_counter.on_test_start()
    kikimr_extensions = get_kikimr_extensions(s3, YQV2_VERSION_NAME, kikimr_settings, mvp_external_ydb_endpoint)
    with start_kikimr(kikimr_params, kikimr_extensions) as kikimr:
        yield kikimr


@pytest.fixture
def kikimr(yq_version: str, kikimr_yqv1, kikimr_yqv2):
    kikimr = None
    if yq_version == YQV1_VERSION_NAME:
        kikimr = kikimr_yqv1
    elif yq_version == YQV2_VERSION_NAME:
        kikimr = kikimr_yqv2

    if kikimr is not None:
        kikimr.control_plane.drop_metering()
        kikimr.control_plane.ensure_is_alive()
        kikimr.compute_plane.ensure_is_alive()

    return kikimr


@pytest.fixture(scope="module")
def tests_counter():
    return TestCounter(200, "Number tests in one module")


@pytest.fixture
def client(kikimr, tests_counter, request=None):
    tests_counter.on_test_start()
    client = FederatedQueryClient(
        request.param["folder_id"] if request is not None else "my_folder", streaming_over_kikimr=kikimr
    )
    yield client

    kikimr.control_plane.ensure_is_alive()
    kikimr.compute_plane.ensure_is_alive()


@pytest.fixture
def unique_prefix(request: pytest.FixtureRequest):
    name_hash = abs(hash(request.node.name))
    return f"h{name_hash}_{request.function.__name__}"
