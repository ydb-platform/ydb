import pytest
import yatest.common
import requests
import logging

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.custom_hooks import *  # noqa: F401,F403 Adding custom hooks for YQv2 support
from ydb.tests.tools.fq_runner.kikimr_utils import ConnectorExtension
from ydb.tests.tools.fq_runner.kikimr_utils import YQv2Extension
from ydb.tests.tools.fq_runner.kikimr_utils import TokenAccessorExtension
from ydb.tests.tools.fq_runner.kikimr_utils import MDBExtension
from ydb.tests.tools.fq_runner.kikimr_utils import YdbMvpExtension
from ydb.tests.tools.fq_runner.kikimr_utils import DefaultConfigExtension
from ydb.tests.tools.fq_runner.kikimr_utils import start_kikimr

from ydb.tests.fq.generic.utils.settings import Settings
from ydb.tests.fq.generic.analytics.s3_helpers import S3
from library.recipes import common as recipes_common

from typing import Final

docker_compose_file_path: Final = "ydb/tests/fq/generic/analytics/docker-compose.yml"


MOTO_SERVER_PATH = "contrib/python/moto/bin/moto_server"
S3_PID_FILE = "s3.pid"


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(docker_compose_file_path=docker_compose_file_path)


@pytest.fixture
def mvp_external_ydb_endpoint(request) -> str:
    return request.param["endpoint"] if request is not None and hasattr(request, "param") else None


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


@pytest.fixture
def kikimr(s3: S3, request: pytest.FixtureRequest, settings: Settings, yq_version: str, mvp_external_ydb_endpoint: str):
    kikimr_extensions = [
        ConnectorExtension(settings.connector.grpc_host, settings.connector.grpc_port, False),
        TokenAccessorExtension(settings.token_accessor_mock.endpoint, settings.token_accessor_mock.hmac_secret_file),
        MDBExtension(settings.mdb_mock.endpoint),
        YdbMvpExtension(mvp_external_ydb_endpoint),
        YQv2Extension(yq_version),
        DefaultConfigExtension(s3.s3_url),
    ]
    with start_kikimr(request, kikimr_extensions) as kikimr:
        yield kikimr


@pytest.fixture
def fq_client(kikimr, request=None) -> FederatedQueryClient:
    client = FederatedQueryClient(
        request.param["folder_id"] if request is not None else "my_folder", streaming_over_kikimr=kikimr
    )
    return client


@pytest.fixture
def unique_prefix(request: pytest.FixtureRequest):
    name_hash = abs(hash(request.node.name))
    return f"h{name_hash}_{request.function.__name__}"
