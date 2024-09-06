#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from ydb.tests.tools.fq_runner.custom_hooks import *  # noqa: F401,F403 Adding custom hooks for YQv2 support
from ydb.tests.tools.fq_runner.kikimr_utils import ExtensionPoint
from ydb.tests.tools.fq_runner.kikimr_utils import YQv2Extension
from ydb.tests.tools.fq_runner.kikimr_utils import ComputeExtension
from ydb.tests.tools.fq_runner.kikimr_utils import DefaultConfigExtension
from ydb.tests.tools.fq_runner.kikimr_utils import StatsModeExtension
from ydb.tests.tools.fq_runner.kikimr_utils import YdbMvpExtension
from ydb.tests.tools.fq_runner.kikimr_utils import start_kikimr


@pytest.fixture
def stats_mode():
    return ''


@pytest.fixture(scope="module")
def mvp_external_ydb_endpoint(request) -> str:
    return request.param["endpoint"] if request is not None and hasattr(request, 'param') else None


@pytest.fixture
def kikimr(request: pytest.FixtureRequest, yq_version: str, stats_mode: str, mvp_external_ydb_endpoint):
    kikimr_extensions = [
        DefaultConfigExtension(""),
        YQv2Extension(yq_version),
        ComputeExtension(),
        YdbMvpExtension(mvp_external_ydb_endpoint),
        StatsModeExtension(stats_mode),
    ]
    with start_kikimr(request, kikimr_extensions) as kikimr:
        yield kikimr


class ManyRetriesConfigExtension(ExtensionPoint):
    def __init__(self):
        super().__init__()

    def is_applicable(self, request):
        return True

    def apply_to_kikimr(self, request, kikimr):
        kikimr.compute_plane.fq_config['control_plane_storage']['retry_policy_mapping'] = [
            {'status_code': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], 'policy': {'retry_count': 10000}}
        ]


@pytest.fixture
def kikimr_many_retries(request: pytest.FixtureRequest, yq_version: str, mvp_external_ydb_endpoint):
    kikimr_extensions = [
        DefaultConfigExtension(""),
        ManyRetriesConfigExtension(),
        YQv2Extension(yq_version),
        YdbMvpExtension(mvp_external_ydb_endpoint),
        ComputeExtension(),
    ]
    with start_kikimr(request, kikimr_extensions) as kikimr:
        yield kikimr


def create_client(kikimr, request):
    return FederatedQueryClient(
        request.param["folder_id"] if request is not None else "my_folder", streaming_over_kikimr=kikimr
    )


@pytest.fixture
def client(kikimr, request=None):
    return create_client(kikimr, request)


@pytest.fixture
def client_many_retries(kikimr_many_retries, request=None):
    return create_client(kikimr_many_retries, request)
