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
from ydb.tests.tools.fq_runner.kikimr_utils import start_kikimr


@pytest.fixture
def stats_mode():
    return ''


@pytest.fixture
def kikimr(request: pytest.FixtureRequest, yq_version: str, stats_mode: str):
    kikimr_extensions = [DefaultConfigExtension(""),
                         YQv2Extension(yq_version),
                         ComputeExtension(),
                         StatsModeExtension(stats_mode)]
    with start_kikimr(request, kikimr_extensions) as kikimr:
        yield kikimr


def create_client(kikimr, request):
    return FederatedQueryClient(request.param["folder_id"] if request is not None else "my_folder",
                                streaming_over_kikimr=kikimr)


@pytest.fixture
def client(kikimr, request=None):
    return create_client(kikimr, request)
