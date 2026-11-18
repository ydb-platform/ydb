#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid

import pytest

from ydb.tests.fq.yds.test_base import YdbTopics
from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config

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


@pytest.fixture(scope='module')
def ydb_cluster(request):
    # Kept separate from kikimr: unported tests still need the YQ environment.
    param = getattr(request, 'param', {})
    with pytest.MonkeyPatch.context() as environment:
        if 'rebalancing_timeout_ms' in param:
            environment.setenv('YDB_TEST_ROW_DISPATCHER_REBALANCING_TIMEOUT_MS', str(param['rebalancing_timeout_ms']))
        config = get_ydb_config(request)
        config.yaml_config['feature_flags']['enable_streaming_query_disposition'] = True
        if 'aggregation_compute_threads' in param:
            config.yaml_config['table_service_config'].setdefault('aggregation_config', {})[
                'aggregation_compute_threads'
            ] = param['aggregation_compute_threads']
        if param.get('distributed_compute'):
            # Match the distributed aggregation setup in KQP's unit tests.
            # Small recovery queries must have workers outside the coordinator node.
            resources = config.yaml_config['table_service_config'].setdefault('resource_manager', {})
            resources['max_non_parallel_tasks_execution_limit'] = 0
            resources['max_non_parallel_top_stage_execution_limit'] = 0
        compute_nodes = getattr(request, 'param', {}).get('compute_nodes', 2)
        cluster = Kikimr(config)
        # Negative SQL tests must expose the original error, even if it retires
        # the session. Retrying INTERNAL_ERROR can mask it with BAD_SESSION.
        cluster.ydb_client.retry_settings.retry_internal_error = False
        try:
            if compute_nodes > 2:
                cluster.cluster.register_and_start_slots(database=cluster.get_database_name(), count=compute_nodes - 2)
            cluster.wait_kqp_node_count(compute_nodes)
            yield cluster
        finally:
            cluster.stop()


@pytest.fixture
def ydb_topics(ydb_cluster):
    topics = []

    def create(name, *, partitions=1, shared=False):
        io = YdbTopics(ydb_cluster, name + '_' + uuid.uuid4().hex, partitions=partitions, shared=shared)
        topics.append(io)
        return io

    try:
        yield create
    finally:
        for io in reversed(topics):
            io.close()
