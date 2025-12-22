# -*- coding: utf-8 -*-
import logging
import copy

import pytest

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels


logger = logging.getLogger(__name__)


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # more logs
        'TX_PROXY': LogLevels.DEBUG,
        # less logs
        'KQP_PROXY': LogLevels.CRIT,
        'KQP_WORKER': LogLevels.CRIT,
        'KQP_GATEWAY': LogLevels.CRIT,
        'GRPC_PROXY': LogLevels.ERROR,
        'TX_DATASHARD': LogLevels.ERROR,
        'TX_PROXY_SCHEME_CACHE': LogLevels.ERROR,
        'KQP_YQL': LogLevels.ERROR,
        'KQP_SESSION': LogLevels.CRIT,
        'KQP_COMPILE_ACTOR': LogLevels.CRIT,
        'PERSQUEUE_CLUSTER_TRACKER': LogLevels.CRIT,
        'SCHEME_BOARD_SUBSCRIBER': LogLevels.CRIT,
        'SCHEME_BOARD_POPULATOR': LogLevels.CRIT,
    },
    # do not clutter logs with resource pools auto creation
    enable_resource_pools=False,
)


@pytest.fixture(scope='module', params=[True, False], ids=['enable_describe_from_scheme_cache--true', 'enable_describe_from_scheme_cache--false'])
def enable_describe_from_scheme_cache(request):
    return request.param


# fixtures.ydb_cluster_configuration local override
@pytest.fixture(scope='module')
def ydb_cluster_configuration(enable_describe_from_scheme_cache):
    conf = copy.deepcopy(CLUSTER_CONFIG)
    conf['enable_describe_from_scheme_cache'] = enable_describe_from_scheme_cache
    return conf


@pytest.mark.parametrize('shard_count', [1, 4])
def test_cached_shard_key_bounds(ydb_database, ydb_client_session, shard_count):
    database_path = ydb_database
    pool = ydb_client_session(database_path)

    test_table_path = f'{database_path}/table'

    partitioning_policy = (
        ydb.PartitioningPolicy()
        .with_auto_partitioning(
            ydb.AutoPartitioningPolicy.AUTO_SPLIT_MERGE,
        )
    )

    if shard_count > 1:
        partitioning_policy.with_uniform_partitions(shard_count)

    with pool.checkout() as session:
        session.create_table(test_table_path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Json))
            )
            .with_primary_keys(
                'key',
            )
            .with_profile(
                ydb.TableProfile()
                .with_partitioning_policy(partitioning_policy)
            )
        )

        describe_settings = (
            ydb.DescribeTableSettings()
            .with_include_table_stats(True)
            .with_include_shard_key_bounds(True)
        )

        ordinary = session.describe_table(test_table_path, describe_settings)

        cached = session.describe_table(test_table_path, describe_settings.with_return_cached_result(True))

    # ordinary version
    assert len(ordinary.shard_key_ranges) == shard_count
    assert ordinary.table_stats is not None

    # cached version
    assert len(cached.shard_key_ranges) == shard_count
    assert cached.table_stats is not None

    #NOTE: cached version must differ in that it should lack table_stats.partition_stats
    # but currently there is no way to request partition stats with python sdk
    # if enable_describe_from_scheme_cache:
    #     assert cached.table_stats.partition_stats is None
    # else:
    #     assert cached.table_stats.partition_stats is not None

    # shard_key_ranges in both versions are the same
    assert len(ordinary.shard_key_ranges) == len(cached.shard_key_ranges)
    for i in range(len(ordinary.shard_key_ranges)):
        # KeyRange doesn't have equality operator
        assert repr(ordinary.shard_key_ranges[i]) == repr(cached.shard_key_ranges[i])
