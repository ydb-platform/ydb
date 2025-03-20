LIBRARY()

SRCS(
    scan.h
    scan.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/auth
    ydb/core/sys_view/common
    ydb/core/sys_view/nodes
    ydb/core/sys_view/partition_stats
    ydb/core/sys_view/pg_tables
    ydb/core/sys_view/query_stats
    ydb/core/sys_view/resource_pool_classifiers
    ydb/core/sys_view/resource_pools
    ydb/core/sys_view/service
    ydb/core/sys_view/sessions
    ydb/core/sys_view/show_create
    ydb/core/sys_view/storage
    ydb/core/sys_view/tablets
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    auth
    common
    nodes
    partition_stats
    pg_tables
    processor
    query_stats
    resource_pool_classifiers
    resource_pools
    service
    storage
    tablets
)

RECURSE_FOR_TESTS(
    ut
    ut_large
)
