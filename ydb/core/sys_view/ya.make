LIBRARY()

SRCS(
    scan.h
    scan.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
    ydb/core/sys_view/nodes
    ydb/core/sys_view/sessions
    ydb/core/sys_view/partition_stats
    ydb/core/sys_view/pg_tables
    ydb/core/sys_view/query_stats
    ydb/core/sys_view/service
    ydb/core/sys_view/storage
    ydb/core/sys_view/tablets
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    common
    nodes
    partition_stats
    pg_tables
    processor
    query_stats
    service
    storage
    tablets
)

RECURSE_FOR_TESTS(
    ut_kqp
)
