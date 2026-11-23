YQL_LIBRARY()

SRCS(
    partition_stats.h
    partition_stats.cpp
    top_partitions.h
    top_partitions.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
