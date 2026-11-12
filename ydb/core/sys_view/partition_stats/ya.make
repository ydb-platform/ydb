YQL_LIBRARY()

SRCS(
    partition_stats.h
    partition_stats.cpp
    top_partitions.h
    top_partitions.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
