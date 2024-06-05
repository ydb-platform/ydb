LIBRARY()

SRCS(
    pool_stats_collector.cpp
    pool_stats_collector.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/minikql

    ydb/core/base
    ydb/core/graph/api
    ydb/core/node_whiteboard

    library/cpp/monlib/dynamic_counters
)

END()
