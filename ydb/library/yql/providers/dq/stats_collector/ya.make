LIBRARY()

SET(
    SOURCE
    pool_stats_collector.cpp
)

SRCS(
    ${SOURCE}
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/helpers
    library/cpp/monlib/dynamic_counters
)

YQL_LAST_ABI_VERSION()

END()
