LIBRARY()

SET(
    SOURCE
    pool_stats_collector.cpp
)

SRCS(
    ${SOURCE}
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/helpers
    library/cpp/monlib/dynamic_counters
)

YQL_LAST_ABI_VERSION()

END()
