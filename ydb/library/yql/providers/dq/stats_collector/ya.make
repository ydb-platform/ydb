LIBRARY()

SET(
    SOURCE
    pool_stats_collector.cpp
)

SRCS(
    ${SOURCE}
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/helpers
    library/cpp/monlib/dynamic_counters
)

YQL_LAST_ABI_VERSION()

END()
