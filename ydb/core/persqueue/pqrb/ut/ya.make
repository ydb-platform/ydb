UNITTEST()

SIZE(MEDIUM)

SRCS(
    balancing_ut.cpp
    partitions_location_queue_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
)

YQL_LAST_ABI_VERSION()

END()
