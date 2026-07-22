GTEST()

SIZE(MEDIUM)

SRCS(
    partitions_location_queue_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
