UNITTEST()

SIZE(MEDIUM)

SRCS(
    partition_scale_manager_ut.cpp
    partitions_location_queue_ut.cpp
)

PEERDIR(
    contrib/restricted/abseil-cpp
    ydb/core/persqueue/pqrb
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/core/tx/tx_proxy
    ydb/public/sdk/cpp/src/client/topic
)

YQL_LAST_ABI_VERSION()

END()
