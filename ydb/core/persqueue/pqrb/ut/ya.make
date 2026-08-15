UNITTEST()

SIZE(MEDIUM)

SRCS(
    balancing_ut.cpp
    partitions_location_queue_ut.cpp
    scale_and_mirror_ut.cpp
    write_partition_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/pqrb
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/core/tx/tx_proxy
)

YQL_LAST_ABI_VERSION()

END()
