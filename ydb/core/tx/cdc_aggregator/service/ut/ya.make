UNITTEST_FOR(ydb/core/tx/cdc_aggregator/service)

SRCS(
    heartbeat_queue_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
