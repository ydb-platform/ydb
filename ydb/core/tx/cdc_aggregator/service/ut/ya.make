UNITTEST_FOR(ydb/core/tx/cdc_aggregator/service)

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    heartbeat_queue_ut.cpp
    service_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
