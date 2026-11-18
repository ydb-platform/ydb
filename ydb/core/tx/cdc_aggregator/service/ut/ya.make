UNITTEST_FOR(ydb/core/tx/cdc_aggregator/service)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx/cdc_aggregator/tablet
)

SRCS(
    cdc_aggregator_heartbeat_delivery_ut.cpp
    service_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
