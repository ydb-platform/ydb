UNITTEST_FOR(ydb/core/tx/cdc_aggregator/tablet)

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    cdc_aggregator_heartbeat_publication_ut.cpp
    tablet_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
