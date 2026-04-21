UNITTEST_FOR(ydb/core/persqueue/public/cloud_events)

YQL_LAST_ABI_VERSION()

PEERDIR(
    ydb/core/audit
    ydb/core/persqueue/public/cloud_events
    ydb/core/persqueue/events
    ydb/core/protos
    library/cpp/json
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

SRCS(
    cloud_events_ut.cpp
)

END()
