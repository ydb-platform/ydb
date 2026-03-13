UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    library/cpp/json
    library/cpp/threading/blocking_queue
    ydb/core/audit
    ydb/core/persqueue/public/cloud_events
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_topic_cloud_events.cpp
)

END()
