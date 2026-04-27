UNITTEST_FOR(ydb/services/persqueue_v1/actors/schema/pqv1)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    create_topic_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query
    library/cpp/testing/unittest
)

ENV(INSIDE_YDB="1")

END()
