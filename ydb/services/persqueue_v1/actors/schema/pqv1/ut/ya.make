UNITTEST_FOR(ydb/services/persqueue_v1/actors/schema/pqv1)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    alter_topic_sdk_ut.cpp
    create_topic_ut.cpp
    create_topic_sdk_ut.cpp
    pqv1_sdk_test_utils.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/testlib/grpc_request
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    library/cpp/testing/unittest
)

ENV(INSIDE_YDB="1")

END()
