UNITTEST_FOR(ydb/services/persqueue_v1/actors/schema/topic)

SIZE(MEDIUM)
FORK_SUBTESTS()

YQL_LAST_ABI_VERSION()

SRCS(
    schema_ops_ut.cpp
    reset_offset_actor_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/testlib/actors
    ydb/core/testlib/grpc_request
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/services/persqueue_v1/actors
    library/cpp/testing/unittest
    library/cpp/threading/future
)

ENV(INSIDE_YDB="1")

END()
