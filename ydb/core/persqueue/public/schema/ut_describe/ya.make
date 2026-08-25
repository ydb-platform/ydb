UNITTEST_FOR(ydb/core/persqueue/public/schema)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    describe_operation_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/testlib
    ydb/core/testlib/actors
    ydb/core/testlib/basics
    ydb/core/testlib/grpc_request
    ydb/core/tx/scheme_cache
    ydb/library/aclib
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    library/cpp/testing/unittest
    library/cpp/threading/future
)

ENV(INSIDE_YDB="1")

END()
