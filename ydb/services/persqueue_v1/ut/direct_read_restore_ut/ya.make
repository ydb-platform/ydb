UNITTEST_FOR(ydb/services/persqueue_v1)

ADDINCL(
    ydb/public/sdk/cpp
)

CFLAGS(
    -DYDB_SDK_USE_STD_STRING
)

SIZE(MEDIUM)
TIMEOUT(600)

ENV(PQ_EXPERIMENTAL_DIRECT_READ="1")

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    ydb/core/persqueue/events
    ydb/core/testlib/default
    ydb/core/testlib/actors
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/services/persqueue_v1
)

SRCS(
    direct_read_restore_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
