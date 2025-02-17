UNITTEST_FOR(ydb/services/datastreams)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/client/topic
    ydb/services/datastreams

    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
