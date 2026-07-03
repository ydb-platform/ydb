UNITTEST_FOR(ydb/services/deprecated/persqueue_v0)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    grpc_dead_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/client/server
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/services/deprecated/persqueue_v0
    ydb/services/deprecated/persqueue_v0/api/grpc
)


YQL_LAST_ABI_VERSION()

END()
