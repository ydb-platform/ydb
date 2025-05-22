UNITTEST_FOR(ydb/core/persqueue)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SPLIT_FACTOR(5)
SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils

    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    autopartitioning_ut.cpp
    pq_ut.cpp
)

END()
