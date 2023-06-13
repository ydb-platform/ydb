UNITTEST_FOR(ydb/core/persqueue)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

SIZE(LARGE)

TAG(ya:fat)

TIMEOUT(3600)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    pq_ut.cpp
)

END()
