UNITTEST_FOR(ydb/core/persqueue)

OWNER(
    alexnick
    g:kikimr
    g:logbroker
)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

SIZE(LARGE)

TAG(ya:fat)

TIMEOUT(3600)

PEERDIR(
    library/cpp/getopt 
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

SRCS(
    pq_ut_slow.cpp
    pq_ut.h
)

END()
