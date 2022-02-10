UNITTEST_FOR(ydb/core/health_check) 
 
OWNER(g:kikimr)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    ydb/core/testlib 
)

SRCS(
    health_check_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
