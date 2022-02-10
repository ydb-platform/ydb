UNITTEST_FOR(ydb/core/viewer)

OWNER(g:kikimr) 
 
FORK_SUBTESTS() 
 
TIMEOUT(600) 

SIZE(MEDIUM) 
 
YQL_LAST_ABI_VERSION()
 
SRCS( 
    viewer_ut.cpp 
) 
 
PEERDIR(
    ydb/core/testlib
)

END() 
