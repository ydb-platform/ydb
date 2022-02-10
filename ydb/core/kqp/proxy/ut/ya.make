UNITTEST_FOR(ydb/core/kqp/proxy)
 
OWNER(g:kikimr) 
 
FORK_SUBTESTS() 
 
SIZE(MEDIUM) 
 
SRCS( 
    kqp_proxy_ut.cpp 
) 
 
PEERDIR( 
    ydb/core/kqp/proxy
    ydb/core/kqp/ut/common
) 
 
YQL_LAST_ABI_VERSION()
 
END() 
