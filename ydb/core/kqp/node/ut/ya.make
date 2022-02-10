UNITTEST_FOR(ydb/core/kqp/node)
 
OWNER(g:kikimr) 
 
FORK_SUBTESTS() 
 
IF (SANITIZER_TYPE OR WITH_VALGRIND) 
    SIZE(MEDIUM) 
ENDIF() 
 
SRCS( 
    kqp_node_ut.cpp 
) 
 
PEERDIR( 
    ydb/core/kqp/ut/common
) 
 
YQL_LAST_ABI_VERSION() 
 
END() 
