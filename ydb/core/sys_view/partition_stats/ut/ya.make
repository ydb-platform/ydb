UNITTEST_FOR(ydb/core/sys_view/partition_stats)
 
OWNER( 
    monster 
    g:kikimr 
) 
 
FORK_SUBTESTS() 

SIZE(MEDIUM) 

TIMEOUT(600) 
 
PEERDIR( 
    library/cpp/testing/unittest
    ydb/core/testlib
) 
 
YQL_LAST_ABI_VERSION()

SRCS( 
    partition_stats_ut.cpp 
) 
 
END() 
