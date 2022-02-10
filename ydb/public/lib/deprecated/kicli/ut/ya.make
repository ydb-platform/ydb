UNITTEST_FOR(ydb/public/lib/deprecated/kicli)
 
OWNER(
    xenoxeno
    g:kikimr
)
 
TIMEOUT(600)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR( 
    ydb/core/client
    ydb/core/testlib
    ydb/public/lib/deprecated/kicli
) 
 
YQL_LAST_ABI_VERSION()

SRCS( 
    cpp_ut.cpp 
) 
 
END() 
