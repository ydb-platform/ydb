UNITTEST_FOR(ydb/core/tx/scheme_board) 

OWNER(
    ilnaz
    g:kikimr
)

FORK_SUBTESTS()
 
SIZE(MEDIUM)
 
TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib 
    ydb/core/tx/schemeshard 
    ydb/core/tx/schemeshard/ut_helpers 
)

YQL_LAST_ABI_VERSION() 

SRCS(
    cache_ut.cpp
    ut_helpers.cpp
)

END()
