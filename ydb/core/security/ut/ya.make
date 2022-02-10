UNITTEST_FOR(ydb/core/security) 

OWNER( 
    g:kikimr 
    g:logbroker 
) 
 
FORK_SUBTESTS()

TIMEOUT(600)
 
SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib 
)

YQL_LAST_ABI_VERSION() 

SRCS(
    ticket_parser_ut.cpp
)

END()
