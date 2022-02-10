UNITTEST_FOR(ydb/core/tx/long_tx_service)
 
OWNER(g:kikimr) 
 
SRCS( 
    long_tx_service_ut.cpp 
) 
 
PEERDIR( 
    ydb/core/testlib
) 
 
YQL_LAST_ABI_VERSION() 
 
END() 
