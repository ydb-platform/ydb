UNITTEST_FOR(ydb/core/tx/sequenceproxy) 

OWNER(g:kikimr)

SRCS(
    sequenceproxy_ut.cpp
)

PEERDIR(
    ydb/core/testlib 
)

YQL_LAST_ABI_VERSION()

END()
