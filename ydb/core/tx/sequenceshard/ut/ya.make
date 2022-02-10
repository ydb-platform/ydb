UNITTEST_FOR(ydb/core/tx/sequenceshard) 

OWNER(g:kikimr)

PEERDIR(
    ydb/core/testlib 
)

SRCS(
    ut_helpers.cpp
    ut_sequenceshard.cpp
)

YQL_LAST_ABI_VERSION()

END()
