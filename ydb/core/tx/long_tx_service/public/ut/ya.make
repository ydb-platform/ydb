UNITTEST_FOR(ydb/core/tx/long_tx_service/public) 

OWNER(g:kikimr)

SRCS(
    types_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
