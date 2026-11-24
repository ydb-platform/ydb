UNITTEST_FOR(ydb/core/base)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/scheme
    ydb/library/actors/testlib
)

SRCS(
    statestorage_proxy_ut.cpp
)

END()
