UNITTEST_FOR(ydb/core/base)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    ydb/core/scheme
    ydb/library/actors/testlib
)

SRCS(
    statestorage_proxy_ut.cpp
)

END()
