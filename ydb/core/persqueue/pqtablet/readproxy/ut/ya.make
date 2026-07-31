UNITTEST_FOR(ydb/core/persqueue/pqtablet/readproxy)

SIZE(SMALL)

SRCS(
    readproxy_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/public/lib/base
)

END()
