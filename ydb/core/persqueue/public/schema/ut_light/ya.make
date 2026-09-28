UNITTEST_FOR(ydb/core/persqueue/public/schema)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

SRCS(
    ../validation_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/testlib/basics
    ydb/core/testlib/default
)

END()
