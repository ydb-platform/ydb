UNITTEST_FOR(ydb/core/base)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/aclib
)

YQL_LAST_ABI_VERSION()

SRCS(
    auth_ut.cpp
)

END()
