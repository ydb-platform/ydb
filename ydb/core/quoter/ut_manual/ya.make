UNITTEST_FOR(ydb/core/quoter)

TAG(ya:manual)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_helpers.cpp
    ut_manual/kesus_quoter_manual_ut.cpp
)

SIZE(MEDIUM)

END()
