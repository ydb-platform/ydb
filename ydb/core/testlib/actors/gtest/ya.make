GTEST()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/basics/default
)

SRCS(
    test_runtime_gtest.cpp
)

YQL_LAST_ABI_VERSION()

END()
