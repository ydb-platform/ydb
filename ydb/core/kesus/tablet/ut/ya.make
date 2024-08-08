UNITTEST_FOR(ydb/core/kesus/tablet)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    tablet_ut.cpp
    quoter_resource_tree_ut.cpp
    ut_helpers.cpp
)

END()
