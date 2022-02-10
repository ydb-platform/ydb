UNITTEST_FOR(ydb/core/kesus/tablet)

OWNER(
    snaury
    g:kikimr
)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/gmock_in_unittest 
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

SRCS(
    tablet_ut.cpp
    quoter_resource_tree_ut.cpp
    ut_helpers.cpp
)

END()
