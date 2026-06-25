UNITTEST_FOR(ydb/core/split)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    ../ut_split.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/core/tablet_flat
    ydb/core/testlib/pg
    library/cpp/testing/unittest
)

END()
