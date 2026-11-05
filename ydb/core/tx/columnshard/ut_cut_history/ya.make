UNITTEST_FOR(ydb/core/tx/columnshard)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
    ydb/core/tx/columnshard/engines/storage/indexes/max
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
)

YQL_LAST_ABI_VERSION()

SRCS(ut_cut_history.cpp)

END()
