UNITTEST_FOR(ydb/core/tx/columnshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_move_data.cpp
)

END()
