UNITTEST_FOR(ydb/core/tx/columnshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/testlib/default
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/core/tx/columnshard/data_sharing/manager
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/engines/storage/actualizer/move
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_move_data.cpp
)

END()
