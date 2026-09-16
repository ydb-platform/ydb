UNITTEST_FOR(ydb/core/tx/columnshard)

SIZE(MEDIUM)

FORK_SUBTESTS()

# The tablet tests drive long wakeup loops; one chunk per few tests keeps each inside the medium budget.
SPLIT_FACTOR(8)

PEERDIR(
    ydb/core/tablet_flat/test/libs/table
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/core/tx/columnshard/blobs_action/counters
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/core/testlib/default
)

SRCS(
    ut_cut_history.cpp
    ut_cut_history_seeding_loader.cpp
    ut_cut_history_tablet.cpp
)

YQL_LAST_ABI_VERSION()

END()
