UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/public/sdk/cpp/src/client/draft
    ydb/core/tx/replication/service
    ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
)

SRCS(
    assign_tx_id_ut.cpp
    apply_progress_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
