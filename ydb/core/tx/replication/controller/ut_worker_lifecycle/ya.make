UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/core/transfer
    ydb/core/tx/replication/service
    ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
)

SRCS(
    cdc_live_mirroring_ut.cpp
    session_lifecycle_ut.cpp
    worker_lifecycle_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
