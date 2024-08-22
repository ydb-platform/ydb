UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

TIMEOUT(600)
SIZE(MEDIUM)

SRCS(
    kqp_scan_spilling_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/core/kqp
    ydb/core/kqp/counters
    ydb/core/kqp/host
    ydb/core/kqp/provider
    ydb/core/kqp/ut/common
    #ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
