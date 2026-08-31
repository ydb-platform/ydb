UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_scan_spilling_ut.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/client/proto
    ydb/core/kqp
    ydb/core/kqp/counters
    ydb/core/kqp/host
    ydb/core/kqp/provider
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
)

YQL_LAST_ABI_VERSION()

END()
