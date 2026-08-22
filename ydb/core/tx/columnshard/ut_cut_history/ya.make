UNITTEST_FOR(ydb/core/tx/columnshard)

SIZE(SMALL)

PEERDIR(
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/core/tx/columnshard/blobs_action/counters
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/testlib/default
)

SRCS(
    ut_cut_history.cpp
)

YQL_LAST_ABI_VERSION()

END()
