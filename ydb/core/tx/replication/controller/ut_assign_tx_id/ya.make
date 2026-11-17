UNITTEST_FOR(ydb/core/tx/replication/controller)

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/core/tx/replication/private_client
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/draft
    ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
    ydb/library/yql/udfs/digest
    yql/essentials/udfs/common/digest
)

SRCS(
    assign_tx_id_ut.cpp
    transactions_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
