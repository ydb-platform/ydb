UNITTEST_FOR(ydb/core/kqp/common)

SIZE(SMALL)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/topics
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    kqp_shard_meta_ut.cpp
    kqp_tx_manager_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
