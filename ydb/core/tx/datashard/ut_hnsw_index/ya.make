UNITTEST_FOR(ydb/core/tx/datashard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/public/api/protos
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    hnsw_index_ut.cpp
)

END()
