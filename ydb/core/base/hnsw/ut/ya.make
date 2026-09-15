UNITTEST_FOR(ydb/core/base/hnsw)

SIZE(SMALL)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    hnsw_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
