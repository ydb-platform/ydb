UNITTEST()

FORK_SUBTESTS()

SRCS(
    hnsw_settings_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/public/api/protos
    yql/essentials/public/decimal
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
