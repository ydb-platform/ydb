UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    hnsw_index_ut.cpp
    hnsw_memory_tracker_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/datashard/hnsw_index
    ydb/public/api/protos
    yql/essentials/public/decimal
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
