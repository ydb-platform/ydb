UNITTEST()

# Test the standalone graph implementation without linking DataShard actors.
ADDINCL(
    ydb/library/nmslib/include
)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/tx/datashard/hnsw
    ydb/core/scheme
    ydb/library/nmslib
    ydb/public/api/protos
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    hnsw_index_ut.cpp
)

END()
