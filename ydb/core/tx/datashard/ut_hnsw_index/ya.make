UNITTEST()

# Test the standalone graph implementation without linking DataShard actors.
# The combined all_hnsw object also brings in shard build units and their dependencies.
ADDINCL(
    ydb/library/nmslib/include
)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/library/nmslib
    ydb/public/api/protos
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ../hnsw_index.cpp
    hnsw_index_ut.cpp
)

END()
