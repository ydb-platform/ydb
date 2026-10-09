UNITTEST_FOR(ydb/library/yql/providers/ydb_external/provider)

SRCS(
    provider_ut.cpp
    metadata_rpc_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/ydb_external/common
    contrib/libs/grpc
    library/cpp/testing/common
    ydb/public/api/grpc
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/ydb_external/expr_nodes
    ydb/library/yql/providers/ydb_external/proto
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SIZE(SMALL)

END()
