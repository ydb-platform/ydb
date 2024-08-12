GTEST()

SRCS(
    metadata_conversion.cpp
)

PEERDIR(
    ydb/core/kqp/gateway
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf/service/stub
    ydb/services/kesus
    ydb/services/ydb
    ydb/services/persqueue_cluster_discovery
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/services/metadata
    ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

END()

