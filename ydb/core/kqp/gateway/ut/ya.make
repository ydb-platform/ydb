GTEST()

SRCS(
    metadata_conversion.cpp
)

PEERDIR(
    ydb/core/kqp/gateway
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
    ydb/services/kesus
    ydb/services/ydb
    ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm14
    ydb/services/metadata
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()

