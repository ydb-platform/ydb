LIBRARY()

SRCS(
    kqp_mkql_compiler.cpp
    kqp_olap_compiler.cpp
    kqp_query_compiler.cpp
)

PEERDIR(
    ydb/core/formats
    ydb/core/kqp/common
    ydb/core/protos
    ydb/core/scheme
    ydb/library/mkql_proto
    yql/essentials/core/arrow_kernels/request
    yql/essentials/core/dq_integration
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    ydb/library/yql/dq/tasks
    yql/essentials/minikql
    yql/essentials/providers/common/mkql
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/s3/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
