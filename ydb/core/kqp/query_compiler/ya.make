LIBRARY()

SRCS(
    kqp_mkql_compiler.cpp
    kqp_olap_compiler.cpp
    kqp_query_compiler.cpp
)

PEERDIR(
    ydb/core/formats
    ydb/core/kqp/common
    ydb/core/kqp/runtime/common
    ydb/core/protos
    ydb/core/scheme
    ydb/library/mkql_proto
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/tasks
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/pq/proto
    ydb/library/yql/providers/s3/expr_nodes
    yql/essentials/core
    yql/essentials/core/arrow_kernels/request
    yql/essentials/core/dq_integration
    yql/essentials/minikql
    yql/essentials/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()
