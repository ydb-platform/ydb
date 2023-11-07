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
    ydb/library/mkql_proto
    ydb/library/yql/core/arrow_kernels/request
    ydb/library/yql/dq/integration
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    ydb/library/yql/dq/tasks
    ydb/library/yql/minikql
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/dq/common
)

YQL_LAST_ABI_VERSION()

END()
