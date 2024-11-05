LIBRARY()

SRCS(
    yql_dq_exectransformer.cpp
    yql_dq_exectransformer.h
)

PEERDIR(
    library/cpp/yson/node
    library/cpp/svnversion
    library/cpp/digest/md5
    library/cpp/threading/future
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/library/yql/core
    ydb/library/yql/dq/integration
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/common/transform
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/dq/opt
    ydb/library/yql/providers/dq/planner
    ydb/library/yql/providers/dq/runtime
    ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
