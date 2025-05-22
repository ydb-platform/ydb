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
    ydb/public/sdk/cpp/src/client/driver
    yql/essentials/core
    yql/essentials/core/dq_integration
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
    ydb/library/yql/dq/type_ann
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/metrics
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/transform
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/dq/opt
    ydb/library/yql/providers/dq/planner
    ydb/library/yql/providers/dq/runtime
    yql/essentials/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
