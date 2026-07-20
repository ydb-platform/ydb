LIBRARY()

SRCS(
    yql_dq_exectransformer.cpp
    yql_dq_exectransformer.h
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/svnversion
    library/cpp/threading/future
    library/cpp/yson/node
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/dq/opt
    ydb/library/yql/providers/dq/planner
    ydb/library/yql/providers/dq/provider
    yql/essentials/core
    yql/essentials/core/dq_integration
    yql/essentials/core/peephole_opt
    yql/essentials/core/services
    yql/essentials/core/type_ann
    yql/essentials/minikql
    yql/essentials/minikql/runtime_settings
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/transform
    yql/essentials/providers/result/expr_nodes
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
