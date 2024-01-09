LIBRARY()

PEERDIR(
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/transform
    ydb/library/yql/utils/log
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    ydb/library/yql/dq/integration
    ydb/library/yql/core
    ydb/library/yql/core/peephole_opt
    ydb/library/yql/core/type_ann
    ydb/library/yql/minikql/computation
    library/cpp/yson/node
)

SRCS(
    dqs_opt.cpp
    logical_optimize.cpp
    physical_optimize.cpp
)

YQL_LAST_ABI_VERSION()

END()
