LIBRARY()

PEERDIR(
    ydb/library/yql/providers/dq/expr_nodes
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/transform
    yql/essentials/utils/log
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    yql/essentials/core/dq_integration
    yql/essentials/core
    yql/essentials/core/peephole_opt
    yql/essentials/core/type_ann
    yql/essentials/minikql/computation
    library/cpp/yson/node
)

SRCS(
    dqs_opt.cpp
    logical_optimize.cpp
    physical_optimize.cpp
)

YQL_LAST_ABI_VERSION()

END()
