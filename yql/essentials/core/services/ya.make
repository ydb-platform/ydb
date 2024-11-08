LIBRARY()

SRCS(
    yql_eval_expr.cpp
    yql_eval_expr.h
    yql_eval_params.cpp
    yql_eval_params.h
    yql_out_transformers.cpp
    yql_out_transformers.h
    yql_lineage.cpp
    yql_lineage.h
    yql_plan.cpp
    yql_plan.h
    yql_transform_pipeline.cpp
    yql_transform_pipeline.h
)

PEERDIR(
    library/cpp/string_utils/base64
    library/cpp/yson
    yql/essentials/ast/serialize
    yql/essentials/minikql
    yql/essentials/sql
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/common_opt
    yql/essentials/core/peephole_opt
    yql/essentials/core/type_ann
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    mounts
)
