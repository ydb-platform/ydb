YQL_LIBRARY()

SRCS(
    yql_eval_expr.cpp
    yql_eval_expr.h
    yql_eval_params.cpp  # gn: pipeline
    yql_eval_params.h
    yql_out_transformers.cpp  # gn: pipeline
    yql_out_transformers.h
    yql_lineage.cpp  # gn: pipeline
    yql_lineage.h
    yql_plan.cpp
    yql_plan.h
    yql_transform_pipeline.cpp  # gn: pipeline
    yql_transform_pipeline.h
    yql_transform_pipeline_eval.cpp
)
# gn: pipeline headers yql_eval_params.h yql_lineage.h yql_out_transformers.h yql_transform_pipeline.h

PEERDIR(
    library/cpp/string_utils/base64
    library/cpp/time_provider
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

END()

RECURSE(
    mounts
)
