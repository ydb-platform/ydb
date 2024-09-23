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
    contrib/libs/openssl
    library/cpp/string_utils/base64
    library/cpp/yson
    ydb/library/yql/ast/serialize
    ydb/library/yql/minikql
    ydb/library/yql/sql
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/common_opt
    ydb/library/yql/core/peephole_opt
    ydb/library/yql/core/type_ann
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    mounts
)
