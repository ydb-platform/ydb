LIBRARY()

SRCS(
    kqp_rbo_transformer.cpp
    kqp_operator.cpp
    kqp_expression.cpp
    kqp_stage_graph.cpp
    kqp_rbo_utils.cpp
    kqp_rbo.cpp
    kqp_plan_conversion_utils.cpp
    kqp_rbo_type_ann.cpp
    kqp_rewrite_select.cpp
    kqp_rbo_compute_statistics.cpp
    kqp_rbo_statistics.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/opt/logical
    ydb/core/kqp/opt/peephole
    ydb/core/kqp/opt/physical
    ydb/core/kqp/opt/rbo/rules
    ydb/core/kqp/opt/rbo/physical_convertion
    ydb/library/yql/dq/common
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/s3/expr_nodes
    ydb/library/yql/providers/s3/statistics
    ydb/library/yql/utils/plan
    ydb/core/kqp/provider
    ydb/library/formats/arrow/protos
)

YQL_LAST_ABI_VERSION()

END()
