LIBRARY()

SRCS(
    kqp_rbo_transformer.cpp
    kqp_operator.cpp
    kqp_rbo.cpp
    kqp_rbo_rules.cpp
    kqp_convert_to_physical.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/opt/logical
    ydb/core/kqp/opt/peephole
    ydb/core/kqp/opt/physical
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
