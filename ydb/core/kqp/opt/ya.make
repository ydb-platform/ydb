LIBRARY()

SRCS(
    kqp_opt.cpp
    kqp_opt_build_phy_query.cpp
    kqp_opt_build_txs.cpp
    kqp_opt_effects.cpp
    kqp_opt_kql.cpp
    kqp_opt_phase.cpp
    kqp_opt_phy_check.cpp
    kqp_opt_phy_finalize.cpp
    kqp_query_blocks_transformer.cpp
    kqp_query_plan.cpp
    kqp_statistics_transformer.cpp
    kqp_constant_folding_transformer.cpp
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
    ydb/library/yql/utils/plan
    ydb/core/kqp/provider
    ydb/core/formats/arrow/protos
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(kqp_query_plan.h)

END()
