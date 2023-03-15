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
    kqp_opt_range_legacy.cpp
    kqp_query_blocks_transformer.cpp
    kqp_query_plan.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/opt/logical
    ydb/core/kqp/opt/peephole
    ydb/core/kqp/opt/physical
    ydb/library/yql/dq/common
    ydb/library/yql/dq/opt
    ydb/core/kqp/provider
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(kqp_query_plan.h)

END()
