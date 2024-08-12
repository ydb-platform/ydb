LIBRARY()

SRCS(
    kqp_opt_phy_build_stage.cpp
    kqp_opt_phy_limit.cpp
    kqp_opt_phy_olap_agg.cpp
    kqp_opt_phy_olap_filter.cpp
    kqp_opt_phy_precompute.cpp
    kqp_opt_phy_sort.cpp
    kqp_opt_phy_source.cpp
    kqp_opt_phy_helpers.cpp
    kqp_opt_phy_stage_float_up.cpp
    kqp_opt_phy.cpp
    predicate_collector.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/opt/physical/effects
    ydb/library/yql/dq/common
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
)

YQL_LAST_ABI_VERSION()

END()
