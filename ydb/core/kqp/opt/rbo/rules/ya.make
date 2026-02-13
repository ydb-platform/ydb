LIBRARY()

SRCS(
    apply_cbo.cpp
    assign_stages.cpp
    build_initial_cbo_tree.cpp
    constant_folding_stage.cpp
    correlated_filter_pullup.cpp
    expand_cbo_tree.cpp
    extract_join_expressions.cpp
    inline_cbo_tree.cpp
    inline_scalar_subplan.cpp
    inline_simple_in_exists_subplan.cpp
    peephole_predicate.cpp
    prune_columns_stage.cpp
    push_filter_into_join.cpp
    push_filter_under_map.cpp
    push_limit_into_sort.cpp
    push_map.cpp
    push_olap_filter.cpp
    remove_extra_renames_stage.cpp
    remove_identity_map.cpp
)

PEERDIR(
    ydb/core/kqp/opt/peephole
)

YQL_LAST_ABI_VERSION()

END()
