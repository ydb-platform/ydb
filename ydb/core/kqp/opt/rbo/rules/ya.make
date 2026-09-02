LIBRARY()

SRCS(
    inline_join_filters.cpp
    map/push_map_elements_into_map.cpp
    map/push_map_elements_through_aggregate.cpp
    map/push_map_elements_through_input.cpp
    map/push_map_elements_through_union_all.cpp
    propagate_topsort_through_stage.cpp
    pull_up_map_over_cbo.cpp
)

JOIN_SRCS(
    all_expand.cpp
    expand_cbo_tree.cpp
    expand_distinct_aggregation.cpp
)

JOIN_SRCS(
    all_extract.cpp
    extract_common_conjuncts.cpp
    extract_join_expressions.cpp
)

JOIN_SRCS(
    all_inline_m1.cpp
    inline_cbo_tree.cpp
    inline_generic_in_exists_subplan.cpp
    inline_scalar_subplan.cpp
    inline_simple_in_exists_subplan.cpp
)

JOIN_SRCS(
    all_kqp.cpp
    kqp_cbo_trees.cpp
    traces/kqp_cbo_trace.cpp
)

JOIN_SRCS(
    all_propagate_m1.cpp
    propagate_aggregate_through_stage.cpp
    propagate_hash_func_stage.cpp
    propagate_limit_through_stage.cpp
)

JOIN_SRCS(
    all_push_filter.cpp
    push_filter_into_join.cpp
    push_filter_under_map.cpp
)

JOIN_SRCS(
    all_push_olap.cpp
    push_olap_filter.cpp
    push_olap_projection.cpp
)

JOIN_SRCS(
    all_push_rest_1.cpp
    push_limit_into_sort.cpp
    push_ranges.cpp
    map/push_rename_into_producer.cpp
    push_simple_join_filter.cpp
)

JOIN_SRCS(
    all_rewrite.cpp
    map/rewrite_to_preferred_alias.cpp
    rewrite_join_to_index_lookup_join.cpp
    rewrite_right_join.cpp
)

JOIN_SRCS(
    all_misc_1.cpp
    apply_cbo.cpp
    assign_stages.cpp
    build_initial_cbo_tree.cpp
    constant_folding_stage.cpp
    decorrelation/dependent_join_pushdown.cpp
    disable_blocks_on_columns_limit.cpp
    eliminate_left_join.cpp
    fuse_filters.cpp
)

JOIN_SRCS(
    all_misc_2.cpp
    merge_union_all.cpp
    peephole_predicate.cpp
    map/prune_dead_outputs.cpp
    map/remove_identity_map.cpp
    map/rename_to_append.cpp
)

PEERDIR(
    ydb/core/kqp/opt/peephole
    ydb/core/kqp/opt/cbo
    ydb/core/kqp/opt/cbo/solver
)

YQL_LAST_ABI_VERSION()

END()
