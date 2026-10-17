YQL_LIBRARY()

SRCS(
    expand_replicate.cpp  # gn: into ydb/core/kqp/opt/rbo
    inline_join_filters.cpp  # gn: into ydb/core/kqp/opt/rbo
    propagate_topsort_through_stage.cpp  # gn: into ydb/core/kqp/opt/rbo
    pull_up_map_over_cbo.cpp  # gn: into ydb/core/kqp/opt/rbo
    push_map_elements_through_input.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_expand.cpp
    expand_cbo_tree.cpp  # gn: into ydb/core/kqp/opt/rbo
    expand_distinct_aggregation.cpp  # gn: into ydb/core/kqp/opt/rbo
    expand_grouping_sets.cpp  # gn: into ydb/core/kqp/opt/rbo
    expand_whole_partition_window.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_extract.cpp
    extract_common_conjuncts.cpp  # gn: into ydb/core/kqp/opt/rbo
    extract_join_expressions.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_inline_m1.cpp
    inline_cbo_tree.cpp  # gn: into ydb/core/kqp/opt/rbo
    inline_generic_in_exists_subplan.cpp  # gn: into ydb/core/kqp/opt/rbo
    inline_scalar_subplan.cpp  # gn: into ydb/core/kqp/opt/rbo
    inline_simple_in_exists_subplan.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_kqp.cpp
    kqp_cbo_trees.cpp  # gn: into ydb/core/kqp/opt/rbo
    traces/kqp_cbo_trace.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_propagate_m1.cpp
    propagate_aggregate_through_stage.cpp  # gn: into ydb/core/kqp/opt/rbo
    propagate_hash_func_stage.cpp  # gn: into ydb/core/kqp/opt/rbo
    propagate_limit_through_stage.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_push_filter.cpp
    push_filter_into_join.cpp  # gn: into ydb/core/kqp/opt/rbo
    push_filter_through_aggregate.cpp  # gn: into ydb/core/kqp/opt/rbo
    push_filter_under_map.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_push_olap.cpp
    push_olap_filter.cpp  # gn: into ydb/core/kqp/opt/rbo
    push_olap_projection.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_push_rest_1.cpp
    push_limit_into_sort.cpp  # gn: into ydb/core/kqp/opt/rbo
    push_ranges.cpp  # gn: into ydb/core/kqp/opt/rbo
    push_simple_join_filter.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_rewrite.cpp
    rewrite_join_to_index_lookup_join.cpp  # gn: into ydb/core/kqp/opt/rbo
    rewrite_right_join.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_misc_1.cpp
    apply_cbo.cpp  # gn: into ydb/core/kqp/opt/rbo
    assign_stages.cpp  # gn: into ydb/core/kqp/opt/rbo
    build_initial_cbo_tree.cpp  # gn: into ydb/core/kqp/opt/rbo
    constant_folding_stage.cpp  # gn: into ydb/core/kqp/opt/rbo
    decorrelation/dependent_join_pushdown.cpp  # gn: into ydb/core/kqp/opt/rbo
    disable_blocks_on_columns_limit.cpp  # gn: into ydb/core/kqp/opt/rbo
    eliminate_left_join.cpp  # gn: into ydb/core/kqp/opt/rbo
    fuse_filters.cpp  # gn: into ydb/core/kqp/opt/rbo
)

JOIN_SRCS(
    all_misc_2.cpp
    merge_union_all.cpp  # gn: into ydb/core/kqp/opt/rbo
    peephole_predicate.cpp  # gn: into ydb/core/kqp/opt/rbo
)

PEERDIR(
    ydb/core/kqp/opt/peephole
    ydb/core/kqp/opt/cbo
    ydb/core/kqp/opt/cbo/solver
)

END()
