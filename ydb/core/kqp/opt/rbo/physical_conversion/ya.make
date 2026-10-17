YQL_LIBRARY()

SRCS(
    kqp_rbo_convert_to_physical.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_compatibility.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_aggregation_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_sort_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_window_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_join_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_lookup_join_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_map_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_union_all_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_filter_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_source_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_strict_cast.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_table_effect_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_convertion_utils.cpp  # gn: into ydb/core/kqp/opt/rbo
    kqp_rbo_physical_query_builder.cpp  # gn: into ydb/core/kqp/opt/rbo
)

PEERDIR(
    ydb/core/kqp/opt/physical
    ydb/core/kqp/opt/peephole
)

END()
