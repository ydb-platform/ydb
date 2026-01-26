LIBRARY()

SRCS(
    kqp_rbo_convert_to_physical.cpp
    kqp_rbo_physical_aggregation_builder.cpp
    kqp_rbo_physical_sort_builder.cpp
    kqp_rbo_physical_join_builder.cpp
    kqp_rbo_physical_map_builder.cpp
    kqp_rbo_physical_filter_builder.cpp
    kqp_rbo_physical_source_builder.cpp
    kqp_rbo_physical_convertion_utils.cpp
)

PEERDIR(
    ydb/core/kqp/opt/peephole
)

YQL_LAST_ABI_VERSION()

END()
