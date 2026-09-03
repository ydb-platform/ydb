LIBRARY()

SRCS(
    aggregator.h
    aggregator.cpp
    aggregator_impl.h
    aggregator_impl.cpp
    analyze_actor.h
    analyze_actor.cpp
    column_statistic_eval.h
    column_statistic_eval.cpp
    schema.h
    schema.cpp
    select_builder.h
    select_builder.cpp
    tx_analyze.cpp
    tx_analyze_deadline.cpp
    tx_analyze_op_cancel.cpp
    tx_analyze_op_forget.cpp
    tx_analyze_op_get.cpp
    tx_analyze_op_list.cpp
    tx_configure.cpp
    tx_finish_trasersal.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_schedule_traversal.cpp
    tx_schemeshard_stats.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/statistics/database
    ydb/library/yql/udfs/statistics_internal
    yql/essentials/core/histogram
    yql/essentials/core/minsketch
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
