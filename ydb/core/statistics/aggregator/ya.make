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
    key_range_predicate.h
    key_range_predicate.cpp
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
    ydb/core/scheme
    ydb/core/ydb_convert
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/statistics/database
    ydb/library/query_actor
    ydb/library/yql/udfs/statistics_internal
    ydb/public/sdk/cpp/src/client/params
    yql/essentials/core/histogram
    yql/essentials/core/minsketch
    yql/essentials/types/dynumber
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
