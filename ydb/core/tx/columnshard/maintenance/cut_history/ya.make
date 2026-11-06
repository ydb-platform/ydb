LIBRARY()

SRCS(
    find_empty_history_intervals.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/data_accessor/cache_policy
    ydb/core/tx/columnshard/engines
    ydb/core/tx/columnshard/maintenance/cut_history/intervals
    ydb/core/tx/general_cache/usage
    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    intervals
    ut
)
