YQL_LIBRARY()

SRCS(
    adapters/column_cache_observer.cpp
    adapters/decode_observer.cpp
    adapters/merge_observer.cpp
    adapters/processor_stats_adapter.cpp
    adapters/program_observer.cpp
    adapters/read_delivery_observer.cpp
    adapters/read_observer.cpp
    adapters/read_report_finalizer.cpp
    operations.cpp
)

PEERDIR(
    ydb/core/formats/arrow/program
    ydb/core/tx/columnshard/blobs_action/common
    ydb/core/tx/columnshard/blobs_reader
    ydb/core/tx/columnshard/column_fetching
    ydb/core/tx/columnshard/data_accessor/cache_policy
    ydb/core/tx/columnshard/stats/scan
    ydb/core/tx/columnshard/stats/scan/transport
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
