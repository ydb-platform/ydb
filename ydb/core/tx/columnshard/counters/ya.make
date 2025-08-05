LIBRARY()

SRCS(
    background_controller.cpp
    counters_manager.cpp
    blobs_manager.cpp
    column_tables.cpp
    columnshard.cpp
    common_data.cpp
    engine_logs.cpp
    indexation.cpp
    req_tracer.cpp
    scan.cpp
    splitter.cpp
    portions.cpp
    writes_monitor.cpp
    portion_index.cpp
    duplicate_filtering.cpp
    error_collector.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/tx/columnshard/counters/aggregation
    ydb/library/signals
    ydb/core/base
    ydb/library/actors/core
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(columnshard.h)
GENERATE_ENUM_SERIALIZATION(scan.h)

END()
