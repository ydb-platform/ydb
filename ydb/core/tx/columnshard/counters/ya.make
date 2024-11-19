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
    insert_table.cpp
    req_tracer.cpp
    scan.cpp
    splitter.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/tx/columnshard/counters/aggregation
    ydb/core/tx/columnshard/counters/common
    ydb/core/base
)

GENERATE_ENUM_SERIALIZATION(columnshard.h)
GENERATE_ENUM_SERIALIZATION(scan.h)

END()
