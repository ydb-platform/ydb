LIBRARY()

SRCS(
    column_tables.cpp
    indexation.cpp
    scan.cpp
    engine_logs.cpp
    blobs_manager.cpp
    columnshard.cpp
    insert_table.cpp
    common_data.cpp
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
