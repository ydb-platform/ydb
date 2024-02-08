LIBRARY()

SRCS(
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
    ydb/core/tx/columnshard/counters/common
    ydb/core/tx/columnshard/splitter
    ydb/core/base
)

END()
