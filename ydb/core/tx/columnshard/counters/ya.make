LIBRARY()

SRCS(
    indexation.cpp
    scan.cpp
    engine_logs.cpp
    blobs_manager.cpp
    columnshard.cpp
    insert_table.cpp
    common_data.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/tx/columnshard/counters/common
    ydb/core/base
)

END()
