LIBRARY()

SRCS(
    conveyor_task.cpp
    queue.cpp
    description.cpp
    result.cpp
    scan_memory_limiter.cpp
    stats.cpp
    comparable.cpp
)

PEERDIR(
    ydb/core/tx/program
    ydb/core/formats/arrow/reader
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/private_events
    ydb/core/tx/limiter/grouped_memory/usage
    ydb/library/actors/core
)

GENERATE_ENUM_SERIALIZATION(description.h)

END()
