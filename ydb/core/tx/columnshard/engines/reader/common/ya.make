LIBRARY()

SRCS(
    conveyor_task.cpp
    queue.cpp
    description.cpp
    result.cpp
    stats.cpp
    comparable.cpp
)

PEERDIR(
    ydb/core/tx/program
    ydb/core/formats/arrow/reader
)

GENERATE_ENUM_SERIALIZATION(description.h)

END()
