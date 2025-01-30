LIBRARY()

SRCS(
    conveyor_task.cpp
    queue.cpp
    description.cpp
    result.cpp
    stats.cpp
)

PEERDIR(
    ydb/core/tx/program
    ydb/core/formats/arrow/reader
)

END()
