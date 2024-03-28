LIBRARY()

SRCS(
    conveyor_task.cpp
    queue.cpp
    description.cpp
    read_filter_merger.cpp
    result.cpp
    stats.cpp
)

PEERDIR(
    ydb/core/tx/program
)

END()
