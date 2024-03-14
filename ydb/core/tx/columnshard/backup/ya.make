LIBRARY()

SRCS(
    task.cpp
    manager.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/columnshard/data_sharing/destination/events
)

END()
