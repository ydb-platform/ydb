LIBRARY()

SRCS(
    worker.cpp
    service.cpp
)

PEERDIR(
    ydb/core/tx/conveyor/usage
    ydb/core/protos
)

END()
