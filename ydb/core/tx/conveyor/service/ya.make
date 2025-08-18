LIBRARY()

SRCS(
    worker.cpp
    service.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/conveyor/usage
    ydb/core/tx/conveyor/tracing
)

END()
