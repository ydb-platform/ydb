LIBRARY()

SRCS(
    worker.cpp
    service.cpp
)

PEERDIR(
    ydb/core/kqp/predictor
    ydb/core/protos
    ydb/core/tx/conveyor/tracing
    ydb/core/tx/conveyor/usage
)

END()
